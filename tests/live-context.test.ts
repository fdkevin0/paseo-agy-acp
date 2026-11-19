import assert from 'node:assert/strict'
import { test, type TestContext } from 'node:test'
import { mkdtemp, mkdir, writeFile, rm, appendFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import { SessionManager } from '../server/session.ts'
import { connectAcp } from '../server/transport.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  ModelUsageObserver,
  BrainTurnObservation,
} from '../server/brain-observer.ts'
import { TranscriptReader } from '../server/brain-io.ts'
import { watchBrain } from '../server/brain.ts'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { normalizeAskQuestionInput } from '../server/questions.ts'
import { isolatedEnv, sequenceDouble } from './helpers.ts'

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}
type Result = {
  stopReason: 'end_turn' | 'cancelled'
  usage?: {
    inputTokens: number
    outputTokens: number
    totalTokens: number
    cachedReadTokens?: number
  }
}
type Row = Record<string, unknown>
const encode = (rows: Row[]) =>
  rows.map((row) => JSON.stringify(row) + '\n').join('')
const user = (step_index: number): Row => ({
  step_index,
  type: 'USER_INPUT',
  content: 'work',
})
const model = (
  step_index: number,
  input_tokens: number,
  content = 'model text',
): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  input_tokens,
  output_tokens: 1,
  content,
})
const checkpoint = (step_index: number, content = 'summary'): Row => ({
  step_index,
  type: 'CHECKPOINT',
  status: 'DONE',
  content,
  input_tokens: 999999,
})

type RpcPrompt = ReturnType<typeof connectAcp>['rpc']['prompt']
type PromptHandler = (
  params: Parameters<RpcPrompt>[0],
  realPrompt: RpcPrompt,
) => Promise<Result>

class HarnessSessionManager extends SessionManager {
  session!: Awaited<ReturnType<SessionManager['start']>>
  override async start(...args: Parameters<SessionManager['start']>) {
    this.session = await super.start(...args)
    return this.session
  }
}

class TrackingObservation extends BrainTurnObservation {
  readonly attached = deferred<void>()
  override async attach(
    consumer: Parameters<BrainTurnObservation['attach']>[0],
  ) {
    await super.attach(consumer)
    this.attached.resolve()
  }
}

class SwappableTranscriptReader extends TranscriptReader {
  impl: TranscriptReader['read'] = (root, id) => super.read(root, id)
  private nextFault?: Error
  injectOnce(error: Error) {
    this.nextFault = error
  }
  override async read(root: string, nativeId: string) {
    if (this.nextFault) {
      const error = this.nextFault
      this.nextFault = undefined
      throw error
    }
    return this.impl(root, nativeId)
  }
}

function wrapConnectAcp(
  getPromptHandler: () => PromptHandler | undefined,
  onRealPrompt: (realPrompt: RpcPrompt) => void,
): typeof connectAcp {
  return (...args) => {
    const transport = connectAcp(...args)
    const realPrompt = transport.rpc.prompt.bind(transport.rpc)
    onRealPrompt(realPrompt)
    const wrappedRpc = new Proxy(transport.rpc, {
      get(target, prop) {
        if (prop === 'prompt') {
          return (params: Parameters<typeof realPrompt>[0]) => {
            const handler = getPromptHandler()
            return handler ? handler(params, realPrompt) : realPrompt(params)
          }
        }
        const value = Reflect.get(target, prop, target)
        return typeof value === 'function' ? value.bind(target) : value
      },
    })
    return new Proxy(transport, {
      get(target, prop) {
        if (prop === 'rpc') return wrappedRpc
        return Reflect.get(target, prop, target)
      },
    })
  }
}

async function harness(
  t: TestContext,
  options?: {
    agyVersion?: string
    hasActiveBackgroundWork?: ConstructorParameters<
      typeof SessionManager
    >[0]['hasActiveBackgroundWork']
    cancelConvergenceTimeoutMs?: number
    backgroundProbeTimeoutMs?: number
    observationPollIntervalMs?: number
    createTranscriptReader?: () => TranscriptReader
  },
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-live-context-'))
  const events: ProviderEvent[] = []
  const listeners = new Set<() => void>()
  let promptHandler: PromptHandler | undefined
  let realPrompt!: RpcPrompt
  let lastObservation!: TrackingObservation
  const env = isolatedEnv(home)
  const manager = new HarnessSessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: options?.agyVersion
        ? { ...env, AGY_TEST_VERSION: options.agyVersion }
        : env,
    },
    usage: new AgyUsageSource({ env }),
    capabilities: ['prompt.message', 'permission'],
    emit: (event) => {
      events.push(structuredClone(event))
      for (const listener of listeners) listener()
    },
    hasActiveBackgroundWork: options?.hasActiveBackgroundWork,
    cancelConvergenceTimeoutMs: options?.cancelConvergenceTimeoutMs,
    backgroundProbeTimeoutMs: options?.backgroundProbeTimeoutMs,
    observationPollIntervalMs: options?.observationPollIntervalMs,
    createTranscriptReader: options?.createTranscriptReader,
    createObservation: (...args) =>
      (lastObservation = new TrackingObservation(...args)),
    connectAcp: wrapConnectAcp(
      () => promptHandler,
      (fn) => {
        realPrompt = fn
      },
    ),
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: { cwd: home, env: {}, mcpServers: {}, settings: {}, persist: true },
  })
  const session = manager.session
  const dir = join(
    session.brainRoot,
    session.state.nativeId,
    '.system_generated/logs',
  )
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  })
  const write = (rows: Row[], partial = '') =>
    writeFile(path, encode(rows) + partial)
  const append = (rows: Row[]) => appendFile(path, encode(rows))
  let sequence = 0
  const prompt = (text = 'work') =>
    manager.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: `p${++sequence}`,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  const latest = () => {
    const event = events.findLast((event) => event.type === 'session.usage')
    assert.ok(event?.type === 'session.usage')
    return event.usage
  }
  const turns = () =>
    events.flatMap((event) =>
      event.type === 'session.turn' && event.state !== 'started'
        ? [event.state]
        : [],
    )
  const compactions = () =>
    events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'compaction'
        ? [event]
        : [],
    )
  const toolCalls = () =>
    events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'tool_call'
        ? [event.item]
        : [],
    )
  const permissions = () =>
    events.flatMap((event) =>
      event.type === 'session.permission' ? [event] : [],
    )
  const wait = async (predicate: () => boolean) => {
    if (predicate()) return
    await new Promise<void>((resolve) => {
      const check = () => {
        if (predicate()) {
          listeners.delete(check)
          resolve()
        }
      }
      listeners.add(check)
      check()
    })
  }
  const read = async () => {
    const observation = session.state.observation
    assert.ok(observation)
    await observation.poll()
    await observation.poll()
  }
  const hold = async () => {
    const entered = deferred<void>(),
      done = deferred<Result>()
    promptHandler = async () => {
      promptHandler = undefined
      entered.resolve()
      return done.promise
    }
    const running = prompt()
    await entered.promise
    return { running, done, observation: lastObservation }
  }
  const holdLive = async (
    mode: 'hold:cancel' | 'hold:cancel-throw' | 'hold:never' = 'hold:cancel',
  ) => {
    const target =
      events.filter((event) => event.type === 'session.commands').length + 1
    const running = prompt(mode)
    await wait(
      () =>
        events.filter((event) => event.type === 'session.commands').length >=
        target,
    )
    return { running, observation: lastObservation }
  }
  return {
    home,
    manager,
    session,
    events,
    path,
    write,
    append,
    prompt,
    nativePrompt: (params: Parameters<RpcPrompt>[0]) => realPrompt(params),
    setPromptHandler: (handler: PromptHandler | undefined) => {
      promptHandler = handler
    },
    latest,
    turns,
    compactions,
    toolCalls,
    permissions,
    wait,
    read,
    hold,
    holdLive,
  }
}

test(
  'held silent foreground observes each reliable snapshot and checkpoint without settling or billing',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
    const held = await h.hold()
    await h.read()
    assert.deepEqual(
      h.turns(),
      [],
      'missing transcript cannot complete an active RPC',
    )
    await h.write([user(1), model(2, 100), checkpoint(3)])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 101)
    assert.equal(h.latest().inputTokens, 1000)
    assert.equal(h.latest().outputTokens, 100)
    assert.equal(h.compactions().length, 1)
    assert.equal(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message',
      ),
      false,
    )
    await h.write([user(1), model(2, 200), checkpoint(3, 'different summary')])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 201)
    const count = h.events.filter(
      (event) => event.type === 'session.usage',
    ).length
    await h.read()
    assert.equal(
      h.events.filter((event) => event.type === 'session.usage').length,
      count,
    )
    assert.equal(h.compactions().length, 1)
    assert.deepEqual(h.turns(), [])
    held.done.resolve({
      stopReason: 'end_turn',
      usage: { inputTokens: 5000, outputTokens: 50, totalTokens: 5050 },
    })
    await held.running
    assert.deepEqual(h.turns(), ['completed'])
    assert.equal(h.latest().inputTokens, 5000)
    assert.equal(h.compactions().length, 1)
    assert.equal(held.observation.phase, 'disposed')
  },
)

test(
  'initial raw handoff waits for ACP tool identity and has no early question-settled effects',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    const args = {
      questions: [{ question: 'same question', options: ['A', 'B'] }],
    }
    const questions = normalizeAskQuestionInput(args)
    h.session.state.handledElicitations.push({
      questions,
      answerText: 'A',
      wokeUp: false,
    })
    await h.write([
      user(1),
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { name: 'run_command', args: { CommandLine: 'echo ok' } },
          { name: 'ask_question', args },
        ],
      },
      { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'ok' },
      { step_index: 4, type: 'GENERIC', status: 'DONE', content: 'A' },
      model(5, 20, ''),
    ])
    await h.read()
    assert.equal(h.session.state.handledElicitations.length, 1)
    assert.equal(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' && event.item.type === 'tool_call',
      ),
      false,
    )
    h.session.state.turnToolCallIds.push('native-command', 'native-question')
    h.session.timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'native-command',
      title: 'tool',
      status: 'in_progress',
    })
    h.session.timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'native-question',
      title: 'tool',
      status: 'in_progress',
    })
    held.done.resolve({ stopReason: 'end_turn' })
    await held.running
    const tools = h.toolCalls()
    assert.deepEqual(
      new Set(tools.map((item) => item.callId)),
      new Set(['native-command', 'native-question']),
    )
    assert.equal(
      tools.some((item) => item.callId.startsWith('tool-')),
      false,
    )
    assert.equal(h.session.state.handledElicitations.length, 0)
    assert.equal(
      h.events.some((event) => event.type === 'session.permission'),
      false,
    )
  },
)

test(
  'continuation streams Brain text/context and resolves only anonymous tool slots after late native IDs',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      continuation = deferred<Result>(),
      entered = deferred<void>()
    const args = { questions: [{ question: 'Choose', options: ['A', 'B'] }] }
    h.setPromptHandler(
      sequenceDouble<[], Promise<Result>>([
        async () => {
          await h.write([
            user(1),
            model(2, 10, 'waiting'),
            {
              step_index: 3,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=system priority=MESSAGE_PRIORITY_HIGH content=continue',
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [{ id: 'ask', name: 'ask_question', args }],
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
        async () => {
          entered.resolve()
          return continuation.promise
        },
      ]),
    )
    const running = h.prompt()
    await h.wait(() =>
      h.events.some((event) => event.type === 'session.permission'),
    )
    const question = h.events.find(
      (event) => event.type === 'session.permission',
    )
    assert.ok(question?.type === 'session.permission')
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: question.request.id,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { Choose: 'A' } },
      },
    })
    await entered.promise
    // Native context from the initial phase must not freeze newer continuation occupancy.
    h.session.state.turnHadContextUsage = true
    h.session.publishUsage({ contextWindowUsedTokens: 15 })
    await h.append([
      user(5),
      {
        ...model(6, 90, 'live continuation'),
        tool_calls: [
          { name: 'run_command', args: { CommandLine: 'first' } },
          { id: 'known', name: 'run_command', args: { CommandLine: 'second' } },
        ],
      },
      {
        step_index: 7,
        type: 'GENERIC',
        status: 'DONE',
        content: 'first result',
      },
      {
        step_index: 8,
        type: 'GENERIC',
        status: 'DONE',
        content: 'second result',
      },
      checkpoint(9),
    ])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 91)
    assert.ok(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === 'live continuation',
      ),
    )
    assert.ok(
      h
        .toolCalls()
        .some((item) => item.callId === 'known' && item.status === 'completed'),
    )
    assert.equal(
      h.toolCalls().some((item) => item.callId.startsWith('tool-')),
      false,
    )
    assert.deepEqual(h.turns(), [])
    h.session.state.turnToolCallIds.push('native-late')
    await h.read()
    assert.ok(
      h
        .toolCalls()
        .some(
          (item) =>
            item.callId === 'native-late' && item.status === 'completed',
        ),
    )
    await h.append([model(10, 40, 'final')])
    await h.read()
    continuation.resolve({ stopReason: 'end_turn' })
    await running
    assert.deepEqual(h.turns(), ['completed'])
    assert.equal(h.compactions().length, 1)
  },
)

async function runFinalPrefixPassTest(
  t: TestContext,
  mode: 'hold:cancel' | 'hold:cancel-throw',
) {
  const local = deferred<void>()
  const cancelEntered = deferred<void>()
  const h = await harness(t, {
      backgroundProbeTimeoutMs: 2000,
      hasActiveBackgroundWork: async () => {
        cancelEntered.resolve()
        await local.promise
        return false
      },
    }),
    held = await h.holdLive(mode)
  h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
  const interrupting = h.manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt',
    sessionId: 's',
  })
  await cancelEntered.promise
  await h.write([user(1), model(2, 70207), checkpoint(3)], '{"step_index":4')
  assert.deepEqual(h.turns(), [])
  local.resolve()
  await Promise.all([held.running, interrupting])
  assert.equal(h.latest().contextWindowUsedTokens, 70208)
  assert.equal(h.latest().inputTokens, 1000)
  assert.equal(h.compactions().length, 1)
  assert.equal(h.session.state.lastKnownStepIndex, 3)
  assert.deepEqual(h.turns(), ['canceled'])
  assert.equal(held.observation.phase, 'disposed')
}

test(
  'cancel final pass waits for observable local work and preserves complete prefix before a partial tail',
  { timeout: 5000 },
  async (t) => {
    await runFinalPrefixPassTest(t, 'hold:cancel')
  },
)

test(
  'throw final pass waits for observable local work and preserves complete prefix before a partial tail',
  { timeout: 5000 },
  async (t) => {
    await runFinalPrefixPassTest(t, 'hold:cancel-throw')
  },
)

test(
  'close claims one terminal and unblocks a held RPC without waiting for it before transport close',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(2, 100)])
    const observation = held.observation
    const read = observation.poll()
    await h.manager.close()
    await read
    await held.running
    const count = h.events.length
    held.done.resolve({ stopReason: 'end_turn' })
    await observation.poll()
    assert.equal(h.events.length, count)
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(observation.phase, 'disposed')
  },
)

test('rewrite reconciliation permits a lower eligible snapshot without flashing partial old context', () => {
  const observed: number[] = []
  const usage = new ModelUsageObserver(9, (step) => {
    observed.push(step.inputTokens + step.outputTokens)
  })
  usage.observe({
    rows: [model(100, 100)],
    reset: true,
    snapshotStepIndices: new Set([9, 100]),
  })
  usage.observe({ rows: [], reset: true })
  usage.observe({ rows: [model(12, 12)], reset: false })
  assert.deepEqual(observed, [101])
  usage.observe({
    rows: [],
    reset: false,
    snapshotStepIndices: new Set([9, 12]),
  })
  assert.deepEqual(observed, [101, 13])
  usage.observe({
    rows: [{ ...model(12, 12), output_tokens: 2 }],
    reset: false,
  })
  assert.deepEqual(observed, [101, 13, 14])
})

test('parallel identical anonymous tools retain their slot identities and explicit fallback waits for RPC boundary', () => {
  const p = new BrainTranscriptProjector('s'),
    ids: string[] = []
  p.bindToolCallIds(ids)
  p.resolveToolIdentities(true)
  const row = {
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    content: 'text',
    tool_calls: [
      { name: 'run_command', args: { CommandLine: 'same' } },
      { name: 'run_command', args: { CommandLine: 'same' } },
    ],
  }
  assert.deepEqual(
    p.project(row).map((item) => item.type),
    ['assistant_message'],
  )
  p.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'DONE',
    content: 'first',
  })
  p.project({
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: 'second',
  })
  ids.push('first-native')
  const first = p.resolveToolIdentities(true)
  assert.ok(
    first.some(
      (item) =>
        item.type === 'tool_call' &&
        item.callId === 'first-native' &&
        item.status === 'completed',
    ),
  )
  assert.equal(
    first.some(
      (item) => item.type === 'tool_call' && item.callId.includes('tool-'),
    ),
    false,
  )
  const second = p.resolveToolIdentities(false)
  assert.ok(
    second.some(
      (item) =>
        item.type === 'tool_call' &&
        item.callId === 'tool-1-1' &&
        item.status === 'completed',
    ),
  )
})

test(
  'native foreground priority ends at wakeup, retains same-model capacity and never bills rewritten snapshots twice',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    await h.prompt('usage-native')
    const held = await h.hold()
    // The mock native runtime emits a real ACP usage_update into the held logical foreground turn.
    await h.nativePrompt({
      sessionId: h.session.state.nativeId,
      prompt: [{ type: 'text', text: 'usage-native' }],
    })
    await h.session.drain()
    assert.equal(h.latest().contextWindowUsedTokens, 1234)
    const rows = [
      user(20),
      model(21, 15000, 'initial'),
      {
        step_index: 22,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'bg', name: 'run_command', args: {} }],
      },
      {
        step_index: 23,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: tracked',
      },
      model(24, 15000, 'waiting'),
    ]
    await h.write(rows)
    await h.read()
    assert.equal(
      h.latest().contextWindowUsedTokens,
      1234,
      'same foreground native snapshot remains authoritative',
    )
    held.done.resolve({ stopReason: 'end_turn' })
    await held.observation.attached.promise
    const wake = [
      {
        step_index: 25,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\n[Message] sender=tracked priority=MESSAGE_PRIORITY_HIGH content=Task id "tracked" finished with result:\nok\n</SYSTEM_MESSAGE>',
      },
      model(26, 90000, 'wakeup'),
      {
        step_index: 27,
        type: 'PLANNER_RESPONSE',
        status: 'RUNNING',
        thinking: 'more work',
      },
    ]
    await h.append(wake)
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 90001)
    assert.equal(h.latest().contextWindowMaxTokens, 128000)
    await h.write([...rows, ...wake])
    await h.read()
    await h.append([model(28, 40000, 'compacted response')])
    await held.running
    assert.equal(h.latest().contextWindowUsedTokens, 40001)
    assert.equal(h.latest().contextWindowMaxTokens, 128000)
    assert.equal(
      h.session.state.wakeup.carriedUsage?.inputTokens,
      130000,
      'step26 is counted once across replay',
    )
  },
)

// Issue #52: the same wakeup can arrive on stdio and in the transcript.
test(
  'live wakeup keeps one timeline, aligns tools past an unstreamed slot and bills the wakeup',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t, { agyVersion: '1.5.12' })
    const held = await h.hold()
    const stream = (text: string) =>
      h.nativePrompt({
        sessionId: h.session.state.nativeId,
        prompt: [{ type: 'text', text }],
      })
    const items = () =>
      h.events.flatMap((event) =>
        event.type === 'timeline.item' ? [event.item] : [],
      )
    await h.write([
      user(1),
      {
        ...model(2, 20, 'Waiting'),
        tool_calls: [{ id: 'background', name: 'run_command', args: {} }],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: tracked',
      },
    ])
    held.done.resolve({
      stopReason: 'end_turn',
      usage: { inputTokens: 20, outputTokens: 1, totalTokens: 21 },
    })
    await held.observation.attached.promise
    await h.append([
      {
        step_index: 4,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\n[Message] sender=tracked priority=MESSAGE_PRIORITY_HIGH content=Task id "tracked" finished with result:\nok\n</SYSTEM_MESSAGE>',
      },
      {
        ...model(5, 30, ''),
        thinking: 'Checking files',
        tool_calls: ['invalid.txt', 'first.txt', 'second.txt'].map((Path) => ({
          name: 'default_api:read_file',
          args: { Path },
        })),
      },
    ])
    await h.read()
    assert.equal(items().filter((item) => item.type === 'reasoning').length, 0)
    const streamingTools = stream('stream-wakeup-tools')
    await h.wait(() => h.permissions().length > 0)
    const [request] = h.permissions()
    assert.equal(request.request.kind, 'tool')
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: request.request.id,
      response: { behavior: 'deny', selectedActionId: 'reject' },
    })
    await streamingTools
    await h.session.drain()
    assert.equal(items().filter((item) => item.type === 'reasoning').length, 1)
    await h.append([
      { step_index: 6, type: 'GENERIC', status: 'INVALID' },
      { step_index: 7, type: 'GENERIC', status: 'ERROR', content: 'denied' },
      {
        step_index: 8,
        type: 'GENERIC',
        status: 'DONE',
        content: 'second result',
      },
    ])
    await h.read()
    await stream('stream-wakeup-reply')
    await h.session.drain()
    await h.append([model(9, 40, 'Wakeup finished.')])
    await held.running
    assert.deepEqual(h.turns(), ['completed'])
    assert.deepEqual(
      items()
        .filter((item) => item.type === 'reasoning')
        .map((item) => item.text),
      ['Checking files'],
    )
    const replies = items().filter((item) => item.type === 'assistant_message')
    assert.equal(new Set(replies.map((item) => item.id)).size, 1)
    assert.equal(replies.at(-1)?.text, '\n\nWakeup finished.')
    const tools = h.toolCalls()
    assert.deepEqual([...new Set(tools.map((item) => item.callId))].sort(), [
      'background',
      'call_first.txt',
      'call_second.txt',
    ])
    assert.equal(
      tools.findLast((item) => item.callId === 'call_first.txt')?.status,
      'canceled',
    )
    assert.ok(
      !tools.some(
        (item) => item.callId === 'call_first.txt' && item.status === 'failed',
      ),
    )
    const second = tools.findLast((item) => item.callId === 'call_second.txt')
    assert.equal(second?.status, 'completed')
    assert.match(JSON.stringify(second?.detail), /second result/)
    assert.equal(h.latest().inputTokens, 90)
    assert.equal(h.latest().contextWindowUsedTokens, 41)
    assert.equal(h.session.state.wakeup.pendingReplay, false)
    await h.prompt('stream-wakeup-next')
    assert.deepEqual(h.turns(), ['completed', 'completed'])
    assert.equal(h.latest().inputTokens, 100)
    assert.equal(h.latest().outputTokens, 5)
    assert.ok(
      items().some(
        (item) =>
          item.type === 'assistant_message' && item.text === 'New turn.',
      ),
    )
  },
)

async function liveWakeupQuestion(t: TestContext) {
  const h = await harness(t, {
    agyVersion: '1.5.10',
    hasActiveBackgroundWork: async () => false,
  })
  const held = await h.hold()
  await h.write([
    user(1),
    model(2, 20, 'Waiting'),
    {
      step_index: 3,
      type: 'SYSTEM_MESSAGE',
      content:
        '[Message] sender=worker priority=MESSAGE_PRIORITY_HIGH content=ready',
    },
    {
      ...model(4, 30, ''),
      tool_calls: [
        {
          name: 'ask_question',
          args: {
            questions: [{ question: 'Choose a file', options: ['one', 'two'] }],
          },
        },
      ],
    },
  ])
  held.done.resolve({ stopReason: 'end_turn' })
  await held.observation.attached.promise
  await h.read()
  assert.equal(h.permissions().length, 0)
  assert.deepEqual(h.turns(), [])
  const native = h.nativePrompt({
    sessionId: h.session.state.nativeId,
    prompt: [{ type: 'text', text: 'stream-wakeup-question' }],
  })
  await h.wait(() => h.permissions().length > 0)
  const [question] = h.permissions()
  assert.equal(question.request.kind, 'question')
  const replies = () =>
    h.events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'assistant_message'
        ? [event.item.text]
        : [],
    )
  return { h, held, native, permissionId: question.request.id, replies }
}

test(
  'live wakeup answer uses the native elicitation without a synthetic prompt',
  { timeout: 5000 },
  async (t) => {
    const { h, held, native, permissionId, replies } =
      await liveWakeupQuestion(t)
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { 'Choose a file': 'one' } },
      },
    })
    await native
    await h.session.drain()
    await h.append([
      { step_index: 5, type: 'GENERIC', status: 'DONE', content: 'one' },
      model(6, 40, 'Answer: accept'),
    ])
    await held.running
    assert.deepEqual(h.turns(), ['completed'])
    assert.equal(h.permissions().length, 1)
    assert.deepEqual(replies(), ['Answer: accept'])
  },
)

test(
  'live wakeup decline cancels the turn without a synthetic prompt',
  { timeout: 5000 },
  async (t) => {
    const { h, held, native, permissionId, replies } =
      await liveWakeupQuestion(t)
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId,
      response: { behavior: 'deny' },
    })
    await native
    await h.session.drain()
    await held.running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(h.permissions().length, 1)
    assert.deepEqual(replies(), [])
  },
)

test(
  'live wakeup interrupt during a native question cancels the turn',
  { timeout: 5000 },
  async (t) => {
    const { h, held, native, replies } = await liveWakeupQuestion(t)
    await h.manager.handle({
      type: 'session.interrupt',
      sessionId: 's',
      requestId: 'interrupt',
    })
    await native
    await h.session.drain()
    await held.running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(h.permissions().length, 1)
    assert.deepEqual(replies(), [])
  },
)

test(
  'late disk writes beyond cancel observation remain unclaimed, cannot bill a new turn and can restore later',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.holdLive('hold:cancel')
    await h.write([user(1), model(2, 10)])
    await h.read()
    await h.manager.handle({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await held.running
    const count = h.events.length
    await h.append([model(3, 100), checkpoint(4)])
    await held.observation.poll()
    assert.equal(
      h.events.length,
      count,
      'no post-terminal polling or invented flush guarantee',
    )
    const usageCount = h.events.filter(
      (event) => event.type === 'session.usage',
    ).length
    await h.prompt('cancel-only')
    assert.equal(
      h.events.filter((event) => event.type === 'session.usage').length,
      usageCount,
    )
    const nativeId = h.session.state.nativeId
    await h.manager.handle({
      type: 'session.close',
      sessionId: 's',
      requestId: 'close',
    })
    await h.manager.handle({
      type: 'session.open',
      sessionId: 'restored',
      requestId: 'restore',
      history: 'skip',
      config: {
        cwd: h.home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
      persistence: { version: 1, data: { sessionId: nativeId } },
    })
    assert.equal(h.latest().contextWindowUsedTokens, 101)
    assert.equal(
      h.compactions().length,
      0,
      'skip history cannot invent a late live checkpoint',
    )
  },
)

test(
  'same-text questions in separate continuation occurrences remain separate, with close unblocking the second wait',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      continued = deferred<Result>(),
      entered = deferred<void>()
    const args = { questions: [{ question: 'Choose', options: ['A', 'B'] }] }
    h.setPromptHandler(
      sequenceDouble<[], Promise<Result>>([
        async () => {
          await h.write([
            user(1),
            model(2, 10, 'waiting'),
            {
              step_index: 3,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=system priority=MESSAGE_PRIORITY_HIGH content=wake',
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [{ id: 'first-ask', name: 'ask_question', args }],
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
        async () => {
          entered.resolve()
          return continued.promise
        },
      ]),
    )
    const running = h.prompt()
    await h.wait(() => h.permissions().length === 1)
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: h.permissions()[0].request.id,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { Choose: 'A' } },
      },
    })
    await entered.promise
    await h.append([
      user(5),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'second-ask', name: 'ask_question', args }],
      },
    ])
    await h.read()
    assert.equal(
      h.permissions().length,
      1,
      'no recursive question action while continuation is in flight',
    )
    continued.resolve({ stopReason: 'end_turn' })
    await h.wait(() => h.permissions().length === 2)
    assert.notEqual(
      h.permissions()[0].request.id,
      h.permissions()[1].request.id,
    )
    await h.manager.close()
    await running
    assert.deepEqual(h.turns(), ['canceled'])
  },
)

test(
  'child late usage and checkpoint stay child-owned and do not restart a completed child or change parent context',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(2, 10)])
    await h.read()
    const child = '22222222-2222-4222-8222-222222222222',
      dir = join(h.session.brainRoot, child, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    const path = join(dir, 'transcript_full.jsonl'),
      events: ProviderEvent[] = []
    await writeFile(path, encode([model(1, 100, 'child complete')]))
    const watcher = watchBrain(
      h.session.brainRoot,
      child,
      'child',
      (event) => events.push(event),
      60_000,
    )
    try {
      await watcher.ready
      assert.equal(watcher.isRunning(), false)
      await writeFile(
        path,
        encode([model(1, 200, 'child complete'), checkpoint(2)]) +
          '{"step_index":3',
      )
      await watcher.cancel()
      const usage = events.findLast((event) => event.type === 'session.usage')
      assert.ok(usage?.type === 'session.usage')
      assert.deepEqual(usage.usage, { contextWindowUsedTokens: 201 })
      assert.equal(usage.sessionId, 'child')
      assert.deepEqual(
        events
          .filter((event) => event.type === 'session.turn')
          .map((event) => event.state),
        ['started', 'completed'],
      )
      assert.ok(
        events.some(
          (event) =>
            event.type === 'timeline.item' &&
            event.item.type === 'compaction' &&
            event.sessionId === 'child',
        ),
      )
      assert.equal(h.latest().contextWindowUsedTokens, 11)
      assert.equal(h.compactions().length, 0)
    } finally {
      watcher()
      held.done.resolve({ stopReason: 'end_turn' })
      await held.running
    }
  },
)

test(
  'confirmed lower-step rewrite updates live context and the next turn boundary after complete-prefix delivery',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(100, 100)])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 101)
    await h.write([])
    await h.read()
    await h.write([user(1), model(12, 12)], '{"step_index":13')
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 101)
    await appendFile(h.path, ',"type":"CHECKPOINT","status":"DONE"}\n')
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 13)
    assert.equal(h.session.state.lastKnownStepIndex, 13)
    held.done.resolve({ stopReason: 'end_turn' })
    await held.running
    h.setPromptHandler(async () => {
      await h.write([user(14), model(15, 23)])
      return { stopReason: 'end_turn' as const }
    })
    await h.prompt()
    assert.equal(h.latest().contextWindowUsedTokens, 24)
    assert.deepEqual(h.turns(), ['completed', 'completed'])
  },
)

test(
  'raw handoff retains only current-turn rows from a large prior history and releases the staged buffer',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    const rows = Array.from({ length: 2000 }, (_, index) =>
      model(index, index + 1, 'old history'),
    )
    await h.write([...rows, user(2000), model(2001, 5)])
    const readings: number[] = []
    const observer = new BrainTurnObservation(
      h.session.brainRoot,
      h.session.state.nativeId,
      's',
      1999,
      {
        isCurrent: () => true,
        onUsage: (usage) => {
          readings.push(usage.inputTokens)
        },
        onItem: () => {},
      },
    )
    try {
      await observer.poll()
      let delivered: Row[] = []
      await observer.attach(async (read) => {
        delivered = read.rows
      })
      assert.deepEqual(
        delivered.map((row) => row.step_index),
        [2000, 2001],
      )
      assert.deepEqual(readings, [5])
      await observer.attach(async (read) => {
        assert.deepEqual(read.rows, [])
      })
    } finally {
      await observer.dispose()
    }
  },
)

test(
  'same-native runtime replacement invalidates a read already in flight without close/RPC dependency cycles',
  { timeout: 5000 },
  async (t) => {
    const reader = new SwappableTranscriptReader()
    const h = await harness(t, {
        createTranscriptReader: () => reader,
        observationPollIntervalMs: 60_000,
        hasActiveBackgroundWork: async () => true,
      }),
      held = await h.holdLive('hold:cancel')
    await h.read()
    await h.write([user(1), model(2, 100), checkpoint(3)])
    const entered = deferred<void>(),
      release = deferred<void>(),
      baseRead = reader.impl
    reader.impl = async (root, id) => {
      const result = await baseRead(root, id)
      entered.resolve()
      await release.promise
      return result
    }
    const pass = held.observation.poll()
    await entered.promise
    const oldTransport = h.session.transport,
      nativeId = h.session.state.nativeId
    await h.manager.handle({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    assert.equal(h.session.state.nativeId, nativeId)
    assert.notEqual(h.session.transport, oldTransport)
    const count = h.events.filter(
      (event) =>
        event.type === 'session.usage' ||
        (event.type === 'timeline.item' && event.item.type === 'compaction'),
    ).length
    release.resolve()
    reader.impl = baseRead
    await pass
    assert.equal(
      h.events.filter(
        (event) =>
          event.type === 'session.usage' ||
          (event.type === 'timeline.item' && event.item.type === 'compaction'),
      ).length,
      count,
    )
    await held.running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(held.observation.phase, 'disposed')
    await h.prompt('cancel-only')
    assert.deepEqual(h.turns(), ['canceled', 'canceled'])
  },
)

test(
  'non-cancel RPC failure retains final observed context/checkpoint and one failed terminal',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(2, 90), checkpoint(3)], '{"step_index":4')
    held.done.reject(new Error('specific native failure'))
    await held.running
    assert.deepEqual(h.turns(), ['failed'])
    const failed = h.events.find(
      (event) => event.type === 'session.turn' && event.state === 'failed',
    )
    assert.ok(failed?.type === 'session.turn' && failed.state === 'failed')
    assert.equal(failed.error?.message, 'specific native failure')
    assert.equal(h.latest().contextWindowUsedTokens, 91)
    assert.equal(h.compactions().length, 1)
  },
)

async function runNoLfEofSnapshotTest(
  t: TestContext,
  initialRows: Row[],
  mode: 'hold:cancel' | 'hold:cancel-throw',
) {
  const h = await harness(t),
    held = await h.holdLive(mode)
  h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
  await h.write(initialRows)
  await h.read()
  assert.equal(h.compactions().length, 1)
  await appendFile(
    h.path,
    JSON.stringify({
      step_index: 4,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      input_tokens: 70,
      output_tokens: 3,
      content: 'newest',
    }),
  )
  await h.read()
  const streamingIndex = held.observation.lastStepIndex
  assert.equal(streamingIndex, 3, 'no-LF EOF is not a delivered streaming row')
  await h.manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt',
    sessionId: 's',
  })
  await held.running
  assert.equal(h.latest().contextWindowUsedTokens, 73)
  assert.equal(h.session.state.lastKnownStepIndex, 4)
  assert.equal(
    h.latest().inputTokens,
    1000,
    'context observation does not add a bill',
  )
  assert.equal(h.compactions().length, 1)
  assert.deepEqual(h.turns(), ['canceled'])
}

test(
  'final drain prefers complete no-LF EOF snapshot (prior observed usage=false, throw=false)',
  { timeout: 5000 },
  async (t) => {
    await runNoLfEofSnapshotTest(t, [user(1), checkpoint(3)], 'hold:cancel')
  },
)

test(
  'final drain prefers complete no-LF EOF snapshot (prior observed usage=false, throw=true)',
  { timeout: 5000 },
  async (t) => {
    await runNoLfEofSnapshotTest(
      t,
      [user(1), checkpoint(3)],
      'hold:cancel-throw',
    )
  },
)

test(
  'final drain prefers complete no-LF EOF snapshot (prior observed usage=true, throw=false)',
  { timeout: 5000 },
  async (t) => {
    await runNoLfEofSnapshotTest(
      t,
      [user(1), model(2, 10), checkpoint(3)],
      'hold:cancel',
    )
  },
)

test(
  'final drain prefers complete no-LF EOF snapshot (prior observed usage=true, throw=true)',
  { timeout: 5000 },
  async (t) => {
    await runNoLfEofSnapshotTest(
      t,
      [user(1), model(2, 10), checkpoint(3)],
      'hold:cancel-throw',
    )
  },
)

test(
  'normal completion reconciles a newer complete no-LF snapshot without projecting its row',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(2, 10), checkpoint(3)])
    await h.read()
    await appendFile(
      h.path,
      JSON.stringify({
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        input_tokens: 70,
        output_tokens: 3,
        content: 'withheld',
      }),
    )
    held.done.resolve({ stopReason: 'end_turn' })
    await held.running
    assert.equal(h.latest().contextWindowUsedTokens, 73)
    assert.equal(h.session.state.lastKnownStepIndex, 4)
    assert.deepEqual(h.turns(), ['completed'])
    assert.equal(h.compactions().length, 1)
    assert.equal(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === 'withheld',
      ),
      false,
    )
  },
)

test(
  'final no-LF measurement advances boundary without overwriting authoritative foreground native context',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    const held = await h.hold()
    await h.nativePrompt({
      sessionId: h.session.state.nativeId,
      prompt: [{ type: 'text', text: 'usage-native' }],
    })
    await h.session.drain()
    const native = h.latest()
    assert.equal(native.contextWindowUsedTokens, 1234)
    await h.write([user(20), model(21, 10), checkpoint(22)])
    await h.read()
    await appendFile(
      h.path,
      JSON.stringify({
        step_index: 23,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        input_tokens: 70,
        output_tokens: 3,
      }),
    )
    held.done.resolve({ stopReason: 'cancelled' })
    await held.running
    assert.equal(h.latest().contextWindowUsedTokens, 1234)
    assert.equal(h.latest().contextWindowMaxTokens, 128000)
    assert.equal(h.session.state.lastKnownStepIndex, 23)
    assert.deepEqual(h.turns(), ['canceled'])
  },
)

async function runFinalTailTest(
  t: TestContext,
  mutateTail: (path: string) => Promise<void>,
  expectedLastStepIndex: number,
) {
  const h = await harness(t),
    held = await h.hold()
  await h.write([user(1), model(4, 80)])
  await h.read()
  await mutateTail(h.path)
  held.done.resolve({ stopReason: 'cancelled' })
  await held.running
  assert.equal(h.latest().contextWindowUsedTokens, 81)
  assert.equal(h.session.state.lastKnownStepIndex, expectedLastStepIndex)
  assert.deepEqual(h.turns(), ['canceled'])
}

test(
  'final older tail preserves observed context while reconciling only complete boundaries',
  { timeout: 5000 },
  async (t) => {
    await runFinalTailTest(
      t,
      (path) => writeFile(path, JSON.stringify(model(2, 10))),
      4,
    )
  },
)

test(
  'final missing tail preserves observed context while reconciling only complete boundaries',
  { timeout: 5000 },
  async (t) => {
    await runFinalTailTest(t, (path) => rm(path), 4)
  },
)

test(
  'final higher-nonusage tail preserves observed context while reconciling only complete boundaries',
  { timeout: 5000 },
  async (t) => {
    await runFinalTailTest(
      t,
      (path) =>
        appendFile(
          path,
          JSON.stringify({
            step_index: 6,
            type: 'GENERIC',
            status: 'CANCELED',
          }),
        ),
      6,
    )
  },
)

test(
  'already closing turn skips final snapshot and pending local interruption without waiting for the RPC',
  { timeout: 5000 },
  async (t) => {
    const local = deferred<void>()
    const h = await harness(t, {
        hasActiveBackgroundWork: async () => {
          await local.promise
          return false
        },
      }),
      held = await h.holdLive('hold:never')
    const interrupting = h.manager.handle({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await h.manager.close()
    await held.running
    assert.deepEqual(h.turns(), ['canceled'])
    const nonRequestEvents = () =>
      h.events.filter((e) => e.type !== 'request.completed')
    const count = nonRequestEvents().length
    local.resolve()
    await interrupting
    await held.observation.poll()
    assert.equal(nonRequestEvents().length, count)
  },
)

test(
  'a recovered reader fault cannot poison a real pending question continuation',
  { timeout: 5000 },
  async (t) => {
    const reader = new SwappableTranscriptReader()
    const h = await harness(t, {
        observationPollIntervalMs: 60_000,
        createTranscriptReader: () => reader,
      }),
      entered = deferred<void>(),
      continued = deferred<Result>()
    const args = { questions: [{ question: 'Choose', options: ['A', 'B'] }] }
    h.setPromptHandler(
      sequenceDouble<[], Promise<Result>>([
        async () => {
          await h.write([
            user(1),
            model(2, 10, 'waiting'),
            {
              step_index: 3,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=system priority=MESSAGE_PRIORITY_HIGH content=wake',
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [{ id: 'ask', name: 'ask_question', args }],
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
        async () => {
          entered.resolve()
          return continued.promise
        },
      ]),
    )
    const running = h.prompt()
    await h.wait(() => h.events.some((e) => e.type === 'session.permission'))
    const question = h.events.find((e) => e.type === 'session.permission')
    assert.ok(question?.type === 'session.permission')
    const obs = h.session.state.observation!
    await obs.poll()
    reader.injectOnce(
      Object.assign(new Error('synthetic transient EBUSY'), {
        code: 'EBUSY',
      }),
    )
    await assert.rejects(obs.poll(), /synthetic transient EBUSY/)
    await obs.poll()
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: question.request.id,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { Choose: 'A' } },
      },
    })
    const phase = await Promise.race([
      entered.promise.then(() => 'entered'),
      running.then(() => 'finished'),
    ])
    assert.equal(phase, 'entered')
    await h.append([user(5), model(6, 40, 'answer')])
    await obs.poll()
    continued.resolve({ stopReason: 'end_turn' })
    await running
    assert.deepEqual(h.turns(), ['completed'])
  },
)

async function pendingBrainQuestion(
  t: TestContext,
  options?: {
    promptText?: 'brain-question-pending' | 'brain-question-ignore-cancel'
    cancelConvergenceTimeoutMs?: number
    observationPollIntervalMs?: number
    createTranscriptReader?: () => TranscriptReader
  },
) {
  const h = await harness(t, {
    cancelConvergenceTimeoutMs: options?.cancelConvergenceTimeoutMs,
    observationPollIntervalMs: options?.observationPollIntervalMs,
    createTranscriptReader: options?.createTranscriptReader,
  })
  const running = h.prompt(options?.promptText ?? 'brain-question-pending')
  await h.wait(() =>
    h.events.some((event) => event.type === 'session.permission'),
  )
  const question = h.events.find((event) => event.type === 'session.permission')
  assert.ok(question?.type === 'session.permission')
  await h.session.state.observation!.poll()
  const answer = () =>
    h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: question.request.id,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { Choose: 'A' } },
      },
    })
  return { ...h, running, answer }
}

async function setupReaderFaultQuestion(t: TestContext) {
  const reader = new SwappableTranscriptReader()
  const h = await pendingBrainQuestion(t, {
      createTranscriptReader: () => reader,
      observationPollIntervalMs: 60_000,
    }),
    obs = h.session.state.observation!
  let reads = 0
  reader.impl = async () => {
    throw Object.assign(new Error(`unavailable reader ${++reads}`), {
      code: 'EBUSY',
    })
  }
  await assert.rejects(obs.poll(), /unavailable reader 1/)
  await assert.rejects(obs.poll(), /unavailable reader 2/)
  assert.match(String(obs.error), /unavailable reader 2/)
  return { h, obs }
}

async function assertReaderFaultPreservedTermination(
  h: Awaited<ReturnType<typeof pendingBrainQuestion>>,
  obs: NonNullable<
    Awaited<
      ReturnType<typeof pendingBrainQuestion>
    >['session']['state']['observation']
  >,
  expectedTurn: 'failed' | 'canceled',
) {
  await h.running
  assert.deepEqual(h.turns(), [expectedTurn])
  assert.equal(
    h.events.some(
      (event) =>
        event.type === 'session.commands' &&
        event.commands[0]?.name === 'continuation-entered',
    ),
    false,
    'no continuation RPC starts while the reader is still failing',
  )
  const count = h.events.length
  await obs.poll()
  assert.equal(h.events.length, count)
}

test(
  'an unresolved reader I/O fault preserves failure question termination',
  { timeout: 5000 },
  async (t) => {
    const { h, obs } = await setupReaderFaultQuestion(t)
    await h.answer()
    await assertReaderFaultPreservedTermination(h, obs, 'failed')
    const terminal = h.events.findLast(
      (event) => event.type === 'session.turn' && event.state === 'failed',
    )
    assert.ok(terminal?.type === 'session.turn' && terminal.state === 'failed')
    assert.match(terminal.error?.message ?? '', /unavailable reader/)
  },
)

test(
  'an unresolved reader I/O fault preserves cancel question termination',
  { timeout: 5000 },
  async (t) => {
    const { h, obs } = await setupReaderFaultQuestion(t)
    await h.manager.handle({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await assertReaderFaultPreservedTermination(h, obs, 'canceled')
  },
)

test(
  'an unresolved reader I/O fault preserves close question termination',
  { timeout: 5000 },
  async (t) => {
    const { h, obs } = await setupReaderFaultQuestion(t)
    await h.manager.close()
    await assertReaderFaultPreservedTermination(h, obs, 'canceled')
  },
)

test(
  'reader recovery cannot erase an actual Brain terminal failure',
  { timeout: 5000 },
  async (t) => {
    const reader = new SwappableTranscriptReader()
    const h = await harness(t, {
        observationPollIntervalMs: 60_000,
        createTranscriptReader: () => reader,
      }),
      held = await h.hold(),
      obs = held.observation
    await obs.poll()
    reader.injectOnce(
      Object.assign(new Error('temporary I/O'), { code: 'EBUSY' }),
    )
    await assert.rejects(obs.poll(), /temporary I\/O/)
    await h.write([
      user(1),
      {
        step_index: 2,
        type: 'ERROR_MESSAGE',
        content: 'actual native model failure',
      },
    ])
    await obs.poll()
    assert.equal(obs.error, undefined)
    held.done.resolve({ stopReason: 'end_turn' })
    await held.running
    assert.deepEqual(h.turns(), ['failed'])
    const terminal = h.events.findLast(
      (event) => event.type === 'session.turn' && event.state === 'failed',
    )
    assert.ok(terminal?.type === 'session.turn' && terminal.state === 'failed')
    assert.match(terminal.error?.message ?? '', /actual native model failure/)
  },
)

test(
  'cancel convergence timeout relaunches the real mock transport and clears continuation flags',
  { timeout: 5000 },
  async (t) => {
    const h = await pendingBrainQuestion(t, {
      promptText: 'brain-question-ignore-cancel',
      cancelConvergenceTimeoutMs: 50,
    })
    await h.answer()
    await h.wait(() =>
      h.events.some(
        (event) =>
          event.type === 'session.commands' &&
          event.commands[0]?.name === 'continuation-entered',
      ),
    )
    const oldTransport = h.session.transport
    await h.manager.handle({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await h.running
    assert.notEqual(h.session.transport, oldTransport)
    assert.equal(h.session.state.suppressContinuationAcp, false)
    assert.equal(h.session.state.inSettle, false)
    assert.deepEqual(h.turns(), ['canceled'])
    await h.prompt('cancel-only')
    assert.deepEqual(h.turns(), ['canceled', 'canceled'])
  },
)

test(
  'Orchestrator planexecute intra-turn handoffs keep exact step occupancy without false wokeUp double-billing, while woken handback and Turn 2 carriedWakeup settle cleanly',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    h.session.state.metadata.configOptions = [
      {
        id: 'model',
        name: 'Model',
        type: 'select',
        currentValue: 'gemini-pro-orchestrator',
        options: [
          {
            value: 'gemini-pro-orchestrator',
            name: 'Orchestrator (Carbon)',
          },
        ],
      },
    ]
    const askArgs = {
      questions: [{ question: 'Deploy target?', options: ['Staging', 'Prod'] }],
    }
    h.session.state.handledElicitations.push({
      questions: normalizeAskQuestionInput(askArgs),
      answerText: 'Staging',
      wokeUp: false,
    })

    const held = await h.hold()
    h.session.state.turnToolCallIds.push('acp-ask', 'acp-exit', 'acp-cmd')
    h.session.timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'acp-ask',
      title: 'ask_question',
      status: 'completed',
      rawInput: askArgs,
      rawOutput: 'Staging',
    })
    h.session.timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'acp-exit',
      title: 'Run contrib:exit_plan_mode',
      kind: 'other',
      status: 'completed',
      rawInput: { plan: '## Plan\n1. Run suite' },
      rawOutput: 'Plan approved',
    })
    h.session.timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'acp-cmd',
      title: 'run_command',
      status: 'in_progress',
      rawInput: { CommandLine: 'npm test' },
    })

    // Foreground Planner asks a question (latching foregroundIdleReached = true) and calls contrib:exit_plan_mode
    await h.write([
      user(1),
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 40_000,
        input_tokens: 2_000,
        output_tokens: 300,
        tool_calls: [{ id: 'acp-ask', name: 'ask_question', args: askArgs }],
      },
      { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'Staging' },
      {
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 42_000,
        input_tokens: 3_000,
        output_tokens: 800,
        tool_calls: [
          {
            id: 'acp-exit',
            name: 'contrib:exit_plan_mode',
            args: { plan: '## Plan\n1. Run suite' },
          },
        ],
      },
      {
        step_index: 5,
        type: 'GENERIC',
        status: 'DONE',
        content: 'Plan approved',
      },
    ])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 45_800)
    assert.equal(h.latest().contextWindowMaxTokens, undefined)

    // Intra-turn planexecute handoff to Hydrogen + KV-cache miss + RUNNING background task
    await h.append([
      {
        step_index: 6,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\nPlan mode is off: this turn runs on the execution model.\n</SYSTEM_MESSAGE>',
      },
      {
        step_index: 7,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 0,
        input_tokens: 46_200,
        output_tokens: 1_100,
        content: 'Starting background suite',
        tool_calls: [
          {
            id: 'acp-cmd',
            name: 'run_command',
            args: { CommandLine: 'npm test' },
          },
        ],
      },
      {
        step_index: 8,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: tracked',
      },
      {
        step_index: 9,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 47_300,
        input_tokens: 400,
        output_tokens: 100,
        content: 'Waiting for suite completion',
      },
    ])
    await h.read()
    assert.equal(h.latest().contextWindowUsedTokens, 47_800)
    assert.equal(h.latest().contextWindowMaxTokens, undefined)
    assert.equal(held.observation.wokeUp, false)

    // Foreground RPC ends with cumulative multi-model totals; turn stays open awaiting background task
    held.done.resolve({
      stopReason: 'end_turn',
      usage: {
        inputTokens: 51_600,
        cachedReadTokens: 129_300,
        outputTokens: 2_300,
        totalTokens: 183_200,
      },
    })
    await held.observation.attached.promise
    await h.read()
    assert.equal(held.observation.phase, 'settling')
    assert.deepEqual(h.turns(), [])
    assert.equal(
      held.observation.wokeUp,
      false,
      'intra-turn planexecute handoff after foreground ask_question must not trigger wokeUp',
    )
    assert.equal(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          (event.item.text === 'Starting background suite' ||
            event.item.text === 'Waiting for suite completion'),
      ),
      false,
      'foreground Hydrogen text must not be duplicated by Brain before wakeup',
    )

    // Post-RPC Reactive Wakeup -> [plan review due] -> contrib:enter_plan_mode -> [plan review] Planner handback
    await h.append([
      {
        step_index: 10,
        type: 'SYSTEM_MESSAGE',
        content:
          '[Message] sender=tracked content=Task id "tracked" finished with result:\nall green',
      },
      {
        step_index: 11,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\n[plan review due] Summarize verification and return to plan mode.\n</SYSTEM_MESSAGE>',
      },
      {
        step_index: 12,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 47_800,
        input_tokens: 500,
        output_tokens: 200,
        tool_calls: [
          {
            id: 'woken-enter',
            name: 'contrib:enter_plan_mode',
            args: {},
          },
        ],
      },
      {
        step_index: 13,
        type: 'GENERIC',
        status: 'DONE',
        content: 'Entered plan mode',
      },
      {
        step_index: 14,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\n[plan review] Plan mode is on: this turn runs on the planning model.\n</SYSTEM_MESSAGE>',
      },
      {
        step_index: 15,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 0,
        input_tokens: 48_900,
        output_tokens: 600,
        content: 'Verification complete.',
      },
    ])
    await held.running
    assert.deepEqual(h.turns(), ['completed'])
    assert.equal(h.latest().contextWindowUsedTokens, 49_500)
    assert.equal(h.latest().contextWindowMaxTokens, undefined)
    // Foreground (51,600) + woken steps 12 & 15 only (500 + 48,900 = 49,400); zero double-counting of foreground Hydrogen!
    assert.equal(h.latest().inputTokens, 101_000)
    assert.equal(h.latest().cachedInputTokens, 177_100)
    assert.equal(h.latest().outputTokens, 3_100)
    assert.deepEqual(h.session.state.wakeup.carriedUsage, {
      inputTokens: 49_400,
      cachedReadTokens: 47_800,
      outputTokens: 800,
    })
    assert.ok(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === 'Verification complete.',
      ),
    )

    // Turn 2: agy includes Turn 1 woken tokens in cumulative PromptResponse.usage; carriedWakeup deducts them cleanly
    const held2 = await h.hold()
    await h.append([
      user(16),
      {
        step_index: 17,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        cache_read_tokens: 49_500,
        input_tokens: 1_200,
        output_tokens: 300,
        content: 'Next turn ready.',
      },
    ])
    await h.read()
    held2.done.resolve({
      stopReason: 'end_turn',
      usage: {
        inputTokens: 49_400 + 1_200,
        cachedReadTokens: 47_800 + 49_500,
        outputTokens: 800 + 300,
        totalTokens: 149_000,
      },
    })
    await held2.running
    assert.deepEqual(h.turns(), ['completed', 'completed'])
    assert.equal(h.latest().contextWindowUsedTokens, 51_000)
    assert.equal(h.latest().contextWindowMaxTokens, undefined)
    assert.equal(h.latest().inputTokens, 1_200)
    assert.equal(h.latest().cachedInputTokens, 49_500)
    assert.equal(h.latest().outputTokens, 300)
  },
)

test(
  'Orchestrator intra-turn [plan review due], woken ask_question continuation KV-cache misses, and woken elicitation decline settle cleanly',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t)
    const askArgs = {
      questions: [{ question: 'Approve review?', options: ['Yes', 'No'] }],
    }
    h.setPromptHandler(
      sequenceDouble([
        async () => {
          // Foreground turn with tool-less Hydrogen DONE -> intra-turn [plan review due] -> foreground [Message] while Planner has pending tools -> woken ask_question
          await h.write([
            user(1),
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 10_000,
              output_tokens: 100,
              content: 'Execution done.',
            },
            {
              step_index: 3,
              type: 'SYSTEM_MESSAGE',
              content:
                '<SYSTEM_MESSAGE>\n[plan review due] Return to plan mode.\n</SYSTEM_MESSAGE>',
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 10_500,
              output_tokens: 100,
              tool_calls: [
                { id: 'fg-enter', name: 'contrib:enter_plan_mode', args: {} },
              ],
            },
            { step_index: 5, type: 'GENERIC', status: 'DONE', content: 'ok' },
            {
              step_index: 6,
              type: 'SYSTEM_MESSAGE',
              content:
                '<SYSTEM_MESSAGE>\n[plan review] Plan mode is on: this turn runs on the planning model.\n</SYSTEM_MESSAGE>',
            },
            {
              step_index: 7,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 11_000,
              output_tokens: 200,
              content: 'Foreground planner spawned helper.',
              tool_calls: [
                {
                  id: 'fg-bg',
                  name: 'run_command',
                  args: { CommandLine: 'check' },
                },
              ],
            },
            {
              step_index: 8,
              type: 'GENERIC',
              status: 'RUNNING',
              content: 'Tool is running as a background task with task id: bg1',
            },
            {
              step_index: 9,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 11_200,
              output_tokens: 50,
              content: 'Waiting on helper.',
            },
            // Post-RPC wakeup asks a woken ask_question
            {
              step_index: 10,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=bg1 content=Task id "bg1" finished with result:\nready',
            },
            {
              step_index: 11,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 12_000,
              output_tokens: 100,
              tool_calls: [
                { id: 'woken-q', name: 'ask_question', args: askArgs },
              ],
            },
          ])
          return {
            stopReason: 'end_turn' as const,
            usage: {
              inputTokens: 42_700,
              outputTokens: 450,
              totalTokens: 43_150,
            },
          }
        },
        async () => {
          // Synthetic continuation RPC runs Orchestrator contrib:exit_plan_mode -> Hydrogen 95k KV-cache miss and ends cleanly (no post-continuation wakeup)
          await h.append([
            { step_index: 12, type: 'GENERIC', status: 'DONE', content: 'Yes' },
            user(13),
            {
              step_index: 14,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 13_000,
              output_tokens: 200,
              tool_calls: [
                {
                  id: 'cont-exit',
                  name: 'contrib:exit_plan_mode',
                  args: { plan: '# Ship' },
                },
              ],
            },
            { step_index: 15, type: 'GENERIC', status: 'DONE', content: 'off' },
            {
              step_index: 16,
              type: 'SYSTEM_MESSAGE',
              content:
                '<SYSTEM_MESSAGE>\nPlan mode is off: this turn runs on the execution model.\n</SYSTEM_MESSAGE>',
            },
            {
              step_index: 17,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              cache_read_tokens: 0,
              input_tokens: 95_000,
              output_tokens: 500,
              content: 'Continuation Hydrogen finished.',
            },
          ])
          return {
            stopReason: 'end_turn' as const,
            usage: {
              inputTokens: 120_000,
              outputTokens: 800,
              totalTokens: 120_800,
            },
          }
        },
        async () => {
          // Turn 2: fresh Orchestrator turn reporting 120k inputTokens; must NOT subtract Turn 1's 120k continuation tokens!
          await h.append([
            user(18),
            {
              step_index: 19,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 120_000,
              output_tokens: 400,
              content: 'Turn 2 done.',
            },
          ])
          return {
            stopReason: 'end_turn' as const,
            usage: {
              inputTokens: 120_000,
              outputTokens: 400,
              totalTokens: 120_400,
            },
          }
        },
        async () => {
          // Woken elicitation decline cancels immediately without hanging for stallTimeoutMs
          h.session.state.handledElicitations.push({
            questions: normalizeAskQuestionInput(askArgs),
            answerText: null,
            wokeUp: true,
            declined: true,
          })
          await h.append([
            user(20),
            model(21, 1000, 'idle'),
            {
              step_index: 22,
              type: 'SYSTEM_MESSAGE',
              content:
                '[Message] sender=system priority=MESSAGE_PRIORITY_HIGH content=wake',
            },
            {
              step_index: 23,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                { id: 'declined-q', name: 'ask_question', args: askArgs },
              ],
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
      ]),
    )

    const turn1 = h.prompt('turn 1')
    await h.wait(() =>
      h.events.some((event) => event.type === 'session.permission'),
    )
    const perm = h.events.find((event) => event.type === 'session.permission')
    assert.ok(perm?.type === 'session.permission')
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: perm.request.id,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { 'Approve review?': 'Yes' } },
      },
    })
    await turn1
    assert.deepEqual(h.turns(), ['completed'])
    // Turn 1 bills foreground (42,700) + woken/continuation steps 11, 14, 17 (12,000 + 13,000 + 95,000 = 120,000) = 162,700
    assert.equal(h.latest().inputTokens, 162_700)
    assert.equal(h.latest().contextWindowUsedTokens, 95_500)
    // Because continuation RPC finished cleanly with no post-continuation wakeup, carriedUsage is undefined and pendingReplay is false!
    assert.equal(h.session.state.wakeup.carriedUsage, undefined)
    assert.equal(h.session.state.wakeup.pendingReplay, false)

    // Turn 2 must report full 120,000 inputTokens (zero false deduction)
    await h.prompt('turn 2')
    assert.deepEqual(h.turns(), ['completed', 'completed'])
    assert.equal(h.latest().inputTokens, 120_000)

    await h.prompt('turn 3')
    assert.deepEqual(h.turns(), ['completed', 'completed', 'canceled'])
  },
)
