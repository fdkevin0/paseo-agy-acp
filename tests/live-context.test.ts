import assert from 'node:assert/strict'
import { test, type TestContext } from 'node:test'
import {
  mkdtemp,
  mkdir,
  writeFile,
  rm,
  appendFile,
  readFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  ModelUsageObserver,
  BrainTurnObservation,
} from '../server/brain-observer.ts'
import { TranscriptReader } from '../server/brain-io.ts'
import { watchBrain } from '../server/brain.ts'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { normalizeAskQuestionInput } from '../server/questions.ts'
import {
  checkpointRow,
  encode,
  modelRow,
  userRow,
  type Row,
} from './brain-fixtures.ts'
import {
  compactionEvents,
  createEventCollector,
  deferred,
  HarnessSessionManager,
  isolatedEnv,
  toolCalls as collectToolCalls,
  TrackingObservation,
  wrapConnectAcp,
  type PromptHandler,
  type RpcPrompt,
} from './helpers.ts'

type Result = Awaited<ReturnType<RpcPrompt>>
const user = userRow
const model = modelRow
const checkpoint = (step_index: number, content = 'summary'): Row =>
  checkpointRow(step_index, { content, input_tokens: 999999 })

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

async function harness(
  t: TestContext,
  options?: {
    logAcp?: boolean
    cancelConvergenceTimeoutMs?: number
    hasActiveBackgroundWork?: ConstructorParameters<
      typeof SessionManager
    >[0]['hasActiveBackgroundWork']
    backgroundProbeTimeoutMs?: number
    observationPollIntervalMs?: number
    createTranscriptReader?: () => TranscriptReader
  },
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-live-context-'))
  const collector = createEventCollector()
  const { events } = collector
  let promptHandler: PromptHandler | undefined
  let realPrompt!: RpcPrompt
  let lastObservation!: TrackingObservation
  const acpLog = join(home, 'acp.log')
  const env = isolatedEnv(home)
  const manager = new HarnessSessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: options?.logAcp
        ? {
            ...env,
            AGY_TEST_LOG: acpLog,
            AGY_TEST_LOG_LOAD: '1',
            AGY_TEST_LOG_ELICITATION_CAP: '1',
          }
        : env,
    },
    usage: new AgyUsageSource({ env }),
    capabilities: ['prompt.message', 'permission'],
    emit: collector.emit,
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
  const compactions = () => compactionEvents(events)
  const assistantMessages = () =>
    events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'assistant_message'
        ? [event.item]
        : [],
    )
  const toolCalls = () => collectToolCalls(events)
  const permissions = () =>
    events.flatMap((event) =>
      event.type === 'session.permission' ? [event] : [],
    )
  const wait = (predicate: () => boolean) => collector.waitCondition(predicate)
  const read = async () => {
    const observation = session.state.activeTurn?.observation
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
    acpLog,
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
    assistantMessages,
    toolCalls,
    permissions,
    wait,
    read,
    attached: () => lastObservation.attached.promise,
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
    const activeTurn = h.session.state.activeTurn
    assert.ok(activeTurn)
    activeTurn.handledElicitations.push({
      questions,
      answerText: 'A',
      wokeUp: false,
      declined: false,
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
    assert.equal(activeTurn.handledElicitations.length, 1)
    assert.equal(
      h.events.some(
        (event) =>
          event.type === 'timeline.item' && event.item.type === 'tool_call',
      ),
      false,
    )
    activeTurn.toolCallIds.add('native-command')
    activeTurn.toolCallIds.add('native-question')
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
    assert.equal(activeTurn.handledElicitations.length, 0)
    assert.equal(h.session.state.activeTurn, null)
    assert.equal(
      h.events.some((event) => event.type === 'session.permission'),
      false,
    )
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

test('parallel identical anonymous tools retain their slot identities without emitting a live fallback', () => {
  const p = new BrainTranscriptProjector('s'),
    ids: string[] = [],
    verified = new Set<string>()
  p.bindLiveToolCallIds(ids, verified)
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
  verified.add('first-native')
  const first = p.resolveToolIdentities()
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
  assert.deepEqual(p.resolveToolIdentities(), [])
  ids.push('second-native')
  verified.add('second-native')
  const second = p.resolveToolIdentities()
  assert.deepEqual(
    second
      .filter((item) => item.type === 'tool_call')
      .map(({ callId, status }) => [callId, status]),
    [
      ['second-native', 'running'],
      ['second-native', 'completed'],
    ],
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
      h.latest().inputTokens,
      130000,
      'step26 is counted once across replay',
    )
  },
)

// Issue #52: the same wakeup can arrive on stdio and in the transcript.
test(
  'live wakeup aligns tools past an invalid slot and keeps a native denial canceled',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t, { logAcp: true })
    const running = h.prompt('recorded-wakeup-denied-tool')
    await h.wait(() => h.permissions().length > 0)
    const [request] = h.permissions()
    assert.equal(request.request.kind, 'tool')
    assert.deepEqual(h.turns(), [])
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: request.request.id,
      response: { behavior: 'deny', selectedActionId: 'reject' },
    })
    await running
    assert.deepEqual(h.turns(), ['completed'])
    const replies = h.assistantMessages()
    assert.equal(new Set(replies.map((item) => item.id)).size, 2)
    assert.equal(replies.at(-1)?.text, '\n\nWakeup finished.')
    const tools = h.toolCalls()
    assert.deepEqual(
      [...new Set(tools.map((item) => item.callId))],
      ['call_1', 'call_2', 'call_3'],
    )
    assert.equal(
      tools.findLast((item) => item.callId === 'call_2')?.status,
      'canceled',
    )
    assert.equal(
      tools.some(
        (item) => item.callId === 'call_2' && item.status === 'failed',
      ),
      false,
    )
    const beta = tools.findLast((item) => item.callId === 'call_3')
    assert.equal(beta?.status, 'completed')
    assert.match(JSON.stringify(beta?.detail), /beta/)
    assert.equal(h.latest().inputTokens, 29488)
    assert.equal(h.latest().contextWindowUsedTokens, 41)
    assert.deepEqual(await promptRequests(h.acpLog), [
      'prompt:recorded-wakeup-denied-tool',
    ])
  },
)

const promptRequests = async (log: string) =>
  (await readFile(log, 'utf8'))
    .split('\n')
    .filter((line) => line.startsWith('prompt:'))

async function recordedQuestion(
  t: TestContext,
  options?: Parameters<typeof harness>[1],
  text = 'recorded-wakeup',
) {
  const h = await harness(t, { ...options, logAcp: true })
  const running = h.prompt(text)
  await h.wait(() => h.permissions().length === 1)
  const [question] = h.permissions()
  assert.equal(question.request.kind, 'question')
  return { h, running, question }
}

const answerRecordedQuestion = (
  h: Awaited<ReturnType<typeof harness>>,
  permissionId: string,
) =>
  h.manager.handle({
    type: 'session.permission',
    sessionId: 's',
    permissionId,
    response: {
      behavior: 'allow',
      updatedInput: { answers: { 'Question 1': 'one' } },
    },
  })

test(
  'recorded agy wakeup streams once and bills only the turn that produced it',
  { timeout: 5000 },
  async (t) => {
    const { h, running, question } = await recordedQuestion(t)
    assert.deepEqual(h.turns(), [])
    await answerRecordedQuestion(h, question.request.id)
    await running
    assert.deepEqual(h.turns(), ['completed'])
    const items = h.events
      .filter((event) => event.type === 'timeline.item')
      .map((event) => event.item)
    const replies = items.filter((item) => item.type === 'assistant_message')
    assert.deepEqual(
      replies.map((item) => item.text),
      ['started', '\n\nfinal: one'],
    )
    assert.equal(new Set(replies.map((item) => item.id)).size, 2)
    assert.deepEqual(
      [...new Set(h.toolCalls().map((item) => item.callId))],
      ['call_1', 'call_2', 'call_3', 'call_4'],
    )
    assert.match(
      JSON.stringify(
        h.toolCalls().findLast((item) => item.callId === 'call_2')?.detail,
      ),
      /alpha/,
    )
    assert.match(
      JSON.stringify(
        h.toolCalls().findLast((item) => item.callId === 'call_3')?.detail,
      ),
      /beta/,
    )
    assert.equal(h.latest().inputTokens, 34262)
    await h.prompt('recorded-next')
    assert.deepEqual(h.turns(), ['completed', 'completed'])
    assert.equal(h.latest().inputTokens, 2156)
    assert.deepEqual(await promptRequests(h.acpLog), [
      'prompt:recorded-wakeup',
      'prompt:recorded-next',
    ])
  },
)

test(
  'recorded result-less ACP completions retain read results Brain recovered first',
  { timeout: 5000 },
  async (t) => {
    const { h, running, question } = await recordedQuestion(
      t,
      { observationPollIntervalMs: 60_000 },
      'recorded-wakeup-delayed-reads',
    )
    const lastRead = (callId: string) =>
      h.toolCalls().findLast((item) => item.callId === callId)
    await h.wait(
      () =>
        lastRead('call_2')?.status === 'running' &&
        lastRead('call_3')?.status === 'running',
    )
    await h.session.drain()
    await h.attached()
    await h.session.transport.rpc.setSessionConfigOption({
      sessionId: h.session.state.nativeId,
      configId: 'fixture-write-wakeup-rows',
      value: 'write',
    })
    await h.read()
    assert.equal(lastRead('call_2')?.status, 'completed')
    assert.match(JSON.stringify(lastRead('call_2')?.detail), /alpha/)
    assert.equal(lastRead('call_3')?.status, 'completed')
    assert.match(JSON.stringify(lastRead('call_3')?.detail), /beta/)
    const before = h.toolCalls().length
    await h.session.transport.rpc.setSessionConfigOption({
      sessionId: h.session.state.nativeId,
      configId: 'fixture-release-reads',
      value: 'release',
    })
    await h.session.drain()
    assert.deepEqual(
      h
        .toolCalls()
        .slice(before)
        .map(({ callId, status }) => [callId, status]),
      [
        ['call_2', 'completed'],
        ['call_3', 'completed'],
      ],
    )
    assert.match(JSON.stringify(lastRead('call_2')?.detail), /alpha/)
    assert.match(JSON.stringify(lastRead('call_3')?.detail), /beta/)
    await finishRecordedQuestion(h, question.request.id, running)
    assert.deepEqual(h.turns(), ['completed'])
  },
)

async function finishRecordedQuestion(
  h: Awaited<ReturnType<typeof harness>>,
  permissionId: string,
  running: Promise<void>,
) {
  await answerRecordedQuestion(h, permissionId)
  await h.wait(() =>
    h.events.some(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'assistant_message' &&
        e.item.text === '\n\nfinal: one',
    ),
  )
  await h.read()
  await running
}

async function recordedQuestionWithReaderFault(t: TestContext) {
  const reader = new SwappableTranscriptReader()
  const setup = await recordedQuestion(t, {
    createTranscriptReader: () => reader,
    observationPollIntervalMs: 60_000,
  })
  const obs = setup.h.session.state.activeTurn
    ?.observation as TrackingObservation
  await obs.attached.promise
  await setup.h.read()
  const error = Object.assign(
    new Error('recorded question reader unavailable'),
    { code: 'EBUSY' },
  )
  return { ...setup, obs, reader, error }
}

async function assertFaultedQuestionTerminated(
  h: Awaited<ReturnType<typeof harness>>,
  obs: BrainTurnObservation,
  running: Promise<void>,
  status: 'failed' | 'canceled',
) {
  await running
  assert.deepEqual(h.turns(), [status])
  assert.deepEqual(await promptRequests(h.acpLog), ['prompt:recorded-wakeup'])
  const count = h.events.length
  await obs.poll()
  assert.equal(h.events.length, count)
}

test(
  'a transient reader fault recovers while native elicitation is pending',
  { timeout: 5000 },
  async (t) => {
    const { h, obs, reader, error, question, running } =
      await recordedQuestionWithReaderFault(t)
    reader.injectOnce(error)
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    assert.equal(obs.error, error)
    await obs.poll()
    assert.equal(obs.error, undefined)
    await finishRecordedQuestion(h, question.request.id, running)
    assert.deepEqual(h.turns(), ['completed'])
    assert.deepEqual(await promptRequests(h.acpLog), ['prompt:recorded-wakeup'])
  },
)

test(
  'an unresolved reader fault fails after native elicitation is answered',
  { timeout: 5000 },
  async (t) => {
    const { h, obs, reader, error, question, running } =
      await recordedQuestionWithReaderFault(t)
    reader.impl = async () => {
      throw error
    }
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    assert.deepEqual(h.turns(), [])
    await answerRecordedQuestion(h, question.request.id)
    await obs.poll().catch(() => undefined)
    await assertFaultedQuestionTerminated(h, obs, running, 'failed')
    const terminal = h.events.findLast(
      (e) => e.type === 'session.turn' && e.state === 'failed',
    )
    assert.ok(terminal?.type === 'session.turn' && terminal.state === 'failed')
    assert.match(
      terminal.error?.message ?? '',
      /recorded question reader unavailable/,
    )
  },
)

test(
  'declining native elicitation cancels despite an unresolved reader fault',
  { timeout: 5000 },
  async (t) => {
    const { h, obs, reader, error, question, running } =
      await recordedQuestionWithReaderFault(t)
    reader.impl = async () => {
      throw error
    }
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    assert.deepEqual(h.turns(), [])
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: question.request.id,
      response: { behavior: 'deny' },
    })
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    await assertFaultedQuestionTerminated(h, obs, running, 'canceled')
  },
)

test(
  'interrupt cancels native elicitation despite an unresolved reader fault',
  { timeout: 5000 },
  async (t) => {
    const { h, obs, reader, error, running } =
      await recordedQuestionWithReaderFault(t)
    reader.impl = async () => {
      throw error
    }
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    assert.deepEqual(h.turns(), [])
    await h.manager.handle({
      type: 'session.interrupt',
      sessionId: 's',
      requestId: 'interrupt',
    })
    await assertFaultedQuestionTerminated(h, obs, running, 'canceled')
  },
)

test(
  'close cancels native elicitation despite an unresolved reader fault',
  { timeout: 5000 },
  async (t) => {
    const { h, obs, reader, error, running } =
      await recordedQuestionWithReaderFault(t)
    reader.impl = async () => {
      throw error
    }
    await assert.rejects(obs.poll(), /recorded question reader unavailable/)
    assert.deepEqual(h.turns(), [])
    await h.manager.close()
    await assertFaultedQuestionTerminated(h, obs, running, 'canceled')
  },
)

test(
  'declining the recorded wakeup question cancels without relaunching or sending another prompt',
  { timeout: 5000 },
  async (t) => {
    const { h, running, question } = await recordedQuestion(t)
    const transport = h.session.transport
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: question.request.id,
      response: { behavior: 'deny' },
    })
    await running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(h.session.transport, transport)
    assert.deepEqual(await promptRequests(h.acpLog), ['prompt:recorded-wakeup'])
    assert.equal(
      h.toolCalls().findLast((item) => item.callId === 'call_4')?.status,
      'canceled',
    )
  },
)

test(
  'interrupting the recorded wakeup question cancels without relaunching',
  { timeout: 5000 },
  async (t) => {
    const { h, running } = await recordedQuestion(t)
    const transport = h.session.transport
    await h.manager.handle({
      type: 'session.interrupt',
      sessionId: 's',
      requestId: 'interrupt',
    })
    await running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.equal(h.session.transport, transport)
    assert.deepEqual(await promptRequests(h.acpLog), ['prompt:recorded-wakeup'])
  },
)

test(
  'a repeated native wakeup question gets its own card and close unblocks it',
  { timeout: 5000 },
  async (t) => {
    const { h, running, question } = await recordedQuestion(
      t,
      undefined,
      'recorded-wakeup-repeat',
    )
    await answerRecordedQuestion(h, question.request.id)
    await h.wait(() => h.permissions().length === 2)
    const second = h.permissions()[1]
    assert.notEqual(second.request.id, question.request.id)
    await h.manager.close()
    await running
    assert.deepEqual(h.turns(), ['canceled'])
    assert.deepEqual(await promptRequests(h.acpLog), [
      'prompt:recorded-wakeup-repeat',
    ])
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
  'a foreground prompt that ignores cancel relaunches and leaves the next turn usable',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t, { cancelConvergenceTimeoutMs: 50 })
    const held = await h.holdLive('hold:never')
    const previousTransport = h.session.transport
    await h.manager.handle({
      type: 'session.interrupt',
      sessionId: 's',
      requestId: 'interrupt',
    })
    await held.running
    assert.notEqual(h.session.transport, previousTransport)
    await h.prompt('cancel-only')
    assert.deepEqual(h.turns(), ['canceled', 'canceled'])
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
  'Orchestrator planexecute intra-turn handoffs keep exact step occupancy without false wokeUp double-billing, while woken handback and independent Turn 2 usage settle cleanly',
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
    const held = await h.hold()
    h.session.state.activeTurn!.toolCallIds.add('acp-ask')
    h.session.state.activeTurn!.toolCallIds.add('acp-exit')
    h.session.state.activeTurn!.toolCallIds.add('acp-cmd')
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
    // The next RPC reports only its own consumption.
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
        inputTokens: 1_200,
        cachedReadTokens: 49_500,
        outputTokens: 300,
        totalTokens: 51_000,
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
