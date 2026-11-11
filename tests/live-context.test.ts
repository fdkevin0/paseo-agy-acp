import assert from 'node:assert/strict'
import { test, type TestContext } from 'node:test'
import { mkdtemp, mkdir, writeFile, rm, appendFile } from 'node:fs/promises'
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
import { watchBrain } from '../server/brain.ts'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { normalizeAskQuestionInput } from '../server/questions.ts'

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
  usage?: { inputTokens: number; outputTokens: number; totalTokens: number }
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

async function harness(t: TestContext) {
  const home = await mkdtemp(join(tmpdir(), 'agy-live-context-'))
  const events: ProviderEvent[] = []
  const listeners = new Set<() => void>()
  const manager = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message', 'permission'],
    emit: (event) => {
      events.push(structuredClone(event))
      for (const listener of listeners) listener()
    },
  })
  const start = manager.start.bind(manager)
  let session!: Awaited<ReturnType<typeof manager.start>>
  t.mock.method(
    manager,
    'start',
    async (...args: Parameters<typeof start>) =>
      (session = await start(...args)),
  )
  await manager.open({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: { cwd: home, env: {}, mcpServers: {}, settings: {}, persist: true },
  })
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
    t.mock.method(session.transport.rpc, 'prompt', async () => {
      entered.resolve()
      return done.promise
    })
    const running = prompt()
    await entered.promise
    return { running, done, observation: session.state.observation! }
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
    latest,
    turns,
    compactions,
    wait,
    read,
    hold,
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
    for (const toolCallId of h.session.state.turnToolCallIds)
      h.session.timeline.update({
        sessionUpdate: 'tool_call',
        toolCallId,
        title: 'tool',
        status: 'in_progress',
      })
    held.done.resolve({ stopReason: 'end_turn' })
    await held.running
    const tools = h.events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'tool_call'
        ? [event.item]
        : [],
    )
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
    let calls = 0
    t.mock.method(h.session.transport.rpc, 'prompt', async () => {
      if (++calls === 1) {
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
      }
      entered.resolve()
      return continuation.promise
    })
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
    const tools = () =>
      h.events.flatMap((event) =>
        event.type === 'timeline.item' && event.item.type === 'tool_call'
          ? [event.item]
          : [],
      )
    assert.ok(
      tools().some(
        (item) => item.callId === 'known' && item.status === 'completed',
      ),
    )
    assert.equal(
      tools().some((item) => item.callId.startsWith('tool-')),
      false,
    )
    assert.deepEqual(h.turns(), [])
    h.session.state.turnToolCallIds.push('native-late')
    await h.read()
    assert.ok(
      tools().some(
        (item) => item.callId === 'native-late' && item.status === 'completed',
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

test(
  'cancel/throw final pass waits for observable local work and preserves complete prefix before a partial tail',
  { timeout: 5000 },
  async (t) => {
    for (const throws of [false, true]) {
      const h = await harness(t),
        held = await h.hold()
      h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
      const local = deferred<void>()
      h.session.state.interrupting = local.promise
      h.session.state.turnAbort!.abort()
      await h.write(
        [user(1), model(2, 70207), checkpoint(3)],
        '{"step_index":4',
      )
      if (throws) held.done.reject(new Error('native failure'))
      else held.done.resolve({ stopReason: 'cancelled' })
      assert.deepEqual(h.turns(), [])
      local.resolve()
      await held.running
      h.session.state.interrupting = null
      assert.equal(h.latest().contextWindowUsedTokens, 70208)
      assert.equal(h.latest().inputTokens, 1000)
      assert.equal(h.compactions().length, 1)
      assert.equal(h.session.state.lastKnownStepIndex, 3)
      assert.deepEqual(h.turns(), ['canceled'])
      assert.equal(held.observation.phase, 'disposed')
    }
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
    const nativePrompt = h.session.transport.rpc.prompt.bind(
      h.session.transport.rpc,
    )
    const held = await h.hold()
    // The mock native runtime emits a real ACP usage_update into the held logical foreground turn.
    await nativePrompt({
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
    const attached = deferred<void>(),
      attach = held.observation.attach.bind(held.observation)
    t.mock.method(
      held.observation,
      'attach',
      async (consumer: Parameters<typeof attach>[0]) => {
        await attach(consumer)
        attached.resolve()
      },
    )
    held.done.resolve({ stopReason: 'end_turn' })
    await attached.promise
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

test(
  'late disk writes beyond cancel observation remain unclaimed, cannot bill a new turn and can restore later',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold()
    await h.write([user(1), model(2, 10)])
    await h.read()
    h.session.state.turnAbort!.abort()
    held.done.resolve({ stopReason: 'cancelled' })
    await held.running
    const count = h.events.length
    await h.append([model(3, 100), checkpoint(4)])
    await held.observation.poll()
    assert.equal(
      h.events.length,
      count,
      'no post-terminal polling or invented flush guarantee',
    )
    t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
      stopReason: 'cancelled' as const,
    }))
    const usageCount = h.events.filter(
      (event) => event.type === 'session.usage',
    ).length
    await h.prompt()
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
    await h.manager.open({
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
    let calls = 0
    t.mock.method(h.session.transport.rpc, 'prompt', async () => {
      if (++calls === 1) {
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
      }
      entered.resolve()
      return continued.promise
    })
    const running = h.prompt()
    const questions = () =>
      h.events.flatMap((event) =>
        event.type === 'session.permission' ? [event] : [],
      )
    await h.wait(() => questions().length === 1)
    await h.manager.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId: questions()[0].request.id,
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
      questions().length,
      1,
      'no recursive question action while continuation is in flight',
    )
    continued.resolve({ stopReason: 'end_turn' })
    await h.wait(() => questions().length === 2)
    assert.notEqual(questions()[0].request.id, questions()[1].request.id)
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
        events.flatMap((event) =>
          event.type === 'session.turn' ? [event.state] : [],
        ),
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
    t.mock.method(h.session.transport.rpc, 'prompt', async () => {
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
    const h = await harness(t),
      held = await h.hold()
    await h.read()
    await h.write([user(1), model(2, 100), checkpoint(3)])
    const entered = deferred<void>(),
      release = deferred<void>(),
      reader = held.observation.reader.read.bind(held.observation.reader)
    t.mock.method(
      held.observation.reader,
      'read',
      async (...args: Parameters<typeof reader>) => {
        const result = await reader(...args)
        entered.resolve()
        await release.promise
        return result
      },
    )
    const pass = held.observation.poll()
    await entered.promise
    const oldTransport = h.session.transport,
      nativeId = h.session.state.nativeId
    await h.session.relaunch(h.session.state.skipPermissions, true)
    assert.equal(h.session.state.nativeId, nativeId)
    assert.notEqual(h.session.transport, oldTransport)
    const count = h.events.filter(
      (event) =>
        event.type === 'session.usage' ||
        (event.type === 'timeline.item' && event.item.type === 'compaction'),
    ).length
    release.resolve()
    await pass
    assert.equal(
      h.events.filter(
        (event) =>
          event.type === 'session.usage' ||
          (event.type === 'timeline.item' && event.item.type === 'compaction'),
      ).length,
      count,
    )
    held.done.reject(new Error('old transport closed'))
    await held.running
    assert.deepEqual(h.turns(), ['failed'])
    assert.equal(held.observation.phase, 'disposed')
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

for (const cached of [false, true])
  for (const throws of [false, true])
    test(
      `final drain prefers complete no-LF EOF snapshot (prior observed usage=${cached}, throw=${throws})`,
      { timeout: 5000 },
      async (t) => {
        const h = await harness(t),
          held = await h.hold()
        h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
        await h.write(
          cached
            ? [user(1), model(2, 10), checkpoint(3)]
            : [user(1), checkpoint(3)],
        )
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
        assert.equal(
          streamingIndex,
          3,
          'no-LF EOF is not a delivered streaming row',
        )
        h.session.state.turnAbort!.abort()
        if (throws) held.done.reject(new Error('native cancellation'))
        else held.done.resolve({ stopReason: 'cancelled' })
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
    const nativePrompt = h.session.transport.rpc.prompt.bind(
      h.session.transport.rpc,
    )
    const held = await h.hold()
    await nativePrompt({
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

for (const tail of ['older', 'missing', 'higher-nonusage'] as const)
  test(
    `final ${tail} tail preserves observed context while reconciling only complete boundaries`,
    { timeout: 5000 },
    async (t) => {
      const h = await harness(t),
        held = await h.hold()
      await h.write([user(1), model(4, 80)])
      await h.read()
      if (tail === 'missing') await rm(h.path)
      else if (tail === 'older')
        await writeFile(h.path, JSON.stringify(model(2, 10)))
      else
        await appendFile(
          h.path,
          JSON.stringify({
            step_index: 6,
            type: 'GENERIC',
            status: 'CANCELED',
          }),
        )
      held.done.resolve({ stopReason: 'cancelled' })
      await held.running
      assert.equal(h.latest().contextWindowUsedTokens, 81)
      assert.equal(
        h.session.state.lastKnownStepIndex,
        tail === 'higher-nonusage' ? 6 : 4,
      )
      assert.deepEqual(h.turns(), ['canceled'])
    },
  )

test(
  'already closing turn skips final snapshot and pending local interruption without waiting for the RPC',
  { timeout: 5000 },
  async (t) => {
    const h = await harness(t),
      held = await h.hold(),
      local = deferred<void>()
    h.session.state.interrupting = local.promise
    await h.manager.close()
    await held.running
    assert.deepEqual(h.turns(), ['canceled'])
    const count = h.events.length
    local.resolve()
    held.done.resolve({ stopReason: 'cancelled' })
    await held.observation.poll()
    assert.equal(h.events.length, count)
  },
)
