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
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { AgyUsageSource } from '../server/usage.ts'
import {
  ModelUsageObserver,
  BrainTurnObservation,
} from '../server/brain-observer.ts'
import { TranscriptReader } from '../server/brain-io.ts'
import { settleBrainTurn } from '../server/brain.ts'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import {
  checkpointRow,
  encode,
  modelRow,
  userRow,
  type Row,
} from './brain-fixtures.ts'
import {
  createEventCollector,
  deferred,
  HarnessSessionManager,
  isolatedEnv,
  ScriptedTranscriptReader,
  sequenceDouble,
  TrackingObservation,
  wrapConnectAcp,
  type PromptHandler,
  type RpcPrompt,
} from './helpers.ts'

type Result = Awaited<ReturnType<RpcPrompt>>

const user = userRow
const model = modelRow
const checkpoint = (step_index: number, content = 'summary'): Row =>
  checkpointRow(step_index, { content })

class RecordingTranscriptReader extends TranscriptReader {
  private readonly rewriteRows: Record<string, unknown>[][]
  constructor(rewriteRows: Record<string, unknown>[][]) {
    super()
    this.rewriteRows = rewriteRows
  }
  override async read(root: string, nativeId: string) {
    const r = await super.read(root, nativeId)
    if (r.reset) this.rewriteRows.push(r.rows)
    return r
  }
}

async function harness(
  t: TestContext,
  options?: {
    observationPollIntervalMs?: number
    createTranscriptReader?: () => TranscriptReader
    createObservation?: (
      ...args: ConstructorParameters<typeof BrainTurnObservation>
    ) => BrainTurnObservation
  },
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-pr8-repro-'))
  const collector = createEventCollector()
  const events = collector.events
  let promptHandler: PromptHandler | undefined
  const env = isolatedEnv(home)
  const manager = new HarnessSessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env,
    },
    usage: new AgyUsageSource({ env }),
    capabilities: ['prompt.message', 'permission'],
    emit: collector.emit,
    observationPollIntervalMs: options?.observationPollIntervalMs,
    connectAcp: wrapConnectAcp(() => promptHandler),
    createTranscriptReader: options?.createTranscriptReader,
    createObservation: options?.createObservation,
  })
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: { cwd: home, env: {}, mcpServers: {}, settings: {}, persist: true },
  })
  const session = manager.session
  const logsDir = join(
    session.brainRoot,
    session.state.nativeId,
    '.system_generated/logs',
  )
  await mkdir(logsDir, { recursive: true })
  const path = join(logsDir, 'transcript_full.jsonl')
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  })
  const holdLive = async (clientMessageId: string) => {
    const target =
      events.filter((event) => event.type === 'session.commands').length + 1
    const running = manager.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'hold:cancel' }],
        },
      },
    })
    await collector.wait(
      () =>
        events.filter((event) => event.type === 'session.commands').length >=
        target,
    )
    return { running, observation: session.state.activeTurn!.observation! }
  }
  return {
    home,
    path,
    events,
    manager,
    session,
    setPromptHandler: (handler: PromptHandler | undefined) => {
      promptHandler = handler
    },
    write: (rows: Row[]) => writeFile(path, encode(rows)),
    append: (rows: Row[]) => appendFile(path, encode(rows)),
    holdLive,
  }
}

test('wakeup rewrite rebuilds the foreground phase and bills each woken step once across turns', async (t) => {
  const rewriteRows: Record<string, unknown>[][] = []
  let trackedObs!: TrackingObservation
  const h = await harness(t, {
    observationPollIntervalMs: 60_000,
    createTranscriptReader: () => new RecordingTranscriptReader(rewriteRows),
    createObservation: (...args) =>
      (trackedObs = new TrackingObservation(...args)),
  })
  const rpcDone = deferred<Result>()
  const started = deferred<void>()
  h.setPromptHandler(
    sequenceDouble([
      () => {
        started.resolve()
        return rpcDone.promise
      },
      async () => {
        await h.append([user(8, 'next'), model(9, 20000, 'next reply')])
        await trackedObs.poll()
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 20000, outputTokens: 3, totalTokens: 20003 },
        }
      },
    ]),
  )

  const turnPromise = h.manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'go' }] },
    },
  })
  await started.promise
  const obs = trackedObs
  await obs.poll()

  const fgRows: Row[] = [
    user(1),
    model(2, 15000, 'foreground step'),
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ id: 'bg', name: 'run_command', args: {} }],
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'RUNNING',
      content: 'Tool is running as a background task with task id: tracked',
    },
  ]
  await h.write(fgRows)
  await obs.poll()

  rpcDone.resolve({
    stopReason: 'end_turn',
    usage: { inputTokens: 15000, outputTokens: 1, totalTokens: 15001 },
  })
  await obs.attached.promise

  const wakeRows: Row[] = [
    {
      step_index: 5,
      type: 'SYSTEM_MESSAGE',
      content:
        '<SYSTEM_MESSAGE>\n[Message] sender=tracked priority=MESSAGE_PRIORITY_HIGH content=Task id "tracked" finished with result:\nok\n</SYSTEM_MESSAGE>',
    },
    model(6, 90000, 'wakeup first step'),
    {
      step_index: 7,
      type: 'PLANNER_RESPONSE',
      status: 'RUNNING',
      thinking: 'thinking...',
    },
  ]
  await h.append(wakeRows)
  await obs.poll()

  const rewrittenWakeRows: Row[] = [
    wakeRows[0],
    wakeRows[1],
    model(7, 40000, 'wakeup final step'),
  ]
  await h.write([...fgRows, ...rewrittenWakeRows])
  await obs.poll()
  await turnPromise

  const finalUsage = [...h.events]
    .reverse()
    .find(
      (e): e is Extract<ProviderEvent, { type: 'session.usage' }> =>
        e.type === 'session.usage' && e.usage.inputTokens !== undefined,
    )!

  assert.equal(
    finalUsage.usage.inputTokens,
    145000,
    'regression: foreground step 2 must not be double-billed in final session.usage',
  )
  assert.ok(
    rewriteRows.some((rows) =>
      rows.some((row) => row.content === 'wakeup final step'),
    ),
    'real changed-byte reset branch reached',
  )
  await h.manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm2',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'next' }] },
    },
  })
  const next = h.events.findLast((e) => e.type === 'session.usage')
  assert.ok(next?.type === 'session.usage')
  assert.equal(
    next.usage.inputTokens,
    20000,
    'the next bill is independent of the 130k wakeup',
  )
})

test('historical message deltas delivered after attach do not stall a completed current turn', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'agy-pr8-hist-msg-'))
  const nativeId = '44444444-4444-4444-8444-444444444444'
  const logsDir = join(root, nativeId, '.system_generated/logs')
  const messagesDir = join(root, nativeId, '.system_generated/messages')
  await mkdir(logsDir, { recursive: true })
  await mkdir(messagesDir, { recursive: true })
  const path = join(logsDir, 'transcript_full.jsonl')
  t.after(() => rm(root, { recursive: true, force: true }))

  await writeFile(
    join(messagesDir, 'msg-hist-1.json'),
    JSON.stringify({
      id: 'msg-hist-1',
      sender: 'sub-1',
      priority: 'MESSAGE_PRIORITY_HIGH',
      content: 'done',
    }),
  )
  await writeFile(
    join(messagesDir, 'read.json'),
    JSON.stringify({ 'msg-hist-1': true }),
  )

  const obs = new TrackingObservation(
    root,
    nativeId,
    's',
    3,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: () => {},
    },
    60_000,
  )

  await writeFile(path, encode([user(1, 'turn 1')]))
  await obs.poll()

  const settlePromise = settleBrainTurn(root, nativeId, 's', () => {}, {
    observation: obs,
    minStepIndex: 3,
    transcriptGraceMs: 2000,
    stallTimeoutMs: 1000,
  })

  await obs.attached.promise
  await appendFile(
    path,
    encode([
      {
        step_index: 2,
        type: 'SYSTEM_MESSAGE',
        content:
          '<SYSTEM_MESSAGE>\n[Message] sender=sub-1 priority=MESSAGE_PRIORITY_HIGH content=done\n</SYSTEM_MESSAGE>',
      },
      model(3, 100, 'turn 1 done'),
      user(4, 'turn 2'),
      model(5, 200, 'turn 2 complete'),
    ]),
  )
  await obs.poll()

  const res = await settlePromise
  await obs.dispose()
  assert.equal(obs.historicalMessages, 1)
  assert.equal(
    res.status,
    'completed',
    'regression: completed Turn 2 must complete',
  )
  assert.equal(res.stalled, undefined)
  assert.equal(res.lastStepUsage?.inputTokens, 200)
})

test('appended foreground checkpoint is not pruned by an older staged snapshot on cancel', async (t) => {
  const h = await harness(t)
  const held = await h.holdLive('m-dup')
  const obs = held.observation

  await h.write([user(1), model(2, 100)])
  await obs.poll()

  await h.append([checkpoint(3)])
  await obs.poll()

  await h.manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt',
    sessionId: 's',
  })
  await held.running

  const compactions = h.events.filter(
    (e) => e.type === 'timeline.item' && e.item.type === 'compaction',
  )
  assert.equal(
    compactions.length,
    1,
    'regression: one compaction item emitted on foreground cancel drain',
  )
})

test('usage cache guards observed steps independently from publication and invalidates confirmed deletion', async () => {
  const obs = new ModelUsageObserver(0, () => false)
  obs.observe({
    rows: [model(10, 90000)],
    reset: true,
    snapshotStepIndices: new Set([10]),
  })
  assert.equal(obs.latest?.inputTokens, 90000)
  obs.observe({
    rows: [model(2, 5000)],
    reset: true,
    snapshotStepIndices: undefined,
  })
  assert.equal(
    obs.latest?.inputTokens,
    90000,
    'regression: unconfirmed lower partial rewrite must not overwrite observed high step',
  )
  assert.equal(obs.latestStepIndex, 10)
  obs.observe({ rows: [], reset: false, snapshotStepIndices: new Set([2]) })
  assert.equal(obs.latest?.inputTokens, 5000)
  assert.equal(obs.latestStepIndex, 2)

  const publications: number[] = []
  const obs2 = new ModelUsageObserver(0, (usage) => {
    publications.push(usage.inputTokens)
    return true
  })
  obs2.observe({
    rows: [user(1), model(2, 50000)],
    reset: true,
    snapshotStepIndices: new Set([1, 2]),
  })
  obs2.observe({
    rows: [user(1)],
    reset: true,
    snapshotStepIndices: new Set([1]),
  })
  assert.equal(
    obs2.latest?.inputTokens,
    undefined,
    'regression: confirmed rewrite back to user(1) must invalidate deleted step 2 usage in obs2.latest',
  )
  assert.equal(obs2.latestStepIndex, -1)
  assert.deepEqual(
    publications,
    [50000],
    'deletion cannot fabricate zero context',
  )
  obs2.observe({ rows: [model(3, 50000)], reset: false })
  assert.deepEqual(
    publications,
    [50000, 50000],
    'a later observation can publish after invalidation',
  )
})

test('native background completion is folded while anonymous identity remains deferred', async () => {
  const rows = (
    await readFile(
      new URL('./fixtures/woken-question-transcript.jsonl', import.meta.url),
      'utf8',
    )
  )
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line))
  const p = new BrainTranscriptProjector('s')
  p.bindLiveToolCallIds([], new Set())
  assert.deepEqual(p.project(rows[1]), [])
  assert.deepEqual(p.project(rows[2]), [])
  assert.equal(
    p.hasBackground(),
    true,
    'a running task is tracked before it has a display identity',
  )
  p.project(rows[3])
  assert.deepEqual(p.project(rows[4]), [])
  assert.equal(p.hasBackground(), false)
  p.bindLiveToolCallIds(['bg'], new Set(['bg']))
  const items = p.resolveToolIdentities()
  assert.deepEqual(
    items.map((item) => item.type === 'tool_call' && item.status),
    ['running', 'completed'],
  )
  assert.equal(
    p.hasBackground(),
    false,
    'identity resolution must not replay task registration',
  )
  assert.equal(p.hasPending(), false)
  assert.deepEqual(
    items.map((item) => p.nativeToolIdentitySource(item)),
    ['acp_input', 'acp_input'],
  )
})

test('shifted GENERIC compatibility fallback consumes each anonymous slot exactly once', () => {
  const p = new BrainTranscriptProjector('s')
  p.bindLiveToolCallIds([], new Set())
  p.project({
    step_index: 10,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      { name: 'read_file', args: { Path: '/a' } },
      { name: 'read_file', args: { Path: '/b' } },
    ],
  })
  p.project({
    step_index: 13,
    type: 'GENERIC',
    status: 'DONE',
    content: 'content-A',
  })
  p.project({
    step_index: 14,
    type: 'GENERIC',
    status: 'DONE',
    content: 'content-B',
  })
  p.bindLiveToolCallIds(['a', 'b'], new Set(['a', 'b']))
  const completed = p
    .resolveToolIdentities()
    .filter((item) => item.type === 'tool_call' && item.status === 'completed')
  assert.deepEqual(
    completed.map(
      (item) =>
        item.type === 'tool_call' &&
        item.detail.type === 'read' &&
        item.detail.content,
    ),
    ['content-A', 'content-B'],
  )
  assert.equal(p.hasPending(), false)
})

test('confirmed usage deletion invalidates the source cache without fabricating a new user context or bill', async (t) => {
  const h = await harness(t)
  h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
  const held = await h.holdLive('deletion')
  const obs = held.observation
  await h.write([user(1), model(2, 70)])
  await obs.poll()
  await h.write([user(1)])
  await obs.poll()
  assert.equal(obs.usage.latest, undefined)
  await h.manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt',
    sessionId: 's',
  })
  await held.running
  const usage = h.events.findLast((e) => e.type === 'session.usage')
  assert.ok(usage?.type === 'session.usage')
  assert.equal(
    usage.usage.contextWindowUsedTokens,
    71,
    'previously observed UI context remains last-known, not zero',
  )
  assert.equal(usage.usage.inputTokens, 1000)
  assert.equal(usage.usage.outputTokens, 100)
})

test('final drain retains confirmed checkpoint deletion observed while publication is stopped', async () => {
  const items: ProviderTimelineItem[] = []
  const reader = new ScriptedTranscriptReader([
    {
      rows: [checkpoint(2)],
      reset: true,
      snapshotStepIndices: new Set([2]),
    },
    { rows: [], reset: false, snapshotStepIndices: new Set([1]) },
    { rows: [checkpoint(2)], reset: false, snapshotStepIndices: undefined },
  ])
  const obs = new BrainTurnObservation(
    '',
    '',
    's',
    -1,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(item),
    },
    200,
    reader,
  )
  await obs.poll()
  obs.stop()
  await obs.poll()
  await obs.drain()
  assert.equal(
    items.length,
    2,
    'proven deletion releases identity even when the proof pass could not publish',
  )
  await obs.dispose()
})
