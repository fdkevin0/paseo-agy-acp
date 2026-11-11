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
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  ModelUsageObserver,
  BrainTurnObservation,
} from '../server/brain-observer.ts'
import { settleBrainTurn } from '../server/brain.ts'
import { BrainTranscriptProjector } from '../server/brain-projector.ts'

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
const user = (step_index: number, content = 'work'): Row => ({
  step_index,
  type: 'USER_INPUT',
  content,
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
})

async function harness(t: TestContext) {
  const home = await mkdtemp(join(tmpdir(), 'agy-pr8-repro-'))
  const events: ProviderEvent[] = []
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
  return {
    home,
    path,
    events,
    manager,
    session,
    write: (rows: Row[]) => writeFile(path, encode(rows)),
    append: (rows: Row[]) => appendFile(path, encode(rows)),
  }
}

test('wakeup rewrite rebuilds the foreground phase and bills each woken step once across turns', async (t) => {
  const h = await harness(t)
  const rpcDone = deferred<Result>()
  const started = deferred<void>()
  t.mock.method(h.session.transport.rpc, 'prompt', () => {
    started.resolve()
    return rpcDone.promise
  })

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
  const obs = h.session.state.observation!
  const rewriteRows: Record<string, unknown>[][] = []
  const read = obs.reader.read.bind(obs.reader)
  t.mock.method(
    obs.reader,
    'read',
    async (...args: Parameters<typeof read>) => {
      const r = await read(...args)
      if (r.reset) rewriteRows.push(r.rows)
      return r
    },
  )
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

  const attached = deferred<void>()
  const origAttach = obs.attach.bind(obs)
  t.mock.method(
    obs,
    'attach',
    async (consumer: Parameters<typeof origAttach>[0]) => {
      await origAttach(consumer)
      attached.resolve()
    },
  )
  rpcDone.resolve({
    stopReason: 'end_turn',
    usage: { inputTokens: 15000, outputTokens: 1, totalTokens: 15001 },
  })
  await attached.promise

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
  await turnPromise

  const finalUsage = [...h.events]
    .reverse()
    .find(
      (e): e is Extract<ProviderEvent, { type: 'session.usage' }> =>
        e.type === 'session.usage' && e.usage.inputTokens !== undefined,
    )!

  assert.equal(
    h.session.state.wakeup.carriedUsage?.inputTokens,
    130000,
    'regression: foreground step 2 (15000) must stay out of wakeup carriedUsage',
  )
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
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'end_turn' as const,
    usage: { inputTokens: 150000, outputTokens: 3, totalTokens: 150003 },
  }))
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
    'the next bill deducts only the legitimate 130k wakeup carry',
  )
})

test('continuation rewrite retains its ACP identity boundary and preceding foreground state', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'agy-pr8-cont-id-'))
  const nativeId = '11111111-1111-4111-8111-111111111111'
  const logsDir = join(root, nativeId, '.system_generated/logs')
  await mkdir(logsDir, { recursive: true })
  const path = join(logsDir, 'transcript_full.jsonl')
  t.after(() => rm(root, { recursive: true, force: true }))

  const items: ProviderTimelineItem[] = []
  const acpToolCallIds = ['acp-ask-1']
  let continuationActive = false
  const obs = new BrainTurnObservation(
    root,
    nativeId,
    's',
    0,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(structuredClone(item)),
    },
    60_000,
  )

  await writeFile(
    path,
    encode([
      user(1, 'deploy'),
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            name: 'ask_question',
            args: {
              questions: [{ question: 'Env?', options: ['staging', 'prod'] }],
            },
          },
        ],
      },
    ]),
  )
  await obs.poll()

  const settlePromise = settleBrainTurn(
    root,
    nativeId,
    's',
    (item) => items.push(structuredClone(item)),
    {
      observation: obs,
      minStepIndex: 0,
      acpToolCallIds,
      isRpcActive: () => continuationActive,
      onQuestion: async (q) => {
        q.beginContinuation?.('staging')
        continuationActive = true
        acpToolCallIds.push('acp-cont-bash-1')
        await writeFile(
          path,
          encode([
            user(1, 'deploy'),
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              truncated_fields: ['tool_calls'],
              tool_calls: [
                {
                  name: 'ask_question',
                  args: {
                    questions: [
                      { question: 'Env?', options: ['staging', 'prod'] },
                    ],
                  },
                },
              ],
            },
            {
              step_index: 3,
              type: 'GENERIC',
              status: 'DONE',
              content: 'staging',
            },
            user(4, 'staging'),
            checkpoint(5),
            {
              step_index: 6,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                { name: 'run_command', args: { CommandLine: 'echo deploy' } },
              ],
            },
            {
              step_index: 7,
              type: 'GENERIC',
              status: 'DONE',
              content: 'deployed',
            },
            model(8, 500, 'Done deploying'),
          ]),
        )
        await obs.poll()
        continuationActive = false
        return { status: 'answered', answerText: 'staging' }
      },
    },
  )

  await settlePromise
  const bashItems = items.filter(
    (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
      i.type === 'tool_call' && i.name !== 'ask_question',
  )
  assert.equal(
    bashItems[0]?.callId,
    'acp-cont-bash-1',
    'regression: continuation tool must retain its continuation ID',
  )
})

test('split continuation rewrite preserves live output and wakeup accounting', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'agy-pr8-cont-split-'))
  const nativeId = '22222222-2222-4222-8222-222222222222'
  const logsDir = join(root, nativeId, '.system_generated/logs')
  await mkdir(logsDir, { recursive: true })
  const path = join(logsDir, 'transcript_full.jsonl')
  t.after(() => rm(root, { recursive: true, force: true }))

  const items: ProviderTimelineItem[] = []
  let continuationActive = false
  const obs = new BrainTurnObservation(
    root,
    nativeId,
    's',
    0,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(structuredClone(item)),
    },
    60_000,
  )

  const initialRows: Row[] = [
    user(1, 'deploy'),
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'ask-1',
          name: 'ask_question',
          args: { questions: [{ question: 'Env?', options: ['staging'] }] },
        },
      ],
    },
  ]
  await writeFile(path, encode(initialRows))
  await obs.poll()

  const settled = await settleBrainTurn(
    root,
    nativeId,
    's',
    (item) => items.push(structuredClone(item)),
    {
      observation: obs,
      minStepIndex: 0,
      isRpcActive: () => continuationActive,
      onQuestion: async (q) => {
        q.beginContinuation?.('staging')
        continuationActive = true
        await writeFile(
          path,
          encode([
            user(1, 'deploy'),
            {
              ...initialRows[1],
              truncated_fields: ['tool_calls'],
            },
            {
              step_index: 3,
              type: 'GENERIC',
              status: 'DONE',
              content: 'staging',
            },
          ]),
        )
        await obs.poll()
        await appendFile(
          path,
          encode([
            user(4, 'staging'),
            model(5, 8000, 'Continuation reply that MUST be shown'),
          ]),
        )
        await obs.poll()
        continuationActive = false
        return { status: 'answered', answerText: 'staging' }
      },
    },
  )

  const assistantTexts = items
    .filter((i) => i.type === 'assistant_message')
    .map((i) => i.text)
  assert.deepEqual(
    assistantTexts,
    ['Continuation reply that MUST be shown'],
    'regression: 100% of continuation assistant output must remain visible',
  )
  assert.equal(settled.wokeUp, true, 'regression: wokeUp must remain true')
  assert.equal(
    settled.wakeupUsage?.inputTokens,
    8000,
    'regression: continuation wakeupUsage must remain accounted',
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

  const obs = new BrainTurnObservation(
    root,
    nativeId,
    's',
    3,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: () => {},
    },
    15,
  )
  obs.start()

  await writeFile(path, encode([user(1, 'turn 1')]))
  await obs.poll()

  const attached = deferred<void>(),
    attach = obs.attach.bind(obs)
  t.mock.method(obs, 'attach', async (...args: Parameters<typeof attach>) => {
    await attach(...args)
    attached.resolve()
  })
  const settlePromise = settleBrainTurn(root, nativeId, 's', () => {}, {
    observation: obs,
    minStepIndex: 3,
    stallTimeoutMs: 60,
  })

  await attached.promise
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
})

test('appended foreground checkpoint is not pruned by an older staged snapshot on cancel', async (t) => {
  const h = await harness(t)
  const held = deferred<Result>()
  const started = deferred<void>()
  t.mock.method(h.session.transport.rpc, 'prompt', () => {
    started.resolve()
    return held.promise
  })

  const turnPromise = h.manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-dup',
      delivery: 'auto',
      input: {
        type: 'message',
        content: [{ type: 'text', text: 'compact then cancel' }],
      },
    },
  })
  await started.promise
  const obs = h.session.state.observation!

  await h.write([user(1), model(2, 100)])
  await obs.poll()

  await h.append([checkpoint(3)])
  await obs.poll()

  h.session.state.turnAbort!.abort(
    Object.assign(new Error('canceled'), { name: 'AbortError' }),
  )
  held.resolve({ stopReason: 'cancelled' })
  await turnPromise

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
  p.resolveToolIdentities(true)
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
  const items = p.resolveToolIdentities(false)
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
  for (const item of items)
    assert.equal(p.nativeToolIdentitySource(item), undefined)
})

test('shifted GENERIC compatibility fallback consumes each anonymous slot exactly once', () => {
  const p = new BrainTranscriptProjector('s')
  p.resolveToolIdentities(true)
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
  const completed = p
    .resolveToolIdentities(false)
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

test(
  'a background task completed inside a held continuation settles after identity handoff',
  { timeout: 2000 },
  async (t) => {
    const root = await mkdtemp(join(tmpdir(), 'agy-cont-background-'))
    const nativeId = '33333333-3333-4333-8333-333333333333'
    const dir = join(root, nativeId, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    const path = join(dir, 'transcript_full.jsonl')
    const controller = new AbortController(),
      items: ProviderTimelineItem[] = []
    let active = false
    const obs = new BrainTurnObservation(
      root,
      nativeId,
      's',
      0,
      {
        isCurrent: () => true,
        onUsage: () => {},
        onItem: (item) => items.push(item),
      },
      60000,
    )
    t.after(async () => {
      controller.abort()
      await obs.dispose()
      await rm(root, { recursive: true, force: true })
    })
    await writeFile(
      path,
      encode([
        user(1),
        {
          step_index: 2,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'ask',
              name: 'ask_question',
              args: { questions: [{ question: 'Run?', options: ['Yes'] }] },
            },
          ],
        },
      ]),
    )
    await obs.poll()
    const native = (
      await readFile(
        new URL('./fixtures/woken-question-transcript.jsonl', import.meta.url),
        'utf8',
      )
    )
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
      .slice(1, 5)
    const result = await settleBrainTurn(
      root,
      nativeId,
      's',
      (item) => items.push(item),
      {
        observation: obs,
        minStepIndex: 0,
        signal: controller.signal,
        isRpcActive: () => active,
        onQuestion: async (q) => {
          q.beginContinuation?.('Yes')
          active = true
          await appendFile(
            path,
            encode([
              {
                step_index: 3,
                type: 'GENERIC',
                status: 'DONE',
                content: 'Yes',
              },
              user(4, 'Yes'),
              ...native.map((row) => ({
                ...row,
                step_index: row.step_index + 4,
              })),
              model(9, 200, 'Finished'),
            ]),
          )
          await obs.poll()
          active = false
          return { status: 'answered', answerText: 'Yes' }
        },
      },
    )
    assert.equal(result.status, 'completed')
    assert.ok(
      items.some(
        (item) =>
          item.type === 'tool_call' &&
          item.name === 'run_command' &&
          item.status === 'completed',
      ),
    )
  },
)

test('confirmed usage deletion invalidates the source cache without fabricating a new user context or bill', async (t) => {
  const h = await harness(t),
    entered = deferred<void>(),
    done = deferred<Result>()
  h.session.publishUsage({ inputTokens: 1000, outputTokens: 100 })
  t.mock.method(h.session.transport.rpc, 'prompt', () => {
    entered.resolve()
    return done.promise
  })
  const running = h.manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'deletion',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'work' }] },
    },
  })
  await entered.promise
  const obs = h.session.state.observation!
  await h.write([user(1), model(2, 70)])
  await obs.poll()
  await h.write([user(1)])
  await obs.poll()
  assert.equal(obs.usage.latest, undefined)
  h.session.state.turnAbort!.abort()
  done.resolve({ stopReason: 'cancelled' })
  await running
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

test('final drain retains confirmed checkpoint deletion observed while publication is stopped', async (t) => {
  const items: ProviderTimelineItem[] = []
  const obs = new BrainTurnObservation('', '', 's', -1, {
    isCurrent: () => true,
    onUsage: () => {},
    onItem: (item) => items.push(item),
  })
  let read = {
    rows: [checkpoint(2)],
    reset: true,
    snapshotStepIndices: new Set([2]) as ReadonlySet<number> | undefined,
  }
  t.mock.method(obs.reader, 'read', async () => read)
  await obs.poll()
  obs.stop()
  read = { rows: [], reset: false, snapshotStepIndices: new Set([1]) }
  await obs.poll()
  read = { rows: [checkpoint(2)], reset: false, snapshotStepIndices: undefined }
  await obs.drain()
  assert.equal(
    items.length,
    2,
    'proven deletion releases identity even when the proof pass could not publish',
  )
  await obs.dispose()
})
