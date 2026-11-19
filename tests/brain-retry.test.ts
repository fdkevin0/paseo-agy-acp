import assert from 'node:assert/strict'
import { EventEmitter, once } from 'node:events'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test, type TestContext } from 'node:test'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import {
  hasActiveBackgroundWork,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
} from '../server/brain.ts'
import { BrainTurnObservation } from '../server/brain-observer.ts'
import {
  BrainNode,
  BrainTranscriptProjector,
} from '../server/brain-projector.ts'
import {
  StepDedupe,
  isRetryWarning,
  RETRY_IDLE_TIMEOUT_MS,
  type InboxState,
} from '../server/brain-io.ts'
import { Timeline } from '../server/mapping.ts'
import { flushObs, writeBrainTranscript, writeFileAtomic } from './helpers.ts'

const id = '11111111-1111-4111-8111-111111111111'
const inbox: InboxState = {
  messages: [],
  readIds: new Set(),
  unconsumedHighPriority: [],
  consumedNonUserCount: 0,
  stepCallIds: new Map(),
}
const warning = (step_index: number, attempt = 1, created_at?: string) => ({
  step_index,
  source: 'SYSTEM',
  type: 'ERROR_MESSAGE',
  status: 'DONE',
  error: `API error (attempt ${attempt}): request failed: synthetic timeout`,
  ...(created_at ? { created_at } : {}),
})
const quiet = (step_index: number) => ({
  step_index,
  source: 'MODEL',
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  thinking: 'No further reply required',
})

function assertRetryWarningAccepted(row: Record<string, unknown>) {
  assert.equal(isRetryWarning(row), true)
  const projector = new BrainTranscriptProjector('p')
  projector.project({
    step_index: 0,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id: 'pending', name: 'run_command', args: {} }],
  })
  assert.deepEqual(projector.project(row), [])
  assert.equal(projector.hasPending(), true)
}

function assertRetryWarningRejected(row: Record<string, unknown>) {
  assert.equal(isRetryWarning(row), false)
  assert.equal(new BrainTranscriptProjector('p').project(row)[0].type, 'error')
}

function trackAll(timeline: Timeline, items: readonly ProviderTimelineItem[]) {
  for (const item of items) timeline.track(item)
}

function assertProgressReleasesRetry(
  t: TestContext,
  progressRow: Record<string, unknown>,
) {
  const node = new BrainNode(id)
  node.ingest({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id: 'pending', name: 'run_command', args: {} }],
  })
  node.ingest(warning(2))
  node.ingest(progressRow)
  assert.equal(node.retryPending(), false)
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS + 1)
  assert.equal(
    node.failure(),
    undefined,
    'a recovered turn must not retain the retry deadline',
  )
}

test('quiet DONE completes normally and after a warning, while incomplete steps and trailing errors do not', () => {
  const node = new BrainNode(id)
  node.ingest({ step_index: 1, type: 'USER_INPUT' })
  node.ingest(quiet(2))
  assert.equal(node.foregroundIdleReached, true)
  assert.equal(node.isSettled(inbox), true)
  node.ingest(warning(3))
  assert.equal(node.retryPending(), true)
  assert.equal(node.isSettled(inbox), false)
  node.ingest({ ...quiet(4), status: 'RUNNING' })
  assert.equal(node.retryPending(), true)
  assert.equal(node.isSettled(inbox), false)
  node.ingest(quiet(5))
  assert.equal(node.retryPending(), false)
  assert.equal(node.isSettled(inbox), true)
  node.ingest(warning(6, 2))
  assert.equal(
    node.isSettled(inbox),
    false,
    'a later warning in the same read wins over a quiet DONE',
  )
  node.ingest({
    step_index: 7,
    type: 'ERROR_MESSAGE',
    status: 'ERROR',
    content: 'Permission denied',
  })
  assert.equal(node.failure(), 'Permission denied')
  assert.equal(node.isSettled(inbox), false)
})

test('quiet foreground completion still establishes Reactive Wakeup ownership', () => {
  const node = new BrainNode(id)
  node.ingest({ step_index: 1, type: 'USER_INPUT' })
  node.ingest({
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'spawn', name: 'invoke_subagent', args: {} }],
  })
  node.ingest({
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: JSON.stringify({
      conversationId: '22222222-2222-4222-8222-222222222222',
    }),
  })
  node.ingest(quiet(4))
  assert.equal(node.foregroundIdleReached, true)
  node.ingest({
    step_index: 5,
    type: 'SYSTEM_MESSAGE',
    content:
      '<SYSTEM_MESSAGE>\n[Message] sender=22222222-2222-4222-8222-222222222222 priority=MESSAGE_PRIORITY_HIGH content=report\n</SYSTEM_MESSAGE>',
  })
  assert.equal(node.wokeUp, true)
  const items = node.ingest({
    ...quiet(6),
    content: 'The child reported back.',
  })
  assert.ok(items.some((item) => item.type === 'assistant_message'))
  assert.equal(node.isSettled(inbox), true)
})

test('retry classification accepts only observed system warning forms and explicit attempt prefixes', () => {
  assertRetryWarningAccepted(warning(1))
  assertRetryWarningAccepted({
    ...warning(1),
    content:
      'Error: API error (attempt 2): request failed: synthetic unavailable',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'model output error: model output must contain either output text or tool calls, these cannot both be empty, please try again',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'The stream was interrupted. Please continue the task you were working on.',
  })
  assertRetryWarningAccepted({
    step_index: 1,
    source: 'SYSTEM',
    type: 'ERROR_MESSAGE',
    status: 'DONE',
    content: null,
    error:
      'Your previous response contained an improperly formatted function call: Tool name must not be empty.\nRetries remaining: 3',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'Your previous response contained an improperly formatted function call: invalid JSON arguments\nRetries remaining: 2',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'Your previous response was blocked by content safety filters:\r\nRetries remaining: 1',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    source: null,
    error:
      'Your previous response was blocked by content safety filters\nPlease provide a response that complies with content safety guidelines.\nRetries remaining: 2',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'Your previous response was cut off because it exceeded the output token limit\nPlease continue.\nRetries remaining: 2',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'Your previous response encountered a generation error\nPlease try again.\nRetries remaining: 1',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'There was a problem. \nError Message: invalid tool arguments \nRetries remaining: 2',
  })
  assertRetryWarningAccepted({
    ...warning(1),
    error:
      'There was a problem parsing the tool call. \nError Message: unexpected EOF \nGuidance: check JSON syntax \nRetries remaining: 1',
  })

  assertRetryWarningRejected({ ...warning(1), status: 'ERROR' })
  assertRetryWarningRejected({ ...warning(1), source: 'MODEL' })
  assertRetryWarningRejected({
    ...warning(1),
    error: 'API error (attempt 0): invalid',
  })
  assertRetryWarningRejected({ ...warning(1), error: 'API error (attempt 1):' })
  assertRetryWarningRejected({
    ...warning(1),
    error: 'quoted API error (attempt 1): network',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error: 'generic::unavailable: please try again',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error: 'model output error: generic::unavailable: please try again',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error: 'request failed: connection reset',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error:
      'Your previous response contained an improperly formatted function call: invalid JSON\nRetries remaining: 0',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error:
      'Your previous response contained an improperly formatted function call: invalid JSON\nRetries remaining: 2 extra',
  })
  assertRetryWarningRejected({
    ...warning(1),
    error:
      'Quoted: Your previous response was blocked by content safety filters:\nRetries remaining: 2',
  })
})

test('distinct attempts refresh bounded retry silence; duplicate replay cannot prolong it', (t) => {
  const started = 1_000_000
  t.mock.timers.enable({ apis: ['Date'], now: started })
  const node = new BrainNode(id)
  node.ingest(warning(1, 1))
  t.mock.timers.tick(121_000)
  assert.equal(
    node.failure(),
    undefined,
    'normal model generation is not cut off at 120 seconds',
  )
  node.ingest(warning(2, 2))
  t.mock.timers.tick(185_000)
  assert.equal(
    node.failure(),
    undefined,
    'the observed 306-second retry sequence fits the default policy',
  )
  node.ingest(warning(3, 3))
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS - 1)
  assert.equal(node.failure(), undefined)
  node.ingest(warning(3, 3))
  t.mock.timers.tick(1)
  assert.match(
    node.failure()!,
    /attempt 3.*synthetic timeout.*No retry recovery was observed/,
  )
  assert.equal(node.isSettled(inbox), false)
})

test('historical retry timestamps survive replay and recovered quiet DONE clears them', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 2_000_000 })
  const node = new BrainNode(id)
  node.ingest(warning(1, 1, new Date(1_000_000).toISOString()))
  node.ingest(warning(2, 2, new Date(1_500_000).toISOString()))
  assert.equal(node.failure(), undefined)
  t.mock.timers.tick(400_000)
  assert.match(node.failure()!, /attempt 2/)
  node.ingest(quiet(3))
  assert.equal(node.failure(), undefined)
  assert.equal(node.isSettled(inbox), true)
})

test('superseded unconfirmed tool calls end canceled rather than becoming successful', () => {
  const projector = new BrainTranscriptProjector('p')
  const timeline = new Timeline('p')
  const first = projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id: 'old', name: 'run_command', args: {} }],
  })
  trackAll(timeline, first)
  projector.project(warning(2))
  const replacement = projector.project({
    step_index: 3,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id: 'new', name: 'run_command', args: {} }],
  })
  const canceled = replacement.find(
    (item) => item.type === 'tool_call' && item.callId === 'old',
  )
  assert.ok(canceled?.type === 'tool_call')
  assert.equal(canceled.status, 'canceled')
  assert.equal(canceled.id, first[0].id)
  trackAll(timeline, replacement)
  trackAll(
    timeline,
    projector.project({
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      content: 'confirmed result',
    }),
  )
  assert.deepEqual(timeline.terminalize('completed'), [])
})

test('confirmed model or correlated tool progress releases retry state; unrelated results do not', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
  assertProgressReleasesRetry(t, {
    step_index: 3,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'new', name: 'run_command', args: {} }],
  })
  assertProgressReleasesRetry(t, {
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: 'confirmed tool result',
  })
  const unrelated = new BrainNode(id)
  unrelated.ingest(warning(1))
  unrelated.ingest({
    step_index: 2,
    type: 'GENERIC',
    status: 'DONE',
    content: 'orphan result',
  })
  assert.equal(unrelated.retryPending(), true)
  unrelated.ingest({ step_index: 3, type: 'USER_INPUT' })
  assert.equal(
    unrelated.retryPending(),
    false,
    'a new turn cannot inherit an earlier turn retry deadline',
  )
})

test('a mixed planexecute and wakeup notice enters woken and a denied wakeup tool settles', () => {
  const node = new BrainNode(id)
  node.ingest({ step_index: 1, type: 'USER_INPUT' })
  node.ingest(quiet(2))
  assert.equal(node.promptPhase, 'foreground_idle')
  // A SYSTEM_MESSAGE with both a planexecute prefix and a real [Message] header still enters woken.
  node.ingest({
    step_index: 3,
    type: 'SYSTEM_MESSAGE',
    content:
      '<SYSTEM_MESSAGE>\nPlan mode is off: this turn runs on Gemini 4 Hydrogen (High).\n[Message] sender=22222222-2222-4222-8222-222222222222 priority=MESSAGE_PRIORITY_HIGH content=report\n</SYSTEM_MESSAGE>',
  })
  assert.equal(node.promptPhase, 'woken')
  assert.equal(node.wokeUp, true)

  node.projector.bindToolCallIds(['call-denied'])
  node.ingest({
    step_index: 5,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'call-denied', name: 'run_command', args: {} }],
  })
  assert.equal(node.promptPhase, 'woken')
  node.settleAbortedDenial(new Set(['call-denied']))
  assert.equal(node.promptPhase, 'foreground_idle')
  assert.equal(node.isWokenWorking(), false)
  assert.equal(
    node.isSettled({
      ...inbox,
      consumedNonUserCount: 1,
    }),
    true,
  )
})

test('observedAt anchors timestamp-less retry warnings, clamps future mtimeMs, survives reset(), and prunes deleted steps', (t) => {
  const started = 2_000_000
  t.mock.timers.enable({ apis: ['Date'], now: started })
  const node = new BrainNode(id)

  // Future mtimeMs is clamped to Date.now() so clock skew cannot extend the deadline beyond 15m.
  node.ingest(warning(1, 1), started + 600_000)
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS - 1)
  assert.equal(node.failure(), undefined)
  t.mock.timers.tick(1)
  assert.match(node.failure()!, /attempt 1.*No retry recovery was observed/)

  // O_TRUNC reset() preserves the original retryObservedAt deadline for an already-seen step.
  const rewriteNode = new BrainNode(id)
  const initialMtime = Date.now() - 300_000
  rewriteNode.ingest({ step_index: 1, type: 'USER_INPUT' }, initialMtime)
  rewriteNode.ingest(warning(2, 1), initialMtime)
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS - 300_000 - 1)
  assert.equal(rewriteNode.failure(), undefined)

  // Simulate O_TRUNC rewrite re-ingesting the same step at a later wall-clock time.
  rewriteNode.reset(inbox, [])
  rewriteNode.ingest({ step_index: 1, type: 'USER_INPUT' })
  rewriteNode.ingest(warning(2, 1))
  assert.equal(rewriteNode.failure(), undefined)
  t.mock.timers.tick(1)
  assert.match(
    rewriteNode.failure()!,
    /attempt 1.*No retry recovery was observed/,
    'reset() on O_TRUNC rewrite must not extend the retry deadline',
  )

  // Pruning via snapshotStepIndices releases deleted retry steps so reused step indices get a fresh deadline.
  rewriteNode.pruneRetrySteps(new Set()) // empty set is a no-op
  rewriteNode.reset(inbox, [])
  rewriteNode.ingest({ step_index: 1, type: 'USER_INPUT' })
  rewriteNode.ingest(warning(2, 1))
  assert.match(
    rewriteNode.failure()!,
    /attempt 1.*No retry recovery was observed/,
  )

  // In-place mutation at the same step_index (e.g. attempt 1 -> attempt 2) refreshes the deadline.
  rewriteNode.reset(inbox, [])
  rewriteNode.ingest({ step_index: 1, type: 'USER_INPUT' })
  rewriteNode.ingest(warning(2, 2))
  assert.equal(
    rewriteNode.failure(),
    undefined,
    'in-place warning mutation at the same step_index refreshes retryObservedAt',
  )
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS - 1)
  assert.equal(rewriteNode.failure(), undefined)
  t.mock.timers.tick(1)
  assert.match(
    rewriteNode.failure()!,
    /attempt 2.*No retry recovery was observed/,
  )

  rewriteNode.pruneRetrySteps(new Set([1])) // step 2 was removed in complete snapshot
  rewriteNode.reset(inbox, [])
  rewriteNode.ingest({ step_index: 1, type: 'USER_INPUT' })
  rewriteNode.ingest(warning(2, 3))
  assert.equal(
    rewriteNode.failure(),
    undefined,
    'pruned step index gets a fresh deadline when reused',
  )
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS - 1)
  assert.equal(rewriteNode.failure(), undefined)
  t.mock.timers.tick(1)
  assert.match(
    rewriteNode.failure()!,
    /attempt 3.*No retry recovery was observed/,
  )
})

test('initial transcript reads anchor timestamp-less retry warnings to file mtimeMs and O_TRUNC rewrites preserve retryObservedAt', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-mtime-'))
  const parentId = '11111111-1111-4111-8111-111111111111'
  const childId = '22222222-2222-4222-8222-222222222222'
  const liveChildId = '33333333-3333-4333-8333-333333333333'
  let handle: BrainWatchHandle | undefined

  const writeTranscript = (
    sessionId: string,
    rows: readonly Record<string, unknown>[],
    mtime?: Date,
  ) => writeBrainTranscript(root, sessionId, rows, mtime)

  try {
    await writeTranscript(parentId, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'invoke', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: childId }),
      },
      quiet(3),
    ])
    const subDir = join(root, parentId, '.system_generated/subagents')
    await mkdir(subDir, { recursive: true })
    await writeFile(
      join(subDir, `${childId}.json`),
      JSON.stringify({
        conversationId: childId,
        state: 'SUBAGENT_STATE_ALIVE',
        spawnStepIndex: 2,
        spawnCallId: 'invoke',
      }),
    )

    const childRows = [
      {
        step_index: 0,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=work\n</SYSTEM_MESSAGE>`,
      },
      warning(1, 1),
    ]
    const expiredDate = new Date(Date.now() - 20 * 60_000)
    await writeTranscript(childId, childRows, expiredDate)

    assert.equal(await hasActiveBackgroundWork(root, parentId), false)

    const watchEvents: ProviderEvent[] = []
    handle = watchBrain(root, childId, 'child', (e) => watchEvents.push(e), 5)
    await handle.ready
    assert.equal(handle.isRunning(), false)
    assert.ok(
      watchEvents.some(
        (e) =>
          e.type === 'session.turn' &&
          e.state === 'failed' &&
          /attempt 1.*No retry recovery was observed/s.test(
            e.error?.message ?? '',
          ),
      ),
    )
    handle()
    handle = undefined

    const settled = await settleBrainTurn(root, parentId, 'p', () => {}, {
      minStepIndex: -1,
      pollIntervalMs: 5,
    })
    assert.equal(settled.status, 'failed')
    assert.equal(settled.stalled, true)
    assert.match(settled.error ?? '', /attempt 1/)

    // Live retry warning: O_TRUNC rewrite mid-window must not extend the retry deadline.
    t.mock.timers.enable({ apis: ['Date'], now: Date.now() })
    await writeTranscript(liveChildId, childRows)
    const liveEvents: ProviderEvent[] = []
    handle = watchBrain(
      root,
      liveChildId,
      'live-child',
      (e) => liveEvents.push(e),
      60_000,
      undefined,
      undefined,
      undefined,
      10_000,
    )
    await handle.ready
    assert.equal(handle.isRunning(), true)
    t.mock.timers.tick(6_000)
    await writeTranscript(liveChildId, childRows)
    await handle.flush()
    assert.equal(handle.isRunning(), true)
    t.mock.timers.tick(4_100)
    await handle.flush()
    assert.equal(handle.isRunning(), false)
    assert.ok(
      liveEvents.some((e) => e.type === 'session.turn' && e.state === 'failed'),
    )
  } finally {
    handle?.()
    await rm(root, { recursive: true, force: true })
  }
})

test('BrainTurnObservation pre-attach buffering preserves mtimeMs across multi-turn preamble reads', async () => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-obs-'))
  const sessionId = '44444444-4444-4444-8444-444444444444'
  const writeRows = (rows: readonly Record<string, unknown>[], mtime?: Date) =>
    writeBrainTranscript(root, sessionId, rows, mtime)

  let observation: BrainTurnObservation | undefined
  try {
    // Turn 0 already on disk when Turn 1 observation starts.
    await writeRows([
      { step_index: 0, type: 'USER_INPUT', content: 'turn 0' },
      quiet(1),
    ])
    observation = new BrainTurnObservation(
      root,
      sessionId,
      'p',
      1,
      {
        isCurrent: () => true,
        onUsage: () => {},
        onItem: () => {},
      },
      10,
    )
    await observation.poll()

    // Turn 1 arrives and is buffered BEFORE settleBrainTurn calls attach().
    const expiredDate = new Date(Date.now() - 20 * 60_000)
    await writeRows(
      [
        { step_index: 0, type: 'USER_INPUT', content: 'turn 0' },
        quiet(1),
        { step_index: 2, type: 'USER_INPUT', content: 'turn 1' },
        warning(3, 1),
      ],
      expiredDate,
    )
    await observation.poll()

    const settled = await settleBrainTurn(root, sessionId, 'p', () => {}, {
      minStepIndex: 1,
      pollIntervalMs: 5,
      observation,
    })
    assert.equal(settled.status, 'failed')
    assert.equal(settled.stalled, true)
    assert.match(settled.error ?? '', /attempt 1/)
  } finally {
    await observation?.dispose()
    await rm(root, { recursive: true, force: true })
  }
})

test('settleChildren handles ENOENT -> appeared and torn initial reads without prematurely latching initialReadDone', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-torn-'))
  const parentId = '55555555-5555-4555-8555-555555555555'
  const childId = '66666666-6666-4666-8666-666666666666'
  const tornChildId = '77777777-7777-4777-8777-777777777777'
  let handle: BrainWatchHandle | undefined
  let obs: BrainTurnObservation | undefined
  const abort = new AbortController()
  let settlePromise:
    Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined

  const transcriptPath = (sessionId: string) =>
    join(root, sessionId, '.system_generated/logs/transcript_full.jsonl')
  const writeTranscript = (
    sessionId: string,
    rows: readonly Record<string, unknown>[],
    mtime?: Date,
  ) => writeBrainTranscript(root, sessionId, rows, mtime)

  try {
    t.mock.timers.enable({ apis: ['Date'], now: Date.now() })
    // 1. settleChildren: child starts as ENOENT, then appears with warning + O_TRUNC rewrite.
    await writeTranscript(parentId, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'invoke', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: childId }),
      },
      quiet(3),
    ])
    const subDir = join(root, parentId, '.system_generated/subagents')
    await mkdir(subDir, { recursive: true })
    await writeFile(
      join(subDir, `${childId}.json`),
      JSON.stringify({
        conversationId: childId,
        state: 'SUBAGENT_STATE_ALIVE',
        spawnStepIndex: 2,
        spawnCallId: 'invoke',
      }),
    )

    const stepEvents = new EventEmitter()
    const step3Seen = once(stepEvents, '3')
    obs = new BrainTurnObservation(
      root,
      parentId,
      'p',
      -1,
      { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
      60_000,
    )
    settlePromise = settleBrainTurn(root, parentId, 'p', () => {}, {
      minStepIndex: -1,
      retryTimeoutMs: 10_000,
      observation: obs,
      signal: abort.signal,
      onStep: (step) => {
        stepEvents.emit(String(step))
      },
    })

    await step3Seen
    // First poll runs while childId is ENOENT.
    await flushObs(obs)
    const childRows = [
      {
        step_index: 0,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=work\n</SYSTEM_MESSAGE>`,
      },
      warning(1, 1),
    ]
    await writeTranscript(childId, childRows)
    await flushObs(obs)
    t.mock.timers.tick(6_000)
    await writeTranscript(childId, childRows)
    await flushObs(obs)
    t.mock.timers.tick(4_100)
    await flushObs(obs)

    const settled = await settlePromise
    settlePromise = undefined
    assert.equal(settled.status, 'failed')
    assert.equal(settled.stalled, true)
    assert.match(settled.error ?? '', new RegExp(`Subagent ${childId} failed`))

    // 2. watchBrain: torn initial read (no trailing newline -> rows: []) must not latch initialReadDone.
    const expiredDate = new Date(Date.now() - 20 * 60_000)
    const line0 = JSON.stringify({
      step_index: 0,
      type: 'SYSTEM_MESSAGE',
      content: `<SYSTEM_MESSAGE>\n[Message] sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=work\n</SYSTEM_MESSAGE>`,
    })
    await writeFileAtomic(
      transcriptPath(tornChildId),
      line0.slice(0, 20),
      expiredDate,
    )

    const events: ProviderEvent[] = []
    handle = watchBrain(
      root,
      tornChildId,
      'torn-child',
      (e) => events.push(e),
      60_000,
    )
    await handle.ready
    assert.equal(
      handle.isRunning(),
      true,
      'torn initial read with rows: [] must not latch initialReadDone',
    )

    await writeTranscript(tornChildId, childRows, expiredDate)
    await handle.flush()
    assert.equal(handle.isRunning(), false)
    assert.ok(
      events.some(
        (e) =>
          e.type === 'session.turn' &&
          e.state === 'failed' &&
          /attempt 1.*No retry recovery was observed/s.test(
            e.error?.message ?? '',
          ),
      ),
    )
  } finally {
    abort.abort()
    await settlePromise?.catch(() => undefined)
    await obs?.dispose()
    handle?.()
    await rm(root, { recursive: true, force: true })
  }
})

test('StepDedupe.fresh re-delivers steps when error or source mutates in place', () => {
  const dedupe = new StepDedupe()
  const base = {
    step_index: 4,
    source: 'SYSTEM',
    type: 'ERROR_MESSAGE',
    status: 'DONE',
    content: null,
    error:
      'Your previous response contained an improperly formatted function call: Tool name must not be empty.\nRetries remaining: 3',
  }
  assert.equal(dedupe.fresh(base), true)
  assert.equal(dedupe.fresh({ ...base }), false)
  assert.equal(
    dedupe.fresh({ ...base, error: 'Fatal terminal error' }),
    true,
    'mutating row.error on O_TRUNC with content: null must re-deliver the step',
  )
  assert.equal(dedupe.fresh({ ...base, error: 'Fatal terminal error' }), false)
  assert.equal(
    dedupe.fresh({ ...base, source: 'MODEL', error: 'Fatal terminal error' }),
    true,
    'mutating row.source must re-deliver the step',
  )
})

test('BrainTurnObservation pre-attach buffering tracks observedAt per step without cross-poll contamination', async () => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-per-step-'))
  const s1 = '88888888-8888-4888-8888-888888888888'
  const s2 = '99999999-9999-4999-8999-999999999999'
  const writeRows = (
    sessionId: string,
    rows: readonly Record<string, unknown>[],
    mtime?: Date,
  ) => writeBrainTranscript(root, sessionId, rows, mtime)

  let obs1: BrainTurnObservation | undefined
  let obs2: BrainTurnObservation | undefined
  try {
    const expiredDate = new Date(Date.now() - 20 * 60_000)

    // Case 1: Poll 1 reads aged USER_INPUT (20m ago); Poll 2 appends fresh warning -> must NOT immediately expire.
    await writeRows(
      s1,
      [{ step_index: 0, type: 'USER_INPUT', content: 'start' }],
      expiredDate,
    )
    obs1 = new BrainTurnObservation(
      root,
      s1,
      'p1',
      -1,
      { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
      10,
    )
    await obs1.poll()

    await writeRows(s1, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 1),
    ])
    await obs1.poll()

    const stepEvents = new EventEmitter()
    const step1Seen = once(stepEvents, '1')
    let settled1: { status: string } | undefined
    const p1 = settleBrainTurn(root, s1, 'p1', () => {}, {
      minStepIndex: -1,
      observation: obs1,
      onStep: (step) => {
        stepEvents.emit(String(step))
      },
    }).then((res) => {
      settled1 = res
      return res
    })
    await step1Seen
    await flushObs(obs1)
    assert.equal(
      settled1,
      undefined,
      'freshly appended warning must not inherit earlier USER_INPUT aged mtimeMs',
    )
    await writeRows(s1, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 1),
      quiet(2),
    ])
    await flushObs(obs1)
    const result1 = await p1
    assert.equal(result1.status, 'completed')

    // Case 2: Poll 1 reads aged warning (20m ago); Poll 2 appends fresh RUNNING row -> still expires immediately.
    await writeRows(
      s2,
      [{ step_index: 0, type: 'USER_INPUT', content: 'start' }, warning(1, 1)],
      expiredDate,
    )
    obs2 = new BrainTurnObservation(
      root,
      s2,
      'p2',
      -1,
      { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
      10,
    )
    await obs2.poll()

    await writeRows(s2, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 1),
      { ...quiet(2), status: 'RUNNING' },
    ])
    await obs2.poll()

    const settled2 = await settleBrainTurn(root, s2, 'p2', () => {}, {
      minStepIndex: -1,
      pollIntervalMs: 10,
      observation: obs2,
    })
    assert.equal(settled2.status, 'failed')
    assert.equal(settled2.stalled, true)
    assert.match(settled2.error ?? '', /attempt 1/)
  } finally {
    await obs1?.dispose()
    await obs2?.dispose()
    await rm(root, { recursive: true, force: true })
  }
})

test('BrainTurnObservation preserves retry observedAt across pre-attach empty O_TRUNC rewrites and refreshes on in-place mutation', async () => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-preattach-otrunc-'))
  const s1 = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
  const s2 = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const transcriptPath = (sessionId: string) =>
    join(root, sessionId, '.system_generated/logs/transcript_full.jsonl')
  const writeRows = (
    sessionId: string,
    rows: readonly Record<string, unknown>[],
    mtime?: Date,
  ) => writeBrainTranscript(root, sessionId, rows, mtime)

  let obs1: BrainTurnObservation | undefined
  let obs2: BrainTurnObservation | undefined
  try {
    const expiredDate = new Date(Date.now() - 20 * 60_000)

    // Case 1: Aged warning -> truncated empty -> identical rows restored with fresh mtimeMs -> still expires immediately.
    const rows1 = [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 1),
    ]
    await writeRows(s1, rows1, expiredDate)
    obs1 = new BrainTurnObservation(
      root,
      s1,
      'p1',
      -1,
      { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
      10,
    )
    await obs1.poll()

    await writeFile(transcriptPath(s1), '')
    await obs1.poll()

    await writeRows(s1, rows1)
    await obs1.poll()

    const settled1 = await settleBrainTurn(root, s1, 'p1', () => {}, {
      minStepIndex: -1,
      pollIntervalMs: 10,
      observation: obs1,
    })
    assert.equal(settled1.status, 'failed')
    assert.equal(settled1.stalled, true)
    assert.match(settled1.error ?? '', /attempt 1/)

    // Case 2: Aged warning -> truncated empty -> mutated warning at same step -> refreshes deadline.
    await writeRows(s2, rows1, expiredDate)
    obs2 = new BrainTurnObservation(
      root,
      s2,
      'p2',
      -1,
      { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
      10,
    )
    await obs2.poll()

    await writeFile(transcriptPath(s2), '')
    await obs2.poll()

    await writeRows(s2, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 2),
    ])
    await obs2.poll()

    const stepEvents = new EventEmitter()
    const step1Seen = once(stepEvents, '1')
    let settled2: { status: string } | undefined
    const p2 = settleBrainTurn(root, s2, 'p2', () => {}, {
      minStepIndex: -1,
      observation: obs2,
      onStep: (step) => {
        stepEvents.emit(String(step))
      },
    }).then((res) => {
      settled2 = res
      return res
    })
    await step1Seen
    await flushObs(obs2)
    assert.equal(
      settled2,
      undefined,
      'mutated warning at same step across pre-attach O_TRUNC must refresh observedAt',
    )
    await writeRows(s2, [
      { step_index: 0, type: 'USER_INPUT', content: 'start' },
      warning(1, 2),
      quiet(2),
    ])
    await flushObs(obs2)
    const result2 = await p2
    assert.equal(result2.status, 'completed')
  } finally {
    await obs1?.dispose()
    await obs2?.dispose()
    await rm(root, { recursive: true, force: true })
  }
})

test('turn floors drop replayed retry warnings and terminal failures below them', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 2_000_000 })
  const node = new BrainNode(id)
  const oldWarning = warning(2, 1)
  node.ingest(oldWarning, Date.now() - RETRY_IDLE_TIMEOUT_MS)
  assert.match(node.failure()!, /No retry recovery/)
  node.advanceTurnFloor(2)
  node.reset(inbox, [])
  node.ingest(oldWarning)
  assert.equal(node.failure(), undefined)
})

test('a current-turn terminal failure survives a later report row', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 2_000_000 })
  const node = new BrainNode(id)
  node.ingest({ step_index: 3, type: 'ERROR_MESSAGE', content: 'fatal' })
  node.ingest({
    step_index: 4,
    type: 'SYSTEM_MESSAGE',
    content: 'background report',
  })
  assert.equal(node.failure(), 'fatal')
  node.advanceTurnFloor(4)
  assert.equal(node.failure(), undefined)
})

test('a retry above the turn floor starts its own deadline', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 2_000_000 })
  const node = new BrainNode(id)
  node.advanceTurnFloor(4)
  node.ingest(warning(5, 2))
  assert.equal(node.failure(), undefined)
  t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS)
  assert.match(node.failure()!, /attempt 2.*No retry recovery/)
})
