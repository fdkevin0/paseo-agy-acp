import assert from 'node:assert/strict'
import { mkdir, mkdtemp, rm, utimes, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { test } from 'node:test'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import {
  hasActiveBackgroundWork,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
} from '../server/brain.ts'
import {
  BrainNode,
  BrainTranscriptProjector,
} from '../server/brain-projector.ts'
import {
  isRetryWarning,
  RETRY_IDLE_TIMEOUT_MS,
  type InboxState,
} from '../server/brain-io.ts'
import { Timeline } from '../server/mapping.ts'

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
  const positives = [
    warning(1),
    {
      ...warning(1),
      content:
        'Error: API error (attempt 2): request failed: synthetic unavailable',
    },
    {
      ...warning(1),
      error:
        'model output error: model output must contain either output text or tool calls, these cannot both be empty, please try again',
    },
    {
      ...warning(1),
      error:
        'The stream was interrupted. Please continue the task you were working on.',
    },
  ]
  for (const row of positives) {
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
  const negatives = [
    { ...warning(1), status: 'ERROR' },
    { ...warning(1), source: 'MODEL' },
    { ...warning(1), error: 'API error (attempt 0): invalid' },
    { ...warning(1), error: 'API error (attempt 1):' },
    { ...warning(1), error: 'quoted API error (attempt 1): network' },
    { ...warning(1), error: 'generic::unavailable: please try again' },
    {
      ...warning(1),
      error: 'model output error: generic::unavailable: please try again',
    },
    { ...warning(1), error: 'request failed: connection reset' },
  ]
  for (const row of negatives) {
    assert.equal(isRetryWarning(row), false)
    assert.equal(
      new BrainTranscriptProjector('p').project(row)[0].type,
      'error',
    )
  }
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
  for (const item of first) timeline.track(item)
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
  for (const item of replacement) timeline.track(item)
  for (const item of projector.project({
    step_index: 4,
    type: 'GENERIC',
    status: 'DONE',
    content: 'confirmed result',
  }))
    timeline.track(item)
  assert.deepEqual(timeline.terminalize('completed'), [])
})

test('confirmed model or correlated tool progress releases retry state; unrelated results do not', (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
  for (const progress of ['model', 'tool'] as const) {
    const node = new BrainNode(id)
    node.ingest({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ id: 'pending', name: 'run_command', args: {} }],
    })
    node.ingest(warning(2))
    node.ingest(
      progress === 'model'
        ? {
            step_index: 3,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [{ id: 'new', name: 'run_command', args: {} }],
          }
        : {
            step_index: 3,
            type: 'GENERIC',
            status: 'DONE',
            content: 'confirmed tool result',
          },
    )
    assert.equal(node.retryPending(), false)
    t.mock.timers.tick(RETRY_IDLE_TIMEOUT_MS + 1)
    assert.equal(
      node.failure(),
      undefined,
      'a recovered turn must not retain the retry deadline',
    )
  }
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

test('synthetic continuation denial settles cleanly even when turn wokeUp latch is true, and mixed planexecute/wakeup SYSTEM_MESSAGE enters woken', () => {
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

  // Woken question triggers a synthetic continuation prompt (wokeUp stays true, promptPhase resets to foreground).
  node.markQuestionContinuation(true)
  node.ingest({ step_index: 4, type: 'USER_INPUT', content: 'Proceed' })
  assert.equal(node.wokeUp, true)
  assert.equal(node.promptPhase, 'foreground')
  node.projector.bindToolCallIds(['call-denied'])
  node.ingest({
    step_index: 5,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'call-denied', name: 'run_command', args: {} }],
  })
  assert.equal(node.promptPhase, 'foreground_active')
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
  rewriteNode.ingest(warning(2, 2))
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

test('initial transcript reads anchor timestamp-less retry warnings to file mtimeMs and O_TRUNC rewrites preserve retryObservedAt', async () => {
  const root = await mkdtemp(join(tmpdir(), 'brain-retry-mtime-'))
  const parentId = '11111111-1111-4111-8111-111111111111'
  const childId = '22222222-2222-4222-8222-222222222222'
  const liveChildId = '33333333-3333-4333-8333-333333333333'
  let handle: BrainWatchHandle | undefined

  const transcriptPath = (sessionId: string) =>
    join(root, sessionId, '.system_generated/logs/transcript_full.jsonl')
  const writeTranscript = async (
    sessionId: string,
    rows: readonly Record<string, unknown>[],
  ) => {
    const dir = join(root, sessionId, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, 'transcript_full.jsonl'),
      rows.map((r) => JSON.stringify(r)).join('\n') + '\n',
    )
  }

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
    await writeTranscript(childId, childRows)
    const expiredDate = new Date(Date.now() - 20 * 60_000)
    await utimes(transcriptPath(childId), expiredDate, expiredDate)

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
    await writeTranscript(liveChildId, childRows)
    const liveEvents: ProviderEvent[] = []
    handle = watchBrain(
      root,
      liveChildId,
      'live-child',
      (e) => liveEvents.push(e),
      10,
      undefined,
      undefined,
      undefined,
      70,
    )
    await handle.ready
    assert.equal(handle.isRunning(), true)
    await delay(35)
    await writeTranscript(liveChildId, childRows)
    await delay(55)
    assert.equal(
      handle.isRunning(),
      false,
      'O_TRUNC rewrite at 35ms must not extend the 70ms retry deadline',
    )
    assert.ok(
      liveEvents.some((e) => e.type === 'session.turn' && e.state === 'failed'),
    )
  } finally {
    handle?.()
    await rm(root, { recursive: true, force: true })
  }
})
