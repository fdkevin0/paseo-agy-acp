import assert from 'node:assert/strict'
import { test } from 'node:test'
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
    {
      step_index: 1,
      source: 'SYSTEM',
      type: 'ERROR_MESSAGE',
      status: 'DONE',
      content: null,
      error:
        'Your previous response contained an improperly formatted function call: Tool name must not be empty.\nRetries remaining: 3',
    },
    {
      ...warning(1),
      error:
        'Your previous response contained an improperly formatted function call: invalid JSON arguments\nRetries remaining: 2',
    },
    {
      ...warning(1),
      error:
        'Your previous response was blocked by content safety filters:\r\nRetries remaining: 1',
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
    {
      ...warning(1),
      error:
        'Your previous response contained an improperly formatted function call: invalid JSON\nRetries remaining: 0',
    },
    {
      ...warning(1),
      error:
        'Your previous response contained an improperly formatted function call: invalid JSON\nRetries remaining: 2 extra',
    },
    {
      ...warning(1),
      error:
        'Quoted: Your previous response was blocked by content safety filters:\nRetries remaining: 2',
    },
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
