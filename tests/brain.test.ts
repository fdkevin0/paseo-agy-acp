import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { EventEmitter, once } from 'node:events'
import { test, type TestContext } from 'node:test'
import {
  mkdir,
  open,
  readFile,
  writeFile,
  appendFile,
  rename,
  rm,
  symlink,
} from 'node:fs/promises'
import { join } from 'node:path'
import { MessageChannel } from 'node:worker_threads'
import {
  advanceForegroundCursor,
  BackgroundWorkProbe,
  captureRuntimeEpoch,
  hasActiveBackgroundWork,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
  type ForegroundAcpSegment,
} from '../server/brain.ts'
import {
  compactionEvents,
  createEventCollector,
  flushObs,
  sequenceDouble,
  toolCalls,
  writeFileAtomic,
} from './helpers.ts'
import recordedWakeup from './fixtures/agy-1.5.12-wakeup.json' with { type: 'json' }
import {
  countRowMessages,
  isRestartNotice,
  readInboxState,
  readLastStepIndex,
  readSubagents,
  TranscriptReader,
} from '../server/brain-io.ts'
import {
  BrainNode,
  BrainTranscriptProjector,
} from '../server/brain-projector.ts'
import { BrainTurnObservation } from '../server/brain-observer.ts'
import {
  child,
  done,
  encode,
  fixture,
  grandchild,
  invoke,
  message,
  parent,
  report,
  type Row,
} from './brain-fixtures.ts'
import { SubagentRegistry } from '../server/subagents.ts'
import { Timeline } from '../server/mapping.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'

const T0 = 1_700_000_000_000

/** settleBrainTurn's poll timer is unref'd; hold the event loop open while awaiting it. */
function alive<T>(promise: Promise<T>): Promise<T> {
  const { port1, port2 } = new MessageChannel()
  port1.on('message', () => {})
  return promise.finally(() => {
    port1.close()
    port2.close()
  })
}
function closeHandles(handles: readonly BrainWatchHandle[]): void {
  for (const close of handles) close()
}
function manualObs(
  root: string,
  nativeId: string,
  minStepIndex = -1,
  onItem: (item: ProviderTimelineItem) => void = () => {},
) {
  return new BrainTurnObservation(
    root,
    nativeId,
    'p',
    minStepIndex,
    { isCurrent: () => true, onUsage: () => {}, onItem },
    60_000,
  )
}
function createStepTracker() {
  const emitter = new EventEmitter()
  let highest = -1
  return {
    onStep(step: number) {
      highest = Math.max(highest, step)
      emitter.emit(String(step))
    },
    waitFor(step: number): Promise<unknown> {
      if (highest >= step) return Promise.resolve()
      return once(emitter, String(step))
    },
  }
}
function projectAll(projector: BrainTranscriptProjector, rows: readonly Row[]) {
  for (const row of rows) projector.project(row)
}
function ingestAll(node: BrainNode, rows: readonly Row[]) {
  for (const row of rows) node.ingest(row)
}
function eventKindSummary(event: ProviderEvent): string {
  if (event.type === 'session.turn') return `turn:${event.state}`
  if (event.type === 'timeline.item') {
    const status = 'status' in event.item ? event.item.status : ''
    return `${event.item.type}:${status}`
  }
  return event.type
}
function itemTag(item: ProviderTimelineItem): string {
  if (item.type === 'tool_call')
    return `${item.type}:${item.callId}:${item.status}`
  const text = 'text' in item ? item.text : ''
  return `${item.type}:${text}`
}
function toolCallIdOrType(item: ProviderTimelineItem): string {
  return item.type === 'tool_call' ? `${item.callId}:${item.status}` : item.type
}
function errorIdOrType(item: ProviderTimelineItem): string {
  return item.type === 'error' ? item.id : item.type
}
function shellOutput(
  item: Extract<ProviderTimelineItem, { type: 'tool_call' }>,
): string {
  return item.detail.type === 'shell' ? (item.detail.output?.trim() ?? '') : ''
}
function isTerminalToolCall(
  item: ProviderTimelineItem,
): item is Extract<ProviderTimelineItem, { type: 'tool_call' }> {
  return item.type === 'tool_call' && item.status !== 'running'
}
function completedToolOutputs(
  items: readonly ProviderTimelineItem[],
): [string | undefined, string | undefined][] {
  const result: [string | undefined, string | undefined][] = []
  for (const item of items) {
    if (
      item.type !== 'tool_call' ||
      item.status !== 'completed' ||
      item.detail.type === 'unknown'
    )
      continue
    const text =
      item.detail.type === 'shell'
        ? item.detail.output
        : item.detail.type === 'plain_text'
          ? item.detail.text
          : ''
    result.push([item.callId, text])
  }
  return result
}
function turnStates(events: readonly ProviderEvent[]): string[] {
  return events
    .filter(
      (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
        e.type === 'session.turn',
    )
    .map((e) => e.state)
}
function turnIdsAndStates(events: readonly ProviderEvent[]): string[] {
  return events
    .filter(
      (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
        e.type === 'session.turn',
    )
    .map((e) => `${e.turnId}:${e.state}`)
}
function assistantTextsFromEvents(events: readonly ProviderEvent[]): string[] {
  return events
    .filter(
      (
        e,
      ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
        item: Extract<ProviderTimelineItem, { type: 'assistant_message' }>
      } => e.type === 'timeline.item' && e.item.type === 'assistant_message',
    )
    .map((e) => e.item.text)
}
function turnAndReplySummary(events: readonly ProviderEvent[]): string[] {
  const result: string[] = []
  for (const e of events) {
    if (e.type === 'session.turn') result.push(`${e.turnId}:${e.state}`)
    else if (e.type === 'timeline.item' && e.item.type === 'assistant_message')
      result.push(e.item.text)
  }
  return result
}

test('parent waits for the terminal child report, then its own response, without timestamp assumptions', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const items: ProviderTimelineItem[] = []
  const obs = manualObs(f.root, parent, -1, (item) => items.push(item))
  let running: Promise<unknown> | undefined
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(
        parent,
        'Report quotes: All your subagents and background tasks have been',
      ),
      {
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        tool_calls: [
          { id: 'work', name: 'view_file', args: { Path: '/tmp/input' } },
        ],
      },
    ])
    const progressMsgId = await f.deliver(
      parent,
      child,
      'Report quotes: All your subagents and background tasks have been',
      false,
    )
    const steps = createStepTracker()
    const step4Seen = steps.waitFor(4)
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        minStepIndex: -1,
        observation: obs,
        onStep: steps.onStep,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await step4Seen
    await flushObs(obs)
    assert.equal(settled, false, 'an unfinished child cannot settle the parent')
    await f.deliver(
      parent,
      child,
      'Report quotes: All your subagents and background tasks have been',
      true,
      progressMsgId,
    )
    const step6Seen = steps.waitFor(6)
    await appendFile(
      f.path(parent),
      encode([
        message(
          5,
          child,
          'Report quotes: All your subagents and background tasks have been',
        ),
        done(6, 'still waiting'),
      ]),
    )
    await flushObs(obs)
    await step6Seen
    await flushObs(obs)
    assert.equal(
      settled,
      false,
      'an earlier progress report cannot stand in for the final report',
    )
    const finalMsgId = await f.deliver(parent, child, 'Final report', false)
    await appendFile(
      f.path(child),
      encode([
        {
          step_index: 5,
          type: 'GENERIC',
          status: 'DONE',
          content: 'read complete',
        },
        ...report(parent, 'Final report', 6),
      ]),
    )
    await flushObs(obs)
    await flushObs(obs)
    assert.equal(
      settled,
      false,
      'unread final report in parent inbox must block parent completion',
    )
    await f.deliver(parent, child, 'Final report', true, finalMsgId)
    const step7Seen = steps.waitFor(7)
    await appendFile(
      f.path(parent),
      encode([message(7, child, 'Final report')]),
    )
    await flushObs(obs)
    await step7Seen
    await flushObs(obs)
    assert.equal(
      settled,
      false,
      'delivery must be followed by a parent response',
    )
    await appendFile(
      f.path(parent),
      encode([
        {
          ...done(8, 'Summary'),
          input_tokens: 100,
          output_tokens: 20,
          cache_read_tokens: 30,
        },
      ]),
    )
    await flushObs(obs)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: {
        inputTokens: 100,
        outputTokens: 20,
        cachedReadTokens: 30,
      },
      wokeUp: true,
      wakeupUsage: {
        inputTokens: 100,
        outputTokens: 20,
        cachedReadTokens: 30,
      },
    })
    assert.equal(
      items.some((item) => item.type === 'assistant_message'),
      false,
    )
  } finally {
    abort.abort()
    await obs.dispose()
    await running
    await f.close()
  }
})

test('nested child completion uses delivered reports and sending to parent never creates a dependency cycle', async () => {
  const f = await fixture()
  const collector = createEventCollector()
  let stop: BrainWatchHandle | undefined
  try {
    await f.subagent(child, grandchild)
    await f.put(grandchild, [
      message(0, child, 'work'),
      ...report(child, 'Grandchild report'),
    ])
    await f.deliver(child, grandchild, 'Grandchild report', true)
    await f.subagent(parent, child)
    await f.put(child, [
      message(0, parent, 'work'),
      ...invoke(grandchild).slice(1),
      message(5, grandchild, 'Grandchild report'),
      ...report(parent, 'Child report', 6),
    ])
    stop = watchBrain(f.root, child, 'c', collector.emit, 5)
    await collector.wait(
      (event) => event.type === 'session.turn' && event.state === 'completed',
    )
    await f.deliver(parent, child, 'Child report', true)
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Child report'),
      done(6, 'Summary'),
    ])
    assert.deepEqual(await settleBrainTurn(f.root, parent, 'p', () => {}), {
      status: 'completed',
      lastStepUsage: undefined,
      wokeUp: true,
    })
  } finally {
    stop?.()
    await f.close()
  }
})

test('quoted restart notices do not clear background work', () => {
  const projector = new BrainTranscriptProjector(parent)
  const notice =
    '[Notice] All your subagents and background tasks have been stopped due to server restart.'
  projectAll(projector, invoke(child))
  projector.project({
    step_index: 5,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: 'run_command', args: { CommandLine: 'work' } }],
  })
  projector.project({
    step_index: 6,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${parent}/task-6`,
  })
  projector.project(message(7, child, notice))
  projector.project(message(8, 'system', `A report quotes: ${notice}`))
  assert.equal(projector.hasBackground(), true, 'background work is retained')
  projector.project(message(9, 'system', notice))
  assert.equal(projector.hasBackground(), false)
})

test('watching retains split UTF-8 records, resets on replacement and truncation, and supports cancel', async () => {
  const f = await fixture()
  const collector = createEventCollector()
  const { events } = collector
  let stop: BrainWatchHandle | undefined
  try {
    await f.put(child, [])
    const bytes = Buffer.from(encode([done(1, '你好')]))
    const split = bytes.indexOf(Buffer.from('你')) + 1
    await writeFile(f.path(child), bytes.subarray(0, split))
    stop = watchBrain(f.root, child, 'c', collector.emit, 5)
    await stop.ready
    assert.equal(events.length, 0)
    await appendFile(f.path(child), bytes.subarray(split))
    const textSeen = (text: string) =>
      events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === text,
      )
    await collector.waitCondition(() => textSeen('你好'))
    await collector.wait(
      (event) => event.type === 'session.turn' && event.state === 'completed',
    )
    await writeFile(
      f.path(child) + '.new',
      encode([done(2, 'replacement is longer')]),
    )
    await rename(f.path(child) + '.new', f.path(child))
    await collector.waitCondition(() => textSeen('replacement is longer'))
    await writeFile(f.path(child), encode([done(3, 'short')]))
    await stop.flush()
    assert.equal(textSeen('short'), true)
    const beforeFollowUp = events.length
    await appendFile(
      f.path(child),
      encode([
        { step_index: 4, type: 'CHECKPOINT', content: 'trailing checkpoint' },
        { step_index: 5, type: 'USER_INPUT', content: 'Continue' },
        {
          step_index: 6,
          type: 'PLANNER_RESPONSE',
          tool_calls: [
            {
              id: 'hang',
              name: 'run_command',
              args: { CommandLine: 'sleep 1' },
            },
          ],
        },
        { step_index: 7, type: 'ERROR_MESSAGE', content: 'Subagent failed' },
      ]),
    )
    await collector.waitCondition(() =>
      events
        .slice(beforeFollowUp)
        .some(
          (event) => event.type === 'session.turn' && event.state === 'failed',
        ),
    )
    assert.deepEqual(events.slice(beforeFollowUp).map(eventKindSummary), [
      'turn:started',
      'user_message:',
      'tool_call:running',
      'error:',
      'tool_call:failed',
      'turn:failed',
    ])
    const countBeforeCancel = events.length
    // cancel() drains rows already written, then ignores everything after it resolves.
    await stop.cancel()
    assert.equal(stop.isStopped(), true)
    await appendFile(
      f.path(child),
      encode([
        {
          step_index: 8,
          type: 'USER_INPUT',
          content: 'Ignored after cancel',
        },
      ]),
    )
    await stop.flush()
    await stop.cancel()
    assert.equal(events.length, countBeforeCancel)
  } finally {
    stop?.()
    await f.close()
  }
})

test('Brain rejects escaping session IDs and rows without an integer step', () => {
  assert.throws(() => watchBrain('/tmp', '../escape', 'c', () => {}))
  assert.equal(
    new BrainTranscriptProjector('c').project({ step_index: 'bad' }).length,
    0,
  )
})

test('Brain matches FIFO tool results without fake assistant bubbles or mutating pending IDs', () => {
  const projector = new BrainTranscriptProjector('child')
  const initItems = projector.project(message(0, parent, 'Initial task prompt'))
  assert.deepEqual(initItems, [
    {
      type: 'user_message',
      id: 'child:step:0',
      text: 'Initial task prompt',
    },
  ])
  const started = projector.project({
    type: 'PLANNER_RESPONSE',
    source: 'MODEL',
    step_index: 1,
    thinking: 'Thinking',
    content: 'Checking',
    tool_calls: [
      { id: 'one', name: 'run_command', args: { CommandLine: 'echo hello' } },
      { id: 'two', name: 'read_file', args: { Path: '/missing' } },
    ],
  })
  assert.deepEqual(
    started.map((i) => i.type),
    ['reasoning', 'assistant_message', 'tool_call', 'tool_call'],
  )
  assert.ok(started[2].type === 'tool_call' && started[2].status === 'running')
  assert.deepEqual(
    projector.project({
      type: 'CHECKPOINT',
      step_index: 2,
      content: 'metadata',
      thinking: 'hidden',
    }),
    [],
  )
  assert.deepEqual(
    projector.project({
      type: 'SYSTEM_MESSAGE',
      step_index: 2,
      content: 'metadata',
      thinking: 'hidden',
    }),
    [],
  )
  assert.deepEqual(
    projector.project({
      type: 'UNKNOWN',
      step_index: 2,
      content: 'metadata',
      thinking: 'hidden',
    }),
    [],
  )
  const doneItem = projector.project({
    type: 'GENERIC',
    source: 'MODEL',
    step_index: 3,
    status: 'DONE',
    content: 'Created At: today\nhello',
  })
  assert.equal(doneItem.length, 1)
  assert.ok(
    doneItem[0].type === 'tool_call' &&
      doneItem[0].status === 'completed' &&
      doneItem[0].detail.type === 'shell',
  )
  assert.equal(doneItem[0].id, started[2].id)
  assert.equal(doneItem[0].callId, started[2].callId)
  assert.equal(doneItem[0].detail.output, 'Created At: today\nhello')
  const failed = projector.project({
    type: 'GENERIC',
    source: 'MODEL',
    step_index: 4,
    status: 'ERROR',
    error: { message: 'not found' },
  })
  assert.ok(
    failed[0].type === 'tool_call' &&
      failed[0].status === 'failed' &&
      failed[0].error,
  )
  assert.equal(failed[0].id, started[3].id)
  assert.ok(
    failed[0].detail.type === 'read' &&
      failed[0].detail.content?.includes('not found'),
  )
  assert.deepEqual(
    projector.project({
      type: 'GENERIC',
      step_index: 5,
      content: 'orphan result',
    }),
    [],
  )
  assert.deepEqual(
    projector.project({
      type: 'ERROR_MESSAGE',
      source: 'SYSTEM',
      step_index: 6,
      content: 'Model error',
    }),
    [{ type: 'error', id: 'child:step:6:error', message: 'Model error' }],
  )
  assert.equal(
    projector.project({
      type: 'USER_INPUT',
      step_index: 7,
      content: 'hello',
    })[0].type,
    'user_message',
  )
  assert.equal(projector.hasBackground(), false)
  const bgTaskId = `${child}/task-9`
  const bgStarted = projector.project({
    type: 'PLANNER_RESPONSE',
    step_index: 8,
    tool_calls: [{ name: 'run_command', args: { CommandLine: 'sleep 5' } }],
  })
  assert.deepEqual(
    projector.project({
      type: 'GENERIC',
      step_index: 9,
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${bgTaskId}\n`,
    }),
    [],
  )
  assert.equal(projector.hasBackground(), true)
  projector.syncMetadata(
    {
      messages: [],
      readIds: new Set(),
      unconsumedHighPriority: [],
      consumedNonUserCount: 0,
      stepCallIds: new Map([[9, 'late-call-id']]),
    },
    [],
  )
  const bgDone = projector.project({
    type: 'SYSTEM_MESSAGE',
    step_index: 10,
    content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-08T17:00:00Z sender=${bgTaskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${bgTaskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\ndone\nLog: file:///tmp/task-9.log\n</SYSTEM_MESSAGE>`,
  })
  assert.equal(projector.hasBackground(), false)
  assert.equal(bgDone.length, 1)
  assert.equal(
    bgDone[0].id,
    bgStarted[0].id,
    'completed card must keep the emitted running card ID',
  )
  assert.ok(
    bgDone[0].type === 'tool_call' &&
      bgDone[0].status === 'completed' &&
      bgDone[0].detail.type === 'shell' &&
      bgDone[0].detail.output?.includes('done'),
  )
})

test('child failure fails the waiting parent instead of leaving it running', async () => {
  const f = await fixture()
  const collector = createEventCollector()
  let stop: BrainWatchHandle | undefined
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.put(child, [
      message(0, parent, 'work'),
      { step_index: 1, type: 'ERROR_MESSAGE', content: 'Child crashed' },
    ])
    const result = await settleBrainTurn(f.root, parent, 'p', () => {})
    assert.equal(result.status, 'failed')
    assert.match(result.error ?? '', /Child crashed/)
    stop = watchBrain(f.root, parent, 'p', collector.emit, 5)
    await collector.wait(
      (event) => event.type === 'session.turn' && event.state === 'failed',
    )
  } finally {
    stop?.()
    await f.close()
  }
})

test('a child can report to its parent and a sibling before completing', async () => {
  const f = await fixture()
  try {
    await f.subagent(parent, child)
    await f.deliver(parent, child, 'Parent report', true)
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Parent report'),
      done(6, 'Summary'),
    ])
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(parent, 'Parent report'),
      ...report(grandchild, 'Sibling report', 4),
    ])
    assert.deepEqual(await settleBrainTurn(f.root, parent, 'p', () => {}), {
      status: 'completed',
      lastStepUsage: undefined,
      wokeUp: true,
    })
  } finally {
    await f.close()
  }
})

test('silent terminal descendants settle and conditional schedules are released by their wakeup', async () => {
  const f = await fixture()
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.subagent(child, grandchild)
    await f.deliver(child, parent, 'continue', true)
    await f.put(child, [
      message(0, parent, 'work'),
      ...invoke(grandchild).slice(1),
      {
        step_index: 5,
        type: 'PLANNER_RESPONSE',
        tool_calls: [
          { id: 'timer', name: 'schedule', args: { TimerCondition: 'any' } },
        ],
      },
      {
        step_index: 6,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${child}/task-6`,
      },
      message(7, parent, 'continue'),
      done(8),
    ])
    await f.put(grandchild, [message(0, child, 'work'), done(1)])
    assert.deepEqual(await settleBrainTurn(f.root, parent, 'p', () => {}), {
      status: 'completed',
      lastStepUsage: undefined,
    })
    const projector = new BrainTranscriptProjector(child)
    const task1 = `${child}/task-1`
    const timer2 = `${child}/task-2`
    projector.project({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      tool_calls: [
        { id: 'timer', name: 'schedule', args: { TimerCondition: task1 } },
      ],
    })
    projector.project({
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${timer2}`,
    })
    projector.project(message(3, 'unrelated', 'hello'))
    assert.equal(projector.hasBackground(), true)
    projector.project(message(4, task1, 'finished'))
    assert.equal(projector.hasBackground(), false)
  } finally {
    await f.close()
  }
})

async function assertRetryWaitsForInboxRead(previous: Row) {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent)
  let running: Promise<unknown> | undefined
  try {
    await f.put(parent, [
      ...invoke(child),
      ...report(child, 'Retry request', 5),
      done(8, 'waiting'),
    ])
    await f.subagent(parent, child)
    const retryMsgId = await f.deliver(child, parent, 'Retry request', false)
    await f.put(child, [message(0, parent, 'work'), previous])
    const steps = createStepTracker()
    const step8Seen = steps.waitFor(8)
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: -1,
        observation: obs,
        onStep: steps.onStep,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await step8Seen
    await flushObs(obs)
    assert.equal(
      settled,
      false,
      'the unread message in child inbox must prevent premature settle',
    )
    await f.deliver(child, parent, 'Retry request', true, retryMsgId)
    await appendFile(
      f.path(child),
      encode([message(2, parent, 'Retry request'), done(3)]),
    )
    await flushObs(obs)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: undefined,
    })
  } finally {
    abort.abort()
    await obs.dispose()
    await running
    await f.close()
  }
}

test('retrying an owned child waits for inbox read instead of accepting old DONE or ERROR', async () => {
  await assertRetryWaitsForInboxRead(done(1))
  await assertRetryWaitsForInboxRead({
    step_index: 1,
    type: 'ERROR_MESSAGE',
    content: 'Previous error',
  })
})

test('an explicit schedule cancellation retains the canceled tool result', () => {
  const projector = new BrainTranscriptProjector(parent)
  const timerId = `${parent}/task-2`
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      { id: 'schedule', name: 'schedule', args: { TimerCondition: 'any' } },
    ],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${timerId}`,
  })
  const items = projector.project(
    message(
      3,
      timerId,
      `Task id "${timerId}" was canceled with result:\ncanceled`,
    ),
  )
  assert.equal(projector.hasBackground(), false)
  assert.equal(items.length, 1)
  assert.ok(
    items[0].type === 'tool_call' &&
      items[0].callId === 'schedule' &&
      items[0].status === 'canceled',
  )
})

test('batched SYSTEM_MESSAGE with multiple task completions resolves all tasks and settles the turn', async () => {
  const f = await fixture()
  try {
    const taskA = `${parent}/task-2`
    const taskB = `${parent}/task-3`
    await f.deliver(
      parent,
      taskA,
      `Task id "${taskA}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nalpha\nLog: file:///tmp/task-2.log`,
      true,
      'msg-a',
      {
        conversationId: parent,
        stepIndex: 2,
        callId: 'call-a',
        name: 'run_command',
      },
    )
    await f.deliver(
      parent,
      taskB,
      `Task id "${taskB}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nbeta\nLog: file:///tmp/task-3.log`,
      true,
      'msg-b',
      {
        conversationId: parent,
        stepIndex: 3,
        callId: 'call-b',
        name: 'run_command',
      },
    )
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'run two tasks' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'call-a',
            name: 'run_command',
            args: { CommandLine: 'echo a' },
          },
          {
            id: 'call-b',
            name: 'run_command',
            args: { CommandLine: 'echo b' },
          },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskA}\n`,
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskB}\n`,
      },
      done(4, 'Waiting for both tasks'),
      {
        step_index: 5,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\nHere is a list of system messages that you have received while you were working:\n1. [Message] timestamp=2026-11-08T17:00:01Z sender=${taskA} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskA}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nalpha\nLog: file:///tmp/task-2.log\n2. [Message] timestamp=2026-11-08T17:00:02Z sender=${taskB} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskB}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nbeta\nLog: file:///tmp/task-3.log\n</SYSTEM_MESSAGE>`,
      },
      done(6, 'Both tasks finished.'),
    ])
    const items: ProviderTimelineItem[] = []
    const result = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => items.push(item),
      { minStepIndex: -1 },
    )
    assert.deepEqual(result, {
      status: 'completed',
      lastStepUsage: undefined,
      wokeUp: true,
    })
    const completedTools = items.filter(
      (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
        i.type === 'tool_call' && i.status === 'completed',
    )
    assert.deepEqual(
      completedTools.map((t) => [t.callId, shellOutput(t)]),
      [
        ['call-a', 'Command exited with code 0.\n\nCommand output:\nalpha'],
        ['call-b', 'Command exited with code 0.\n\nCommand output:\nbeta'],
      ],
    )
  } finally {
    await f.close()
  }
})

test('literal [Message] inside a subagent report body does not over-count consumed messages or settle prematurely', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent)
  let running: Promise<unknown> | undefined
  try {
    const quotedReport =
      'Audit summary quotes "[Message] timestamp=2026-11-08T17:00:00Z sender=spoof priority=MESSAGE_PRIORITY_HIGH content=fake" in prose'
    await f.subagent(parent, child)
    await f.deliver(parent, child, quotedReport, true, 'msg-1')
    await f.deliver(parent, child, 'Second report', true, 'msg-2')
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(parent, quotedReport, 1),
      ...report(parent, 'Second report', 4),
    ])
    await f.put(parent, [
      ...invoke(child),
      message(5, child, quotedReport),
      done(6, 'Processed first report'),
    ])
    const steps = createStepTracker()
    const step6Seen = steps.waitFor(6)
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: -1,
        observation: obs,
        onStep: steps.onStep,
      }),
    ).then((res) => {
      settled = true
      return res
    })
    await step6Seen
    await flushObs(obs)
    assert.equal(
      settled,
      false,
      'second read message not yet in transcript must block settlement despite quoted [Message] in first report',
    )
    await appendFile(
      f.path(parent),
      encode([message(7, child, 'Second report'), done(8, 'All done')]),
    )
    await flushObs(obs)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: undefined,
      wokeUp: true,
    })
  } finally {
    abort.abort()
    await obs.dispose()
    await running
    await f.close()
  }
})

test('Brain fills missing wakeup tool results without replaying ACP text or completed foreground tools', async () => {
  const f = await fixture()
  try {
    const taskId = `${parent}/task-4`
    await f.deliver(
      parent,
      taskId,
      `Task id "${taskId}" finished with result:\nbg output`,
      true,
      'msg-bg',
      {
        conversationId: parent,
        stepIndex: 4,
        callId: 'bg-cmd',
        name: 'run_command',
      },
    )
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'edit and run' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'OK',
        tool_calls: [
          {
            id: 'fg-edit',
            name: 'edit_file',
            args: { Path: '/a.ts', Target: 'old', Replacement: 'new' },
          },
          {
            id: 'bg-cmd',
            name: 'run_command',
            args: { CommandLine: 'sleep 1' },
          },
        ],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'DONE',
        content: 'Edited file',
      },
      {
        step_index: 4,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskId}\n`,
      },
      done(5, 'OK'),
      message(
        6,
        taskId,
        `Task id "${taskId}" finished with result:\nbg output`,
      ),
      {
        step_index: 7,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'post-read', name: 'read_file', args: { Path: '/a.ts' } },
        ],
      },
      {
        step_index: 8,
        type: 'GENERIC',
        status: 'DONE',
        content: 'file contents',
      },
      done(9, 'OK'),
    ])
    const items: ProviderTimelineItem[] = []
    const snapshots = new Map([
      [
        'fg-edit',
        { toolCallId: 'fg-edit', title: 'Edit', rawOutput: 'ACP output' },
      ],
    ])
    const result = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => items.push(item),
      {
        minStepIndex: 0,
        toolSnapshot: (id) => snapshots.get(id),
      },
    )
    assert.equal(result.status, 'completed')
    assert.equal(
      items.some((i) => i.type === 'tool_call' && i.callId === 'fg-edit'),
      false,
      'already-completed foreground tool call must not be re-emitted',
    )
    assert.deepEqual(items.map(itemTag), [
      'tool_call:bg-cmd:completed',
      'tool_call:post-read:completed',
    ])
  } finally {
    await f.close()
  }
})

test('hasActiveBackgroundWork preserves Turn 1 background tasks in Turn 2 and ignores completed or restarted Turn 1 subagents', async () => {
  const f = await fixture()
  try {
    await f.subagent(parent, child)
    await f.put(child, [message(0, parent, 'work'), done(1, 'Child done')])
    await f.put(parent, [
      ...invoke(child),
      { step_index: 5, type: 'USER_INPUT', content: 'Turn 2 question' },
      done(6, 'Turn 2 answer'),
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      false,
      'completed Turn 1 subagent must not be treated as active in Turn 2',
    )

    // Interrupted Turn 1 subagent followed by server restart notice in parent
    await f.put(child, [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        tool_calls: [
          {
            id: 'hang',
            name: 'run_command',
            args: { CommandLine: 'sleep 60' },
          },
        ],
      },
    ])
    await f.put(parent, [
      ...invoke(child),
      message(
        5,
        'system',
        '[Notice] All your subagents and background tasks have been stopped due to server restart.',
      ),
      { step_index: 6, type: 'USER_INPUT', content: 'Turn 2 after restart' },
      done(7, 'Turn 2 answer'),
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      false,
      'Turn 1 subagent stopped before server restart notice must not be active in Turn 2',
    )

    // Turn 1 background command still running during Turn 2
    const bgTaskId = `${parent}/task-3`
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'Turn 1 start bg' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'bg', name: 'run_command', args: { CommandLine: 'sleep 30' } },
        ],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${bgTaskId}\n`,
      },
      done(4, 'Started background task'),
      { step_index: 5, type: 'USER_INPUT', content: 'Turn 2 while bg runs' },
      done(6, 'Still working'),
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      true,
      'Turn 1 background task must remain active across Turn 2 USER_INPUT',
    )

    // Parent in an active post-RPC Reactive Wakeup turn after all children settled
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'Turn 1' },
      done(2, 'Foreground idle'),
      message(3, child, 'Child report'),
      {
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'sync-tool', name: 'write_file', args: { Path: '/out.md' } },
        ],
      },
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      true,
      'unsettled post-RPC wakeup turn on parent must count as active work',
    )
  } finally {
    await f.close()
  }
})

test('GENERIC rows consume one call each and map native terminal statuses', () => {
  const projector = new BrainTranscriptProjector('p')
  const call = (id: string) => ({ id, name: 'run_command', args: {} })
  const items = [
    { step_index: 1, type: 'USER_INPUT', content: 'tools' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        'running',
        'waiting',
        'canceled',
        'interrupted',
        'failed',
      ].map(call),
    },
    // agy logs a step once; RUNNING/WAITING rows are never followed by a final row.
    { step_index: 3, type: 'GENERIC', status: 'RUNNING', content: 'busy' },
    { step_index: 4, type: 'GENERIC', status: 'WAITING', content: 'approve?' },
    { step_index: 5, type: 'GENERIC', status: 'CANCELED', content: 'stop' },
    { step_index: 6, type: 'GENERIC', status: 'INTERRUPTED', content: 'cut' },
    { step_index: 7, type: 'GENERIC', status: 'ERROR', content: 'boom' },
  ].flatMap((row) => projector.project(row))
  assert.deepEqual(
    items.filter(isTerminalToolCall).map((i) => `${i.callId}:${i.status}`),
    ['canceled:canceled', 'interrupted:canceled', 'failed:failed'],
  )
  assert.equal(projector.hasPending(), false, 'no call is left to misattribute')
  assert.equal(projector.hasBackground(), false)
})

test('an in-place O_TRUNC rewrite re-delivers changed steps once and drops unlogged rows', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const items: ProviderTimelineItem[] = []
  const obs = manualObs(f.root, parent, -1, (item) => items.push(item))
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    const plan = {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ id: 'cmd', name: 'run_command', args: {} }],
    }
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      plan,
      { step_index: 3, type: 'GENERIC', status: 'RUNNING', content: 'busy' },
    ])
    const stepEvents = new EventEmitter()
    const step3Seen = once(stepEvents, '3')
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        observation: obs,
        shouldUpdateTool: () => true,
        onStep: (step) => {
          stepEvents.emit(String(step))
        },
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await step3Seen
    await flushObs(obs)
    assert.equal(settled, false)
    // Same inode, larger file: only content probes can notice the rewrite.
    await f.rewrite(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      plan,
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'DONE',
        content: 'finished ok',
      },
      { step_index: 4, type: 'PLANNER_RESPONSE', status: 'GENERATING' },
    ])
    await flushObs(obs)
    assert.equal(settled, false, 'a GENERATING row is superseded, not final')
    await appendFile(f.path(parent), encode([done(4, 'All done')]))
    await flushObs(obs)
    assert.equal((await running).status, 'completed')
    assert.deepEqual(
      items.map(toolCallIdOrType),
      ['cmd:completed'],
      'the rewritten DONE step is delivered once; unchanged steps are not repeated',
    )
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('settlement waits for a late USER_INPUT flush within the grace period', async (t) => {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent, 0)
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, [{ step_index: 0, type: 'USER_INPUT', content: 'old' }])
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        minStepIndex: 0,
        signal: abort.signal,
        observation: obs,
        transcriptGraceMs: 2000,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await flushObs(obs)
    t.mock.timers.tick(40)
    await flushObs(obs)
    assert.equal(settled, false, 'must not give up before the row is flushed')
    await appendFile(
      f.path(parent),
      encode([
        { step_index: 1, type: 'USER_INPUT', content: 'new' },
        {
          step_index: 2,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          content: 'hi',
          input_tokens: 10,
          cache_read_tokens: 5,
          output_tokens: 2,
        },
      ]),
    )
    await flushObs(obs)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: { inputTokens: 10, cachedReadTokens: 5, outputTokens: 2 },
    })
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('settlement without grace completes immediately when no turn row exists', async () => {
  const f = await fixture()
  const obs = manualObs(f.root, parent, 0)
  try {
    await f.put(parent, [{ step_index: 0, type: 'USER_INPUT', content: 'old' }])
    // Unstarted observation only polls once, so resolving proves immediate completion without waiting.
    assert.deepEqual(
      await settleBrainTurn(f.root, parent, 'p', () => {}, {
        minStepIndex: 0,
        observation: obs,
      }),
      { status: 'completed' },
    )
  } finally {
    await obs.dispose()
    await f.close()
  }
})

test('a missing transcript never throws and only waits when the session directory exists', async (t) => {
  const f = await fixture()
  const abort = new AbortController()
  const obs1 = manualObs(f.root, parent)
  const obs2 = manualObs(f.root, parent)
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    // Unstarted observation only polls once, so resolving proves no grace applies without a session directory.
    assert.deepEqual(
      await settleBrainTurn(f.root, parent, 'p', () => {}, {
        observation: obs1,
        transcriptGraceMs: 2000,
      }),
      { status: 'completed' },
    )
    // Earlier subagents and no transcript used to throw ENOENT.
    await f.subagent(parent, child)
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs2,
        transcriptGraceMs: 100,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await flushObs(obs2)
    t.mock.timers.tick(99)
    await flushObs(obs2)
    assert.equal(settled, false, 'directory exists: grace applies')
    t.mock.timers.tick(1)
    await flushObs(obs2)
    assert.deepEqual(await running, { status: 'completed' })
  } finally {
    abort.abort()
    await obs1.dispose()
    await obs2.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('settlement stops waiting on an idle transcript without a final response', async (t) => {
  const f = await fixture()
  const items: ProviderTimelineItem[] = []
  const obs = manualObs(f.root, parent, -1, (item) => items.push(item))
  const abort = new AbortController()
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    const steps = createStepTracker()
    const step2Seen = steps.waitFor(2)
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        observation: obs,
        stallTimeoutMs: 50,
        onStep: steps.onStep,
      }),
    )
    await step2Seen
    t.mock.timers.tick(50)
    await flushObs(obs)
    const result = await running
    running = undefined
    assert.equal(result.status, 'failed')
    assert.match(result.error ?? '', /idle for 0s without a final response/)
    assert.deepEqual(items.map(errorIdOrType), ['p:step:2:stalled'])
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('a settled ALIVE child does not suspend the parent stall timeout', async (t) => {
  const f = await fixture()
  const items: ProviderTimelineItem[] = []
  const obs = manualObs(f.root, parent, -1, (item) => items.push(item))
  const abort = new AbortController()
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    // agy keeps finished subagents ALIVE until an explicit kill.
    await f.subagent(parent, child)
    await f.deliver(parent, child, 'Report', true, 'report')
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(parent, 'Report'),
    ])
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Report'),
      { step_index: 6, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    const steps = createStepTracker()
    const step6Seen = steps.waitFor(6)
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        signal: abort.signal,
        observation: obs,
        stallTimeoutMs: 50,
        onStep: steps.onStep,
      }),
    )
    await step6Seen
    t.mock.timers.tick(50)
    await flushObs(obs)
    const result = await running
    running = undefined
    assert.equal(result.status, 'failed')
    assert.ok(
      items.some((i) => i.type === 'error' && i.id.endsWith(':stalled')),
    )
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('woken-turn LLM generation after SYSTEM_MESSAGE or completed GENERIC uses wokenStallTimeoutMs and stalls if that cap expires', async (t) => {
  const f = await fixture()
  const abort = new AbortController()
  const items: ProviderTimelineItem[] = []
  const stalledItems: ProviderTimelineItem[] = []
  const obs1 = manualObs(f.root, parent, -1, (item) => items.push(item))
  const obs2 = manualObs(f.root, parent, -1, (item) => stalledItems.push(item))
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  let stalledRunning:
    Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.subagent(parent, child)
    await f.deliver(parent, child, 'Report', true, 'report')
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(parent, 'Report'),
    ])
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Report'),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'r1', name: 'read_file', args: { Path: '/a.ts' } }],
      },
      { step_index: 7, type: 'GENERIC', status: 'DONE', content: 'code' },
    ])
    const stepEvents = new EventEmitter()
    const step7Seen = once(stepEvents, '7')
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        signal: abort.signal,
        observation: obs1,
        stallTimeoutMs: 25,
        wokenStallTimeoutMs: 200,
        onStep: (step) => {
          stepEvents.emit(String(step))
        },
      }),
    ).then((res) => {
      settled = true
      return res
    })
    await step7Seen
    t.mock.timers.tick(70)
    await flushObs(obs1)
    assert.equal(
      settled,
      false,
      'post-GENERIC LLM generation in a woken turn must not trigger the short stallTimeoutMs',
    )
    await appendFile(
      f.path(parent),
      encode([
        {
          step_index: 8,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            { id: 'w1', name: 'write_file', args: { Path: '/out.md' } },
          ],
        },
        { step_index: 9, type: 'GENERIC', status: 'DONE', content: 'Created' },
        done(10, 'Final synthesis complete.'),
      ]),
    )
    await flushObs(obs1)
    const result = await running
    running = undefined
    assert.equal(result.status, 'completed')
    assert.equal(
      items.some((i) => i.type === 'error'),
      false,
    )
    assert.equal(result.wokeUp, true)

    // A woken turn that never produces a follow-up response after SYSTEM_MESSAGE or GENERIC still stalls at wokenStallTimeoutMs.
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Report'),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'r1', name: 'read_file', args: { Path: '/a.ts' } }],
      },
      { step_index: 7, type: 'GENERIC', status: 'DONE', content: 'code' },
    ])
    const stepEvents2 = new EventEmitter()
    const step7Seen2 = once(stepEvents2, '7')
    let stalledSettled = false
    stalledRunning = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => stalledItems.push(item), {
        minStepIndex: -1,
        signal: abort.signal,
        observation: obs2,
        stallTimeoutMs: 20,
        wokenStallTimeoutMs: 80,
        onStep: (step) => {
          stepEvents2.emit(String(step))
        },
      }),
    ).then((res) => {
      stalledSettled = true
      return res
    })
    await step7Seen2
    t.mock.timers.tick(79)
    await flushObs(obs2)
    assert.equal(
      stalledSettled,
      false,
      'woken stall must wait for wokenStallTimeoutMs (80ms), not stallTimeoutMs (20ms)',
    )
    t.mock.timers.tick(1)
    await flushObs(obs2)
    const stalledResult = await stalledRunning
    stalledRunning = undefined
    assert.equal(stalledResult.status, 'failed')
    assert.equal(stalledResult.stalled, true)
    assert.ok(
      stalledItems.some(
        (i) => i.type === 'error' && i.id === 'p:step:7:stalled',
      ),
    )
  } finally {
    abort.abort()
    await obs1.dispose()
    await obs2.dispose()
    await running?.catch(() => undefined)
    await stalledRunning?.catch(() => undefined)
    await f.close()
  }
})

test('unmatched foreground tool calls and permanent consumed-message drift still stall at stallTimeoutMs', async (t) => {
  const f = await fixture()
  const abort = new AbortController()
  const obs1 = manualObs(f.root, parent)
  const obs2 = manualObs(f.root, parent)
  let orphanRunning:
    Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  let driftRunning:
    Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    // 1. Unmatched foreground tool call (!wokeUp with hasPending() === true)
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'orphan', name: 'read_file', args: {} }],
      },
    ])
    const orphanSteps = new EventEmitter()
    const orphanStep2 = once(orphanSteps, '2')
    orphanRunning = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs1,
        stallTimeoutMs: 30,
        wokenStallTimeoutMs: 500,
        onStep: (step) => {
          orphanSteps.emit(String(step))
        },
      }),
    )
    await orphanStep2
    t.mock.timers.tick(30)
    await flushObs(obs1)
    const orphanResult = await orphanRunning
    orphanRunning = undefined
    assert.equal(orphanResult.status, 'failed')
    assert.equal(orphanResult.stalled, true)

    // 2. Permanent gap between inbox.consumedNonUserCount and rendered [Message] sender= lines
    await f.deliver(parent, child, 'Unrendered message', true, 'drift')
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      done(2, 'Done without rendering inbox message'),
    ])
    const driftSteps = new EventEmitter()
    const driftStep2 = once(driftSteps, '2')
    driftRunning = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs2,
        stallTimeoutMs: 30,
        wokenStallTimeoutMs: 500,
        onStep: (step) => {
          driftSteps.emit(String(step))
        },
      }),
    )
    await driftStep2
    t.mock.timers.tick(30)
    await flushObs(obs2)
    const driftResult = await driftRunning
    driftRunning = undefined
    assert.equal(driftResult.status, 'failed')
    assert.equal(driftResult.stalled, true)
  } finally {
    abort.abort()
    await obs1.dispose()
    await obs2.dispose()
    await orphanRunning?.catch(() => undefined)
    await driftRunning?.catch(() => undefined)
    await f.close()
  }
})

test('a restart notice inside a woken turn keeps accounting for the woken response', async () => {
  const f = await fixture()
  try {
    const notice = message(
      4,
      'system',
      '[Notice] All your subagents and background tasks have been stopped due to server restart.',
    )
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'go' },
      done(2, 'Foreground answer'),
      message(3, child, 'Wake up'),
      notice,
      {
        ...done(5, 'Woken answer after notice'),
        input_tokens: 10,
        output_tokens: 2,
      },
    ])
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 50,
      }),
    )
    assert.equal(result.status, 'completed')
    assert.equal(result.wakeupUsage?.inputTokens, 10)

    // The restarted runtime has no woken turn left running, even if a stopped child's report lands after the notice.
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'go' },
      done(2, 'Foreground answer'),
      message(3, child, 'Wake up'),
      notice,
      message(5, child, 'Late report'),
    ])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
    await f.close()
  }
})

test('hasActiveBackgroundWork tracks woken and about-to-wake parent turns without false positives from native GENERIC rows', async () => {
  const f = await fixture()
  try {
    await f.subagent(parent, child)
    await f.deliver(parent, child, 'Report', true, 'report')
    await f.put(child, [
      message(0, parent, 'work'),
      ...report(parent, 'Report'),
    ])

    // Woken parent awaiting LLM generation after SYSTEM_MESSAGE or GENERIC is active
    await f.put(parent, [...invoke(child), message(5, child, 'Report')])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

    await appendFile(
      f.path(parent),
      encode([
        {
          step_index: 6,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [{ id: 'r1', name: 'read_file', args: {} }],
        },
        { step_index: 7, type: 'GENERIC', status: 'DONE', content: 'ok' },
      ]),
    )
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

    // Once the woken parent emits its final PLANNER_RESPONSE, it is no longer active
    await appendFile(f.path(parent), encode([done(8, 'All done')]))
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)

    // Woken parent ending on a CANCELED GENERIC row is not working
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Report'),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'r1', name: 'read_file', args: {} }],
      },
      { step_index: 7, type: 'GENERIC', status: 'CANCELED' },
    ])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)

    // A synchronous GENERIC row followed by SYSTEM_MESSAGE without a completed foreground response does not count as woken
    await f.put(parent, [
      { step_index: 10, type: 'USER_INPUT', content: '/compact' },
      { step_index: 11, type: 'GENERIC', status: 'DONE', content: 'compacted' },
      { step_index: 12, type: 'SYSTEM_MESSAGE', content: 'info' },
    ])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)

    // After the foreground goes idle, a wakeup message agy has read but not yet logged is about to wake the parent
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Report'),
      done(6, 'All done'),
    ])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
    await f.deliver(parent, child, 'Unlogged wakeup', true, 'unlogged')
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)
  } finally {
    await f.close()
  }
})

async function assertWaitSuspendsStall(
  t: TestContext,
  rows: Row[],
  lastStep: number,
  isWaitingOnUser: () => boolean,
  label: string,
) {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent)
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, rows)
    const steps = createStepTracker()
    const stepSeen = steps.waitFor(lastStep)
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs,
        stallTimeoutMs: 20,
        isWaitingOnUser,
        onStep: steps.onStep,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await stepSeen
    t.mock.timers.tick(100)
    await flushObs(obs)
    assert.equal(settled, false, `a pending ${label} is not a stall`)
    abort.abort()
    assert.equal((await running).status, 'canceled')
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
}

test('a pending approval suspends the stall timeout', (t) =>
  assertWaitSuspendsStall(
    t,
    [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ],
    2,
    () => true,
    'approval',
  ))

test('a pending background task suspends the stall timeout', (t) =>
  assertWaitSuspendsStall(
    t,
    [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'bg', name: 'run_command', args: {} }],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${parent}/task-3\n`,
      },
      done(4, 'Waiting'),
    ],
    4,
    () => false,
    'background task',
  ))

test('unknown subagent states do not count as active background work', async () => {
  const f = await fixture()
  try {
    await f.subagent(parent, child, 'SUBAGENT_STATE_FINISHED')
    await f.put(child, [message(0, parent, 'work')])
    await f.put(parent, [...invoke(child)])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
    await f.close()
  }
})

test('readLastStepIndex reads the tail past large rows and torn writes', async () => {
  const f = await fixture()
  try {
    assert.equal(await readLastStepIndex(f.root, parent), -1)
    const big = 'x'.repeat(200 * 1024)
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'a' },
      done(1),
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'DONE', content: big },
    ])
    assert.equal(await readLastStepIndex(f.root, parent), 2)
    await appendFile(f.path(parent), '{"step_index": 3, "type": "PLAN')
    assert.equal(await readLastStepIndex(f.root, parent), 2)
  } finally {
    await f.close()
  }
})

test('a resumed child watcher skips its history and continues turn IDs', async () => {
  const f = await fixture()
  const collector = createEventCollector()
  const { events } = collector
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(child, [message(0, parent, 'work'), done(1, 'First reply')])
    handle = watchBrain(f.root, child, 'c', collector.emit, 5, () => true, {
      afterStep: 1,
      turnSeq: 3,
    })
    await handle.ready
    assert.equal(
      events.length,
      0,
      'history up to the resume point is not replayed',
    )
    await appendFile(
      f.path(child),
      encode([message(2, parent, 'more'), done(3, 'Second reply')]),
    )
    await collector.wait(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    )
    assert.deepEqual(turnAndReplySummary(events), [
      'c:turn:4:started',
      'Second reply',
      'c:turn:4:completed',
    ])
    assert.equal(handle.isStopped(), false)
    await handle.cancel()
    assert.equal(handle.isStopped(), true)
  } finally {
    handle?.()
    await f.close()
  }
})

async function assertWatcherSurvivesEmptyRewriteWindow(emptyFlushes: number) {
  const f = await fixture()
  const events: ProviderEvent[] = []
  let handle: BrainWatchHandle | undefined
  try {
    const rows = [message(0, parent, 'work'), done(1, 'Reply')]
    await f.put(child, rows)
    handle = watchBrain(
      f.root,
      child,
      'c',
      (event) => events.push(event),
      60_000,
    )
    await handle.ready
    const before = events.length
    await f.rewrite(
      child,
      [...rows, { step_index: 2, type: 'CHECKPOINT' }],
      async () => {
        for (let i = 0; i < emptyFlushes; i++) await handle!.flush()
      },
    )
    await handle.flush()
    assert.equal(events.length, before, 'unchanged steps are not new activity')
    await appendFile(
      f.path(child),
      encode([message(3, parent, 'New work'), done(4, 'New reply')]),
    )
    await handle.flush()
    assert.deepEqual(assistantTextsFromEvents(events), ['Reply', 'New reply'])
    const beforeCancel = events.length
    // Cancel drains a rewrite while preserving history identity, then stops future polling.
    await f.rewrite(
      child,
      [
        ...rows,
        { step_index: 2, type: 'CHECKPOINT' },
        message(3, parent, 'New work'),
        done(4, 'New reply'),
      ],
      () => handle!.flush(),
    )
    await handle.cancel()
    const canceled = events.length
    assert.equal(
      canceled,
      beforeCancel,
      'cancel must not re-emit rewritten history',
    )
    assert.equal(handle.isStopped(), true)
    await appendFile(
      f.path(child),
      encode([message(5, parent, 'After cancel'), done(6, 'Invisible')]),
    )
    await handle.cancel()
    assert.equal(events.length, canceled)
  } finally {
    handle?.()
    await f.close()
  }
}

test(
  'a child watcher does not restart turns across a single observed empty rewrite window',
  { timeout: 5000 },
  () => assertWatcherSurvivesEmptyRewriteWindow(1),
)

test(
  'a child watcher does not restart turns across repeated observed empty rewrite windows',
  { timeout: 5000 },
  () => assertWatcherSurvivesEmptyRewriteWindow(3),
)

test(
  'canceling an active child during an observed empty rewrite emits one canceled terminal',
  { timeout: 5000 },
  async () => {
    const f = await fixture()
    const events: ProviderEvent[] = []
    let handle: BrainWatchHandle | undefined
    try {
      const rows = [
        message(0, parent, 'work'),
        {
          step_index: 1,
          type: 'PLANNER_RESPONSE',
          status: 'RUNNING',
          content: 'Still running',
        },
      ]
      await f.put(child, rows)
      handle = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
      await handle.ready
      assert.equal(handle.isRunning(), true)
      await f.rewrite(child, rows, () => handle!.cancel())
      assert.equal(handle.isStopped(), true)
      assert.deepEqual(turnStates(events), ['started', 'canceled'])
      const count = events.length
      await handle.cancel()
      assert.equal(
        events.length,
        count,
        'a stopped watcher cannot replay restored rows',
      )
    } finally {
      handle?.()
      await f.close()
    }
  },
)

test(
  'cancel drains a complete rewritten prefix once even when the next record is partial',
  { timeout: 5000 },
  async () => {
    const f = await fixture()
    const events: ProviderEvent[] = []
    let handle: BrainWatchHandle | undefined
    const pollEvents = new EventEmitter()
    let polls = 0
    try {
      const rows = [
        { step_index: 0, type: 'USER_INPUT', content: 'work' },
        {
          step_index: 1,
          type: 'PLANNER_RESPONSE',
          status: 'RUNNING',
          content: 'old',
        },
      ]
      await f.put(child, rows)
      handle = watchBrain(
        f.root,
        child,
        'c',
        (e) => events.push(e),
        5,
        undefined,
        undefined,
        () => {
          polls++
          pollEvents.emit(String(polls))
        },
      )
      await handle.ready
      const file = await open(f.path(child), 'r+')
      try {
        await file.truncate(0)
        const target = polls + 2
        await once(pollEvents, String(target))
        await file.write(
          encode([
            ...rows,
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'RUNNING',
              content: 'NEW COMPLETE ROW',
            },
          ]) + '{"step_index":3',
          0,
        )
        await handle.cancel()
      } finally {
        await file.close()
      }
      assert.deepEqual(assistantTextsFromEvents(events), [
        'old',
        'NEW COMPLETE ROW',
      ])
      assert.deepEqual(turnStates(events), ['started', 'canceled'])
    } finally {
      handle?.()
      await f.close()
    }
  },
)

test('a rewrite observed mid-O_TRUNC does not end an anchored turn', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent)
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    const rows = [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ]
    await f.put(parent, rows)
    const stepEvents = new EventEmitter()
    const step2Seen = once(stepEvents, '2')
    let settled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs,
        onStep: (step) => {
          stepEvents.emit(String(step))
        },
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await step2Seen
    await flushObs(obs)
    await f.rewrite(parent, [...rows, done(3, 'ok')], async () => {
      await flushObs(obs)
      assert.equal(settled, false, 'an empty file mid-rewrite is not "no turn"')
    })
    await flushObs(obs)
    assert.equal((await running).status, 'completed')
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

const stoppedNotice = (step: number) =>
  message(
    step,
    'system',
    '[Notice] All your subagents and background tasks have been stopped',
  )
const toolCall = (step: number, id: string): Row => ({
  step_index: step,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  tool_calls: [{ id, name: 'run_command', args: {} }],
})

function assertStoppedCallsDropped(interruption: Row[]) {
  const projector = new BrainTranscriptProjector('c')
  const items = [
    message(0, parent, 'work'),
    toolCall(1, 'stopped'),
    ...interruption,
    message(3, parent, 'continue'),
    toolCall(4, 'fresh'),
    { step_index: 5, type: 'GENERIC', status: 'DONE', content: 'out' },
  ].flatMap((row) => projector.project(row))
  assert.deepEqual(
    items
      .filter(
        (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
          i.type === 'tool_call' && i.status === 'completed',
      )
      .map((i) => i.callId),
    ['fresh'],
    'the result belongs to the current response',
  )
  assert.equal(projector.hasPending(), false)
}

test('stopped calls never linger: restart notices and new responses drop them', () => {
  assertStoppedCallsDropped([stoppedNotice(2)])
  assertStoppedCallsDropped([])
})

test('a child woken after an interrupted tool call completes its new turn', async () => {
  const f = await fixture()
  const collector = createEventCollector()
  const { events } = collector
  let handle: BrainWatchHandle | undefined
  try {
    // Child transcripts have no USER_INPUT: a wakeup is a SYSTEM_MESSAGE.
    await f.put(child, [
      message(0, parent, 'work'),
      toolCall(1, 'stopped'),
      stoppedNotice(2),
      message(3, parent, 'continue'),
      toolCall(4, 'fresh'),
      { step_index: 5, type: 'GENERIC', status: 'DONE', content: 'out' },
      done(6, 'Finished'),
    ])
    handle = watchBrain(f.root, child, 'c', collector.emit, 5)
    await collector.wait(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    )
    assert.deepEqual(assistantTextsFromEvents(events), ['Finished'])
    const finalByCallId = new Map(
      toolCalls(events).map((call) => [call.callId, call]),
    )
    const freshCall = finalByCallId.get('fresh')
    const stoppedCall = finalByCallId.get('stopped')
    assert.ok(
      freshCall?.status === 'completed' &&
        freshCall.detail.type === 'unknown' &&
        freshCall.detail.output === 'out',
    )
    assert.ok(
      stoppedCall?.detail.type === 'unknown' &&
        stoppedCall.detail.output === null,
      'interrupted tool must not steal the post-wakeup tool result',
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('resuming after a drained cancel neither repeats nor skips child rows', async () => {
  const f = await fixture()
  const first: ProviderEvent[] = []
  const secondCollector = createEventCollector()
  const second = secondCollector.events
  let resumed: BrainWatchHandle | undefined
  try {
    await f.put(child, [message(0, parent, 'work')])
    const old = watchBrain(f.root, child, 'c', (e) => first.push(e), 5)
    await old.ready
    // Written before the cancel request but not yet polled.
    await appendFile(f.path(child), encode([done(1, 'Before cancel')]))
    const canceling = old.cancel()
    assert.equal(old.isStopping(), true, 'canceling already counts as stopping')
    await canceling
    resumed = watchBrain(
      f.root,
      child,
      'c',
      secondCollector.emit,
      5,
      () => true,
      old.resumePoint(),
    )
    await resumed.ready
    await appendFile(
      f.path(child),
      encode([message(2, parent, 'again'), done(3, 'After resume')]),
    )
    await secondCollector.wait(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    )
    assert.deepEqual(assistantTextsFromEvents(first), ['Before cancel'])
    assert.deepEqual(assistantTextsFromEvents(second), ['After resume'])
  } finally {
    resumed?.()
    await f.close()
  }
})

test('daemon schedule does not block foreground settlement or trigger interrupt relaunch, keeps includeDaemons true, and clears on MaxIterations or manage_task kill', async () => {
  const f = await fixture()
  try {
    const daemonId = `${parent}/task-2`
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'start daemon cron' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'cron-1',
            name: 'schedule',
            args: {
              CronExpression: '*/5 * * * *',
              Prompt: 'check status',
              IsDaemon: true,
              MaxIterations: 2,
            },
          },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${daemonId}\n`,
      },
      done(3, 'Standing cron scheduled.'),
    ])
    const settled = await settleBrainTurn(f.root, parent, 'p', () => {}, {
      minStepIndex: -1,
    })
    assert.equal(settled.status, 'completed')
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      false,
      'foreground interrupt check excludes standing daemons so they are not killed',
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      true,
      'idle runtime check includes standing daemons',
    )

    // First cron trigger decrements remaining iterations (2 -> 1), still active
    await appendFile(
      f.path(parent),
      encode([message(4, daemonId, 'check status'), done(5, 'Checked #1.')]),
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      true,
    )

    // Second cron trigger reaches MaxIterations (1 -> 0) and clears the daemon automatically
    await appendFile(
      f.path(parent),
      encode([message(6, daemonId, 'check status'), done(7, 'Checked #2.')]),
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      false,
    )

    // Unlimited daemon clears on explicit manage_task kill
    const daemon2Id = `${parent}/task-9`
    await appendFile(
      f.path(parent),
      encode([
        { step_index: 8, type: 'USER_INPUT', content: 'start unlimited cron' },
        {
          step_index: 9,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'cron-2',
              name: 'schedule',
              args: {
                CronExpression: '*/5 * * * *',
                Prompt: 'check forever',
                IsDaemon: true,
              },
            },
          ],
        },
        {
          step_index: 10,
          type: 'GENERIC',
          status: 'RUNNING',
          content: `Tool is running as a background task with task id: ${daemon2Id}\n`,
        },
        done(11, 'Unlimited cron started.'),
      ]),
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      true,
    )
    await appendFile(
      f.path(parent),
      encode([
        { step_index: 12, type: 'USER_INPUT', content: 'stop daemon cron' },
        {
          step_index: 13,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'kill-1',
              name: 'manage_task',
              args: { Action: 'kill', TaskId: daemon2Id },
            },
          ],
        },
        {
          step_index: 14,
          type: 'GENERIC',
          status: 'DONE',
          content: `Killed task ${daemon2Id}`,
        },
        done(15, 'Stopped standing cron.'),
      ]),
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      false,
    )
  } finally {
    await f.close()
  }
})

const loadWokenQuestionFixture = async (): Promise<Row[]> =>
  (
    await readFile(
      new URL('./fixtures/woken-question-transcript.jsonl', import.meta.url),
      'utf8',
    )
  )
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as Row)

test('a woken native question is not interrupt-relaunch work but blocks settlement until agy replies', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const items: ProviderTimelineItem[] = []
  const obs = manualObs(f.root, parent, -1, (item) => items.push(item))
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    const rows = await loadWokenQuestionFixture()
    await f.put(parent, rows.slice(0, 6))
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
    const checks = new EventEmitter()
    const questionChecked = once(checks, 'question')
    let answer: { answerText: string; declined: boolean } | undefined
    let isSettled = false
    running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        signal: abort.signal,
        observation: obs,
        takeQuestionAnswer: () => {
          checks.emit('question')
          return answer
        },
      }),
    ).then((res) => {
      isSettled = true
      return res
    })
    await questionChecked
    assert.equal(isSettled, false)
    const matched = once(checks, 'question')
    answer = { answerText: '(Recommended) Option 1', declined: false }
    await flushObs(obs)
    await matched
    assert.equal(isSettled, false)
    await appendFile(
      f.path(parent),
      encode([rows[6], done(7, 'Done: (Recommended) Option 1')]),
    )
    await flushObs(obs)
    const settled = await running
    running = undefined
    assert.equal(settled.status, 'completed')
    assert.equal(
      items.some((item) => item.type === 'assistant_message'),
      false,
    )
  } finally {
    abort.abort()
    await obs.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test(
  'a rewritten transcript cannot give a replacement question an anonymous slot’s cached answer',
  { timeout: 5000 },
  async (t) => {
    const f = await fixture()
    t.after(f.close)
    const tracker = createStepTracker()
    const obs = manualObs(f.root, parent)
    const abort = new AbortController()
    t.after(() => {
      abort.abort()
      return obs.dispose()
    })
    const ask = (question: string, id?: string): Row => ({
      ...done(1, ''),
      tool_calls: [
        {
          id,
          name: 'ask_question',
          args: { questions: [{ question, options: ['A', 'B'] }] },
        },
      ],
    })
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'work' },
      ask('Original question?'),
      { step_index: 2, type: 'GENERIC', status: 'DONE', content: 'A' },
      { step_index: 3, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    const items: ProviderTimelineItem[] = []
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        observation: obs,
        onStep: tracker.onStep,
        takeQuestionAnswer: sequenceDouble([
          () => ({ answerText: 'original answer', declined: false }),
          () => ({ answerText: 'replacement answer', declined: false }),
        ]),
      }),
    )
    await tracker.waitFor(3)
    await writeFileAtomic(
      f.path(parent),
      encode([
        { step_index: 0, type: 'USER_INPUT', content: 'work' },
        ask('Replacement question?', 'replacement-call'),
      ]),
    )
    await flushObs(obs)
    assert.equal((await running).status, 'completed')
    const replacement = items.find(
      (item) => item.type === 'tool_call' && item.callId === 'replacement-call',
    )
    assert.ok(
      replacement?.type === 'tool_call' &&
        replacement.detail.type === 'plain_text',
    )
    assert.match(replacement.detail.text ?? '', /Answer:\nreplacement answer$/)
  },
)

test('ask_question results stay attributed by step next to sibling tool calls', () => {
  // Mixed [ask_question, run_command] in one PLANNER_RESPONSE preserves FIFO/step attribution
  const p1 = new BrainTranscriptProjector('s')
  const mixedLogged = [
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'ask',
          name: 'ask_question',
          args: { questions: [{ question: 'Choose', options: ['A', 'B'] }] },
        },
        {
          id: 'cmd',
          name: 'run_command',
          args: { CommandLine: 'echo hi' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'DONE',
      content: 'User chose A',
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'hi' },
  ].flatMap((row) => p1.project(row))
  const completedMixed = mixedLogged.filter(
    (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
      i.type === 'tool_call' && i.status === 'completed',
  )
  assert.equal(completedMixed.length, 2)
  assert.equal(completedMixed[0].callId, 'ask')
  assert.ok(
    completedMixed[0].detail.type === 'plain_text' &&
      completedMixed[0].detail.text?.includes('Answer:\nUser chose A'),
  )
  assert.equal(completedMixed[1].callId, 'cmd')
  assert.ok(
    completedMixed[1].detail.type === 'shell' &&
      completedMixed[1].detail.output === 'hi',
  )

  // When leading ask_question stays unlogged in WAITING (step 2), GENERIC(step 3) still attributes to cmd (step 3)
  const p2 = new BrainTranscriptProjector('s')
  const mixedUnlogged = [
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'ask',
          name: 'ask_question',
          args: { questions: [{ question: 'Choose', options: ['A', 'B'] }] },
        },
        {
          id: 'cmd',
          name: 'run_command',
          args: { CommandLine: 'echo hi' },
        },
      ],
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'hi' },
  ].flatMap((row) => p2.project(row))
  const completedUnlogged = mixedUnlogged.filter(
    (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
      i.type === 'tool_call' && i.status === 'completed',
  )
  assert.equal(completedUnlogged.length, 1)
  assert.equal(completedUnlogged[0].callId, 'cmd')
  assert.equal(p2.hasPending(), false)
  assert.equal(p2.consumePendingQuestion(() => true)?.tool.callId, 'ask')

  // A CHECKPOINT between the response and its GENERIC rows shifts the expected step of later calls.
  const p3 = new BrainTranscriptProjector('s')
  const checkpointed = [
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'ask',
          name: 'ask_question',
          args: { questions: [{ question: 'Choose', options: ['A', 'B'] }] },
        },
        { id: 'cmd', name: 'run_command', args: { CommandLine: 'echo hi' } },
      ],
    },
    { step_index: 2, type: 'CHECKPOINT', status: 'DONE' },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'User chose A' },
    { step_index: 4, type: 'GENERIC', status: 'DONE', content: 'hi' },
  ].flatMap((row) => p3.project(row))
  assert.deepEqual(completedToolOutputs(checkpointed), [
    ['ask', 'Choose\n  1. A\n  2. B\n\nAnswer:\nUser chose A'],
    ['cmd', 'hi'],
  ])
})

test('countRowMessages aligns with inbox wakeup counts across restart notices, low priority, and batched step 0', () => {
  const notice =
    '[Notice] All your subagents and background tasks have been stopped due to server restart.'
  assert.equal(countRowMessages(message(3, 'system', notice)), 0)
  assert.equal(
    countRowMessages({
      step_index: 4,
      type: 'SYSTEM_MESSAGE',
      content:
        '<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-08T17:00:00Z sender=bg-1 priority=MESSAGE_PRIORITY_LOW content=quiet\n</SYSTEM_MESSAGE>',
    }),
    0,
  )
  assert.equal(
    countRowMessages({
      step_index: 0,
      type: 'SYSTEM_MESSAGE',
      content: `<SYSTEM_MESSAGE>\n1. [Message] timestamp=2026-11-08T17:00:00Z sender=${parent} priority=MESSAGE_PRIORITY_HIGH content=init\n2. [Message] timestamp=2026-11-08T17:00:01Z sender=${parent} priority=MESSAGE_PRIORITY_HIGH content=followup\n</SYSTEM_MESSAGE>`,
    }),
    1,
  )
})

test('quoted sender text does not cancel conditional schedule timers while header matches emit canceled item', () => {
  const projector = new BrainTranscriptProjector(parent)
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      {
        id: 'sched-1',
        name: 'schedule',
        args: { DurationSeconds: 60, Prompt: 'wake', TimerCondition: 'any' },
      },
    ],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: 'Tool is running as a background task with task id: timer-1',
  })
  assert.equal(projector.hasBackground(), true)

  // A USER_INPUT or unanchored body text mentioning sender= must not cancel timer-1.
  const quotedItems = projector.project({
    step_index: 3,
    type: 'SYSTEM_MESSAGE',
    content:
      '<SYSTEM_MESSAGE>\nBody mentions sender=fake-agent inline without header\n</SYSTEM_MESSAGE>',
  })
  assert.deepEqual(quotedItems, [])
  assert.equal(projector.hasBackground(), true)

  // A genuine [Message] header cancels the conditional timer and emits a canceled tool_call update.
  const canceledItems = projector.project(message(4, child, 'Subagent report'))
  assert.equal(projector.hasBackground(), false)
  assert.equal(canceledItems.length, 1)
  assert.ok(
    canceledItems[0]?.type === 'tool_call' &&
      canceledItems[0].callId === 'sched-1' &&
      canceledItems[0].status === 'canceled',
  )
})

test('readInboxState and readSubagents reject symlinked leaf files', async () => {
  const f = await fixture()
  try {
    const outside = join(f.root, 'outside.json')
    await writeFile(outside, JSON.stringify({ id: 'escaped', sender: child }))
    const msgDir = join(f.root, parent, '.system_generated/messages')
    await mkdir(msgDir, { recursive: true })
    await symlink(outside, join(msgDir, 'escaped.json'))
    await assert.rejects(
      readInboxState(f.root, parent),
      /Brain artifact escapes its directory/,
    )

    const subDir = join(f.root, child, '.system_generated/subagents')
    await mkdir(subDir, { recursive: true })
    await symlink(outside, join(subDir, `${grandchild}.json`))
    await assert.rejects(
      readSubagents(f.root, child),
      /Brain artifact escapes its directory/,
    )
  } finally {
    await f.close()
  }
})

test('foreground turn aborted by user permission denial settles immediately without stalling', async (t) => {
  const f = await fixture()
  const items: ProviderTimelineItem[] = []
  const obs1 = manualObs(f.root, parent, -1, (item) => items.push(item))
  const spoofed = parent.replace(/^1/, '3')
  const unlogged = parent.replace(/^1/, '2')
  const obs2 = manualObs(f.root, spoofed)
  const obs3 = manualObs(f.root, unlogged)
  const abort = new AbortController()
  let spoofRunning:
    Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', status: 'DONE', content: 'echo 1' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'call_denied_0',
            name: 'run_command',
            args: { CommandLine: 'echo 1' },
          },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'ERROR',
        content:
          'Encountered error in step execution: permission check failed for unsandboxed "echo 1": user denied permission to execute',
      },
    ])
    // Unstarted observation polls only once, proving immediate completion without waiting for stallTimeoutMs.
    const result = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => items.push(item),
      {
        observation: obs1,
        stallTimeoutMs: 5000,
        acpToolCallIds: ['call_denied_0'],
        deniedToolCallIds: new Set(['call_denied_0']),
      },
    )
    assert.equal(result.status, 'completed')

    // Untrusted tool error output containing permission-denial wording without a recorded denial must not settle early.
    await f.put(spoofed, [
      { step_index: 0, type: 'USER_INPUT', status: 'DONE', content: 'grep' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ name: 'run_command', args: { CommandLine: 'false' } }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'ERROR',
        content:
          'Command exited with code 1.\npermission check failed: user denied permission',
      },
    ])
    const spoofSteps = new EventEmitter()
    const spoofStep2 = once(spoofSteps, '2')
    spoofRunning = alive(
      settleBrainTurn(f.root, spoofed, 'p', () => {}, {
        signal: abort.signal,
        observation: obs2,
        stallTimeoutMs: 60,
        acpToolCallIds: ['call_not_denied'],
        deniedToolCallIds: new Set(),
        onStep: (step) => {
          spoofSteps.emit(String(step))
        },
      }),
    )
    await spoofStep2
    t.mock.timers.tick(60)
    await flushObs(obs2)
    const spoofResult = await spoofRunning
    spoofRunning = undefined
    assert.equal(spoofResult.status, 'failed')

    // A denied call does not need a GENERIC row to settle.
    await f.put(unlogged, [
      { step_index: 0, type: 'USER_INPUT', status: 'DONE', content: 'echo 2' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'call_denied_1',
            name: 'run_command',
            args: { CommandLine: 'echo 2' },
          },
        ],
      },
    ])
    const result2 = await settleBrainTurn(f.root, unlogged, 'p', () => {}, {
      observation: obs3,
      stallTimeoutMs: 5000,
      acpToolCallIds: ['call_denied_1'],
      deniedToolCallIds: new Set(['call_denied_1']),
    })
    assert.equal(result2.status, 'completed')
  } finally {
    abort.abort()
    await obs1.dispose()
    await obs2.dispose()
    await obs3.dispose()
    await spoofRunning?.catch(() => undefined)
    await f.close()
  }
})

const interrupted = (step_index: number, created_at?: string): Row => ({
  step_index,
  type: 'ERROR_MESSAGE',
  status: 'DONE',
  content:
    'Error: The stream was interrupted. Please continue the task you were working on.',
  ...(created_at ? { created_at } : {}),
})

test('stream retries preserve tool correlation and do not project historical errors', () => {
  const projector = new BrainTranscriptProjector('retry')
  const [started] = projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id: 'tool', name: 'run_command', args: {} }],
  })
  assert.deepEqual(projector.project(interrupted(2)), [])
  assert.equal(projector.hasPending(), true)
  const [finished] = projector.project({
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: 'ok',
  })
  assert.equal(finished.id, started.id)
  assert.ok(finished.type === 'tool_call' && finished.status === 'completed')
  assert.equal(
    projector.project({
      step_index: 4,
      type: 'ERROR_MESSAGE',
      content: 'disk failure',
    })[0].type,
    'error',
  )
})

test('API/stream retries recover after 4s and multiple interruptions; unfinished response stays pending', async (t) => {
  const f = await fixture()
  const items: ProviderTimelineItem[] = []
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'work' },
      {
        step_index: 2,
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        source: 'SYSTEM',
        error: 'API error (attempt 1): request failed: synthetic timeout',
      },
    ])
    const stepEvents = new EventEmitter()
    const onStep = (step: number) => {
      stepEvents.emit(String(step))
    }
    const step2Seen = once(stepEvents, '2')
    const step4Seen = once(stepEvents, '4')
    const obs = new BrainTurnObservation(
      f.root,
      parent,
      'p',
      -1,
      {
        isCurrent: () => true,
        onUsage: () => {},
        onItem: (item) => items.push(item),
      },
      60_000,
    )
    let finished = false
    const pending = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        observation: obs,
        stallTimeoutMs: 50,
        onStep,
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await step2Seen
    t.mock.timers.tick(4_000)
    await flushObs(obs)
    await appendFile(
      f.path(parent),
      encode([
        interrupted(3),
        {
          step_index: 4,
          type: 'PLANNER_RESPONSE',
          status: 'RUNNING',
          thinking: 'still thinking',
        },
      ]),
    )
    await flushObs(obs)
    await step4Seen
    assert.equal(finished, false)
    await appendFile(f.path(parent), encode([done(5)]))
    await flushObs(obs)
    assert.equal((await pending).status, 'completed')
    assert.equal(
      items.some((item) => item.type === 'error'),
      false,
    )
  } finally {
    await f.close()
  }
})

test('root retry exhaustion is bounded even with retained background work; cancellation wins', async (t) => {
  const f = await fixture()
  const abort = new AbortController()
  const obs1 = manualObs(f.root, parent)
  const obs2 = manualObs(f.root, parent)
  let running: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    const rows: Row[] = [
      { step_index: 1, type: 'USER_INPUT', content: 'work' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        tool_calls: [{ name: 'run_command', args: {} }],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: task',
      },
      interrupted(4),
    ]
    await f.put(parent, rows)
    const steps = createStepTracker()
    const step4Seen = steps.waitFor(4)
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs1,
        stallTimeoutMs: 30,
        retryTimeoutMs: 30,
        onStep: steps.onStep,
      }),
    )
    await step4Seen
    t.mock.timers.tick(30)
    await flushObs(obs1)
    const result = await running
    running = undefined
    assert.equal(result.status, 'failed')
    assert.match(
      result.error!,
      /stream was interrupted.*No retry recovery was observed/,
    )
    assert.equal(result.stalled, true)
    const controller = new AbortController()
    const pending = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: controller.signal,
        observation: obs2,
      }),
    )
    controller.abort()
    assert.equal((await pending).status, 'canceled')
  } finally {
    abort.abort()
    await obs1.dispose()
    await obs2.dispose()
    await running?.catch(() => undefined)
    await f.close()
  }
})

test('child retry recovers and delivers its report before parent completion', async () => {
  const f = await fixture()
  const abort = new AbortController()
  const obs = manualObs(f.root, parent)
  let pending: Promise<Awaited<ReturnType<typeof settleBrainTurn>>> | undefined
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.put(child, [interrupted(1)])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)
    const stepEvents = new EventEmitter()
    const step4Seen = once(stepEvents, '4')
    let finished = false
    pending = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        observation: obs,
        stallTimeoutMs: 1000,
        onStep: (step) => {
          stepEvents.emit(String(step))
        },
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await step4Seen
    await flushObs(obs)
    assert.equal(finished, false)
    const msgId = await f.deliver(parent, child, 'report', false)
    await appendFile(f.path(child), encode(report(parent, 'report', 2)))
    await f.deliver(parent, child, 'report', true, msgId)
    await appendFile(
      f.path(parent),
      encode([message(5, child, 'report'), done(6)]),
    )
    await flushObs(obs)
    const settled = await pending
    pending = undefined
    assert.equal(settled.status, 'completed')
    assert.equal(settled.wokeUp, true)
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
    abort.abort()
    await obs.dispose()
    await pending?.catch(() => undefined)
    await f.close()
  }
})

test('expired child retry fails parent and active-work checks, recovered history does not', async () => {
  const f = await fixture()
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.put(child, [interrupted(1, '2020-01-01T00:00:00Z')])
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, { pollIntervalMs: 5 }),
    )
    assert.equal(result.status, 'failed')
    assert.match(result.error!, /Subagent .*No retry recovery was observed/)
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
    await appendFile(f.path(child), encode([done(2)]))
    assert.equal(
      (
        await alive(
          settleBrainTurn(f.root, parent, 'p', () => {}, { pollIntervalMs: 5 }),
        )
      ).status,
      'completed',
    )
  } finally {
    await f.close()
  }
})

async function startRetryWatcher(
  t: TestContext,
  f: Awaited<ReturnType<typeof fixture>>,
) {
  await f.put(child, [interrupted(1)])
  const events: ProviderEvent[] = []
  const handle = watchBrain(
    f.root,
    child,
    'c',
    (event) => events.push(event),
    60_000,
    undefined,
    undefined,
    undefined,
    80,
  )
  t.after(() => handle())
  await handle.ready
  assert.equal(handle.isRunning(), true)
  return { events, handle }
}

test('watcher retries recover without failed events, while cancellation and exhaustion stay distinct', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })

  const recovered = await startRetryWatcher(t, f)
  await appendFile(
    f.path(child),
    encode([
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'RUNNING',
        thinking: 'retry',
      },
    ]),
  )
  t.mock.timers.tick(15)
  await recovered.handle.flush()
  assert.equal(recovered.handle.isRunning(), true)
  await appendFile(f.path(child), encode([done(3)]))
  await recovered.handle.flush()
  assert.ok(
    recovered.events.some(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    ),
  )
  assert.equal(
    recovered.events.some(
      (e) => e.type === 'timeline.item' && e.item.type === 'error',
    ),
    false,
  )
  recovered.handle()

  const canceled = await startRetryWatcher(t, f)
  await canceled.handle.cancel()
  assert.ok(
    canceled.events.some(
      (e) => e.type === 'session.turn' && e.state === 'canceled',
    ),
  )
  assert.equal(
    canceled.events.some(
      (e) => e.type === 'timeline.item' && e.item.type === 'error',
    ),
    false,
  )
  canceled.handle()

  const exhausted = await startRetryWatcher(t, f)
  t.mock.timers.tick(80)
  await exhausted.handle.flush()
  assert.ok(
    exhausted.events.some(
      (e) => e.type === 'session.turn' && e.state === 'failed',
    ),
  )
  assert.equal(
    exhausted.events.some(
      (e) => e.type === 'timeline.item' && e.item.type === 'error',
    ),
    false,
  )
  exhausted.handle()
})

test('a real failure after a stream warning remains a failure, recovered replay remains completed', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'work' },
      interrupted(2),
      {
        step_index: 3,
        type: 'ERROR_MESSAGE',
        content: 'Model request rejected',
      },
    ])
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item)),
    )
    assert.equal(result.status, 'failed')
    assert.equal(result.error, 'Model request rejected')
    assert.equal(items.filter((item) => item.type === 'error').length, 1)
    await f.put(child, [interrupted(1, '2020-01-01T00:00:00Z'), done(2)])
    const events: ProviderEvent[] = []
    handle = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await handle.ready
    assert.deepEqual(
      events.filter((e) => e.type === 'session.turn').map((e) => e.state),
      ['started', 'completed'],
    )
    assert.equal(
      events.some((e) => e.type === 'timeline.item' && e.item.type === 'error'),
      false,
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('retry warnings retain background and subagent associations', () => {
  const projector = new BrainTranscriptProjector('retry')
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: 'invoke_subagent', args: {} }],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'DONE',
    content: JSON.stringify({ conversationId: child }),
  })
  projector.project({
    step_index: 3,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: 'run_command', args: {} }],
  })
  projector.project({
    step_index: 4,
    type: 'GENERIC',
    status: 'RUNNING',
    content: 'Tool is running as a background task with task id: task',
  })
  projector.project(interrupted(5))
  assert.equal(projector.hasBackground(), true)
  assert.equal(projector.touchedSubagentIds().has(child), true)
  projector.project({
    step_index: 6,
    type: 'ERROR_MESSAGE',
    content: 'Real failure',
  })
  assert.equal(projector.hasBackground(), false)
  assert.equal(projector.touchedSubagentIds().size, 0)
})

test('a missing transcript during retry cannot report success', async (t) => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(child, [interrupted(1)])
    const events: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'c',
      (event) => events.push(event),
      60_000,
      () => false,
      undefined,
      undefined,
      50,
    )
    await handle.ready
    assert.equal(handle.isRunning(), true)
    await rm(f.path(child))
    await handle.flush()
    assert.equal(handle.isRunning(), true)
    t.mock.timers.tick(50)
    await handle.flush()
    assert.ok(
      events.some((e) => e.type === 'session.turn' && e.state === 'failed'),
    )
    assert.equal(
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
      false,
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('a busy sibling cannot hide another child retry timeout', async () => {
  const f = await fixture()
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.subagent(parent, grandchild)
    await f.put(child, [
      { step_index: 1, type: 'PLANNER_RESPONSE', thinking: 'busy' },
    ])
    await f.put(grandchild, [interrupted(1, '2020-01-01T00:00:00Z')])
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, { pollIntervalMs: 5 }),
    )
    assert.equal(result.status, 'failed')
    assert.equal(result.stalled, true)
    assert.match(result.error!, new RegExp(grandchild))
  } finally {
    await f.close()
  }
})

test('cancellation observed while ingesting wins over a terminal failure', async () => {
  const f = await fixture()
  const controller = new AbortController()
  try {
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'work' },
      interrupted(2),
      { step_index: 3, type: 'ERROR_MESSAGE', content: 'final failure' },
    ])
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: controller.signal,
        onStep: () => controller.abort(),
      }),
    )
    assert.equal(result.status, 'canceled')
  } finally {
    await f.close()
  }
})

test('quiet DONE completes root and child turns instead of stalling them', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const quiet = {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      thinking: 'No further reply',
    }
    await f.put(parent, [{ step_index: 1, type: 'USER_INPUT' }, quiet])
    assert.equal(
      (
        await alive(
          settleBrainTurn(f.root, parent, 'p', () => {}, {
            stallTimeoutMs: 50,
            pollIntervalMs: 5,
          }),
        )
      ).status,
      'completed',
    )
    await f.put(child, [quiet])
    const events: ProviderEvent[] = []
    handle = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await handle.ready
    assert.equal(handle.isRunning(), false)
    assert.ok(
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      ),
    )
    await f.subagent(parent, child)
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
    handle?.()
    await f.close()
  }
})

test('root retry wait uses its own bounded policy and can recover with a quiet DONE', async (t) => {
  const f = await fixture()
  const controller = new AbortController()
  const obs = manualObs(f.root, parent)
  let pending: ReturnType<typeof settleBrainTurn> | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT' },
      {
        step_index: 2,
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        source: 'SYSTEM',
        error: 'API error (attempt 1): request failed: synthetic timeout',
      },
    ])
    const stepEvents = new EventEmitter()
    const step2Seen = once(stepEvents, '2')
    let finished = false
    pending = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: controller.signal,
        observation: obs,
        stallTimeoutMs: 5,
        retryTimeoutMs: 60_000,
        onStep: (step) => {
          stepEvents.emit(String(step))
        },
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await step2Seen
    t.mock.timers.tick(25)
    await flushObs(obs)
    assert.equal(
      finished,
      false,
      'the ordinary stall must not cut short a retry',
    )
    await appendFile(
      f.path(parent),
      encode([
        {
          step_index: 3,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          thinking: 'Quiet recovery',
        },
      ]),
    )
    await flushObs(obs)
    assert.equal((await pending).status, 'completed')
  } finally {
    controller.abort()
    await obs.dispose()
    await pending?.catch(() => undefined)
    await f.close()
  }
})

async function assertExpiredRetryRetainsTask(
  f: Awaited<ReturnType<typeof fixture>>,
  expired: Row,
  IsDaemon: boolean,
) {
  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ name: 'run_command', args: { IsDaemon } }],
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'RUNNING',
      content: 'Tool is running as a background task with task id: retained',
    },
    expired,
  ])
  assert.equal(await hasActiveBackgroundWork(f.root, parent), !IsDaemon)
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
    true,
  )
}

test('expired retries do not hide retained processes, daemon policy, unread messages or active descendants', async () => {
  const f = await fixture()
  try {
    const expired = interrupted(4, '2020-01-01T00:00:00Z')
    await assertExpiredRetryRetainsTask(f, expired, false)
    await assertExpiredRetryRetainsTask(f, expired, true)
    await f.put(parent, [{ step_index: 1, type: 'USER_INPUT' }, expired])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
    await f.deliver(parent, child, 'unread report', false)
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)
    await f.subagent(parent, child)
    await f.put(child, [
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'RUNNING',
        thinking: 'active child',
      },
    ])
    await f.deliver(parent, child, 'unread report', true)
    // Use a fresh root inbox to isolate live descendants from unread-message detection.
    await rm(join(f.root, parent, '.system_generated/messages'), {
      recursive: true,
      force: true,
    })
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)
  } finally {
    await f.close()
  }
})

test('child registry forwards the shared retry policy to its watcher', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: T0 })
  const f = await fixture()
  const events: ProviderEvent[] = []
  const registry = new SubagentRegistry({
    sessionId: 'p',
    cwd: f.root,
    brainRoot: f.root,
    capabilities: ['session.subsession'],
    emit: (event) => events.push(event),
    getNativeId: () => parent,
    isParentTurnActive: () => true,
    isLoading: () => false,
    isUnavailable: () => false,
    retryTimeoutMs: 50,
  })
  try {
    await f.put(child, [interrupted(1, new Date(T0 - 1000).toISOString())])
    registry.linkSubsession('p', parent, {
      type: 'tool_call',
      id: 'spawn',
      callId: 'spawn',
      name: 'invoke_subagent',
      status: 'completed',
      error: null,
      detail: { type: 'sub_agent', childSessionId: child, log: '' },
    })
    await registry.waitCurrentReady()
    assert.ok(
      events.some(
        (event) =>
          event.type === 'session.turn' &&
          event.sessionId === `p:child:${child}` &&
          event.state === 'failed',
      ),
    )
    assert.equal(registry.hasRunningChildren(), false)
  } finally {
    registry.closeAll()
    await f.close()
  }
})

test('SubagentRegistry resumeChild racing cancelRunning and cancelAll does not resurrect stopped watchers', async (t) => {
  const f = await fixture()
  const collector = createEventCollector()
  const registry = new SubagentRegistry({
    sessionId: 'p',
    cwd: f.root,
    brainRoot: f.root,
    capabilities: ['session.subsession'],
    emit: collector.emit,
    getNativeId: () => parent,
    isParentTurnActive: () => true,
    isLoading: () => false,
    isUnavailable: () => false,
  })
  t.after(async () => {
    registry.closeAll()
    await f.close()
  })
  await f.put(child, [
    message(0, parent, 'work'),
    { step_index: 1, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
  ])
  registry.linkSubsession('p', parent, {
    type: 'tool_call',
    id: 'spawn',
    callId: 'spawn',
    name: 'invoke_subagent',
    status: 'completed',
    error: null,
    detail: { type: 'sub_agent', childSessionId: child, log: '' },
  })
  await collector.wait(
    (event) =>
      event.type === 'session.turn' &&
      event.sessionId === `p:child:${child}` &&
      event.state === 'started',
  )

  await Promise.all(registry.cancelRunning())
  assert.equal(registry.hasRunningChildren(), false)
  assert.equal(registry.isChildResumable(child), true)

  registry.linkSubsession('p', parent, {
    type: 'tool_call',
    id: 'msg-1',
    callId: 'msg-1',
    name: 'send_message',
    status: 'completed',
    error: null,
    detail: { type: 'sub_agent', childSessionId: child, log: '' },
  })
  assert.equal(registry.hasRunningChildren(), true)
  await Promise.all(registry.cancelRunning())
  assert.equal(registry.hasRunningChildren(), false)
  assert.equal(registry.isChildResumable(child), true)

  registry.linkSubsession('p', parent, {
    type: 'tool_call',
    id: 'msg-2',
    callId: 'msg-2',
    name: 'send_message',
    status: 'completed',
    error: null,
    detail: { type: 'sub_agent', childSessionId: child, log: '' },
  })
  await registry.cancelAll()
  assert.equal(registry.hasRunningChildren(), false)

  await appendFile(f.path(child), encode([done(2, 'child finished')]))
  registry.linkSubsession('p', parent, {
    type: 'tool_call',
    id: 'msg-3',
    callId: 'msg-3',
    name: 'send_message',
    status: 'completed',
    error: null,
    detail: { type: 'sub_agent', childSessionId: child, log: '' },
  })
  await collector.wait(
    (event) =>
      event.type === 'session.turn' &&
      event.sessionId === `p:child:${child}` &&
      event.state === 'completed',
  )
  assert.equal(registry.hasRunningChildren(), false)
})

test('cancel wins after multiple exhausted warnings are ingested', async () => {
  const f = await fixture()
  const controller = new AbortController()
  try {
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT' },
      interrupted(2, '2020-01-01T00:00:00Z'),
      interrupted(3, '2020-01-01T00:00:01Z'),
    ])
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: controller.signal,
        onStep: sequenceDouble([() => {}, () => {}, () => controller.abort()]),
        retryTimeoutMs: 50,
      }),
    )
    assert.equal(result.status, 'canceled')
    assert.equal(result.error, undefined)
  } finally {
    await f.close()
  }
})

async function checkpointFixture(): Promise<Row> {
  return JSON.parse(
    await readFile(
      new URL('./fixtures/checkpoint.sanitized.jsonl', import.meta.url),
      'utf8',
    ),
  )
}

test('completed checkpoint projects preTokens without guessing trigger or post-compaction usage and keeps tool offsets', async () => {
  const checkpoint = await checkpointFixture()
  const projector = new BrainTranscriptProjector('p')
  projector.project({
    step_index: 92,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      { id: 'a', name: 'run_command', args: {} },
      { id: 'b', name: 'view_file', args: {} },
    ],
  })
  assert.deepEqual(projector.project(checkpoint), [
    {
      type: 'compaction',
      id: 'p:step:93:compaction',
      status: 'completed',
      preTokens: 268_740,
    },
  ])
  assert.deepEqual(
    new BrainTranscriptProjector('p').project({
      step_index: 99,
      type: 'CHECKPOINT',
      status: 'DONE',
      input_tokens: 0,
      cache_read_tokens: 0,
      output_tokens: 4_200,
      content: 'summary',
    }),
    [{ type: 'compaction', id: 'p:step:99:compaction', status: 'completed' }],
  )
  const a = projector.project({
    step_index: 94,
    type: 'GENERIC',
    status: 'DONE',
    content: 'first',
  })[0]
  const b = projector.project({
    step_index: 95,
    type: 'GENERIC',
    status: 'DONE',
    content: 'second',
  })[0]
  assert.ok(
    a.type === 'tool_call' && a.callId === 'a' && a.status === 'completed',
  )
  assert.ok(
    b.type === 'tool_call' && b.callId === 'b' && b.status === 'completed',
  )
  assert.deepEqual(
    new BrainTranscriptProjector('p').project({
      ...checkpoint,
      status: undefined,
    }),
    [],
  )
  assert.deepEqual(
    new BrainTranscriptProjector('p').project({
      ...checkpoint,
      status: 'RUNNING',
    }),
    [],
  )
  assert.deepEqual(
    new BrainTranscriptProjector('p').project({
      ...checkpoint,
      status: 'CANCELED',
    }),
    [],
  )
  assert.deepEqual(
    new BrainTranscriptProjector('p').project({
      ...checkpoint,
      status: 'ERROR',
    }),
    [],
  )
})

async function assertCheckpointExcludesModelUsage(
  extraRows: Row[],
  expectedWakeupInput?: number,
  expectedWakeupOutput?: number,
) {
  const f = await fixture()
  try {
    const checkpoint = { ...(await checkpointFixture()), step_index: 4 }
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'work' },
      { ...done(2, 'waiting'), input_tokens: 100, output_tokens: 5 },
      ...extraRows,
      checkpoint,
      { ...done(5), input_tokens: 10, output_tokens: 2 },
    ])
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        pollIntervalMs: 5,
      }),
    )
    assert.equal(result.status, 'completed')
    assert.deepEqual(
      items.filter((item) => item.type === 'compaction'),
      [
        {
          type: 'compaction',
          id: 'p:step:4:compaction',
          status: 'completed',
          preTokens: 268_740,
        },
      ],
    )
    assert.equal(result.lastStepUsage?.inputTokens, 10)
    assert.equal(result.lastStepUsage?.outputTokens, 2)
    assert.equal(result.wakeupUsage?.inputTokens, expectedWakeupInput)
    assert.equal(result.wakeupUsage?.outputTokens, expectedWakeupOutput)
  } finally {
    await f.close()
  }
}

test('foreground and wakeup project checkpoints without using checkpoint tokens as model usage', async () => {
  await assertCheckpointExcludesModelUsage([], undefined, undefined)
  await assertCheckpointExcludesModelUsage(
    [message(3, 'system', 'continue')],
    10,
    2,
  )
})

test('a command-only checkpoint is visible without inventing a model response or usage', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [await checkpointFixture()])
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: 92,
      }),
    )
    assert.equal(result.status, 'completed')
    assert.equal(result.lastStepUsage, undefined)
    assert.deepEqual(items, [
      {
        type: 'compaction',
        id: 'p:step:93:compaction',
        status: 'completed',
        preTokens: 268_740,
      },
    ])
    const old: ProviderTimelineItem[] = []
    await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => old.push(item), {
        minStepIndex: 93,
      }),
    )
    assert.deepEqual(old, [])
  } finally {
    await f.close()
  }
})

test('child compaction stays on child timeline, survives cancellation drain, and is deduplicated across an empty rewrite', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const checkpoint = { ...(await checkpointFixture()), step_index: 2 }
    const rows = [
      message(0, parent, 'work'),
      { step_index: 1, type: 'PLANNER_RESPONSE', thinking: 'working' },
      checkpoint,
    ]
    await f.put(child, rows)
    const events: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'child',
      (event) => events.push(event),
      5,
    )
    await handle.ready
    assert.equal(handle.isRunning(), true)
    const compactions = () => compactionEvents(events)
    assert.equal(compactions().length, 1)
    assert.equal(compactions()[0].sessionId, 'child')
    await f.rewrite(
      child,
      [...rows, { step_index: 3, type: 'CHECKPOINT', status: 'RUNNING' }],
      async () => {
        await handle!.flush()
      },
    )
    await handle.cancel()
    assert.equal(compactions().length, 1)
    assert.ok(
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'canceled',
      ),
    )
    assert.equal(
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      ),
      false,
    )
    assert.deepEqual(compactions()[0].item, {
      type: 'compaction',
      id: 'child:step:2:compaction',
      status: 'completed',
      preTokens: 268_740,
    })
  } finally {
    handle?.()
    await f.close()
  }
})

test('a late child checkpoint keeps the completed child terminal and retains native ownership', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(child, [message(0, parent, 'work'), done(1)])
    const events: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'child',
      (event) => events.push(event),
      5,
    )
    await handle.ready
    const turnsBefore = events.filter(
      (event) => event.type === 'session.turn',
    ).length
    await appendFile(
      f.path(child),
      encode([{ ...(await checkpointFixture()), step_index: 2 }]),
    )
    await handle.flush()
    assert.equal(handle.isRunning(), false)
    assert.equal(
      events.filter((event) => event.type === 'session.turn').length,
      turnsBefore,
    )
    const projected = events.find(
      (event) =>
        event.type === 'timeline.item' && event.item.type === 'compaction',
    )
    assert.ok(projected?.type === 'timeline.item')
    assert.equal(projected.sessionId, 'child')
    assert.equal(projected.item.id, 'child:step:2:compaction')
  } finally {
    handle?.()
    await f.close()
  }
})

test('a complete checkpoint before a partial trailing row is delivered before cancellation', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(child, [
      message(0, parent, 'work'),
      { ...(await checkpointFixture()), step_index: 1 },
    ])
    await appendFile(f.path(child), '{"step_index":2,"type":"PLANNER_RESPONSE"')
    const events: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'child',
      (event) => events.push(event),
      5,
    )
    await handle.ready
    await handle.cancel()
    const projected = compactionEvents(events)
    assert.equal(
      projected.length,
      1,
      'a partial tail must not discard the complete checkpoint prefix',
    )
    assert.equal(projected[0].sessionId, 'child')
    assert.ok(
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'canceled',
      ),
    )
  } finally {
    handle?.()
    await f.close()
  }
})

function assertCheckpointRetainsTask(checkpoint: Row, IsDaemon: boolean) {
  const projector = new BrainTranscriptProjector('p')
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: 'run_command', args: { IsDaemon } }],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: 'Tool is running as a background task with task id: tracked',
  })
  projector.project({ ...checkpoint, step_index: 3 })
  assert.equal(projector.hasBackground(), !IsDaemon)
  assert.equal(projector.hasDaemons(), IsDaemon)
}

test('a completed checkpoint retains background and daemon task tracking', async () => {
  const checkpoint = await checkpointFixture()
  assertCheckpointRetainsTask(checkpoint, false)
  assertCheckpointRetainsTask(checkpoint, true)
})

test('low-priority kill notice during foreground loop or after inline ask_question does not trigger wakeup', async () => {
  // Case 1: ad0fa4c8 shape — backgrounded task, continued synchronous tools, manage_task(kill), LOW SYSTEM_MESSAGE, final reply.
  const f1 = await fixture()
  try {
    const taskId = `${parent}/task-3`
    const msgDir = join(f1.root, parent, '.system_generated/messages')
    await mkdir(msgDir, { recursive: true })
    await writeFile(
      join(msgDir, 'msg-low-kill.json'),
      JSON.stringify({
        id: 'msg-low-kill',
        sender: taskId,
        recipient: parent,
        priority: 'MESSAGE_PRIORITY_LOW',
        hideFromUser: true,
        content: `Task id "${taskId}" was canceled with result:\ncanceled`,
        timestamp: new Date().toISOString(),
      }),
    )
    await writeFile(
      join(msgDir, 'read.json'),
      JSON.stringify({ 'msg-low-kill': true }),
    )
    await f1.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'investigate' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'fg-bg',
            name: 'run_command',
            args: { CommandLine: 'sleep 30' },
          },
        ],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskId}\n`,
      },
      {
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'fg-read', name: 'read_file', args: { Path: '/a.ts' } },
        ],
      },
      {
        step_index: 5,
        type: 'GENERIC',
        status: 'DONE',
        content: 'file content',
      },
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'fg-kill',
            name: 'manage_task',
            args: { Action: 'kill', TaskId: taskId },
          },
        ],
      },
      {
        step_index: 7,
        type: 'GENERIC',
        status: 'DONE',
        content: `Killed task ${taskId}`,
      },
      {
        step_index: 8,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-13T04:00:00Z sender=${taskId} priority=MESSAGE_PRIORITY_LOW content=Task id "${taskId}" was canceled with result:\ncanceled\n</SYSTEM_MESSAGE>`,
      },
      {
        ...done(9, 'Final foreground answer.'),
        input_tokens: 100,
        cache_read_tokens: 50,
        output_tokens: 20,
      },
    ])
    const itemsA: ProviderTimelineItem[] = []
    const resA = await settleBrainTurn(
      f1.root,
      parent,
      'p',
      (item) => itemsA.push(item),
      {
        minStepIndex: 0,
      },
    )
    assert.equal(resA.status, 'completed')
    assert.equal(resA.wokeUp, undefined)
    assert.equal(resA.wakeupUsage, undefined)
    assert.equal(
      itemsA.some((i) => i.type === 'assistant_message'),
      false,
      'foreground final reply after low-priority cancellation must not be re-emitted',
    )
  } finally {
    await f1.close()
  }

  // Case 2: Inline-answered foreground ask_question followed by LOW SYSTEM_MESSAGE and final foreground reply.
  const f2 = await fixture()
  try {
    await f2.put(parent, [
      { step_index: 10, type: 'USER_INPUT', content: 'ask and continue' },
      {
        step_index: 11,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'ask-fg',
            name: 'ask_question',
            args: { questions: [{ question: 'Pick?', options: ['A', 'B'] }] },
          },
        ],
      },
      {
        step_index: 12,
        type: 'GENERIC',
        status: 'DONE',
        content: '{"action":"accept","content":{"q1":"A"}}',
      },
      {
        step_index: 13,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-13T04:01:00Z sender=bg priority=MESSAGE_PRIORITY_LOW content=background info\n</SYSTEM_MESSAGE>`,
      },
      done(14, 'Foreground answer after inline question.'),
    ])
    const itemsQ: ProviderTimelineItem[] = []
    const resQ = await settleBrainTurn(
      f2.root,
      parent,
      'p',
      (item) => itemsQ.push(item),
      {
        minStepIndex: 9,
      },
    )
    assert.equal(resQ.status, 'completed')
    assert.equal(resQ.wokeUp, undefined)
    assert.equal(
      itemsQ.some((i) => i.type === 'assistant_message'),
      false,
    )
  } finally {
    await f2.close()
  }
})

test('orphan trailing GENERIC(DONE) after complete PLANNER_RESPONSE does not un-latch foreground_idle', async () => {
  const f = await fixture()
  try {
    const taskId = `${parent}/task-late-generic`
    await f.deliver(
      parent,
      taskId,
      `Task id "${taskId}" finished with result:\nready`,
      true,
      'msg-late-generic',
    )
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run and wait' },
      { ...done(2, 'Waiting for task'), input_tokens: 10, output_tokens: 5 },
      // Orphan GENERIC(DONE) with no pending tool call must not reset foreground_idle -> foreground_active.
      { step_index: 3, type: 'GENERIC', status: 'DONE' },
      message(4, taskId, `Task id "${taskId}" finished with result:\nready`),
      { ...done(5, 'Task ready.'), input_tokens: 20, output_tokens: 4 },
    ])
    const items: ProviderTimelineItem[] = []
    const res = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => items.push(item),
      { minStepIndex: 0 },
    )
    assert.equal(res.status, 'completed')
    assert.equal(res.wokeUp, true)
    assert.equal(res.wakeupUsage?.inputTokens, 20)
  } finally {
    await f.close()
  }
})

test('a verified wakeup call keeps its ID when its input repeats a foreground call', async (t) => {
  const f = await fixture()
  t.after(f.close)
  const repeated = { CommandLine: 'echo repeated' }
  const snapshots = new Map([
    ['fg', { toolCallId: 'fg', title: 'Command', rawInput: repeated }],
    ['post', { toolCallId: 'post', title: 'Command', rawInput: repeated }],
  ])
  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'run and wait' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { name: 'run_command', args: { CommandLine: 'echo other' } },
      ],
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'other output' },
    done(4, 'Waiting for inbox.'),
    message(5, 'inbox', 'Continue after the response'),
    {
      step_index: 6,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ name: 'run_command', args: repeated }],
    },
    {
      step_index: 7,
      type: 'GENERIC',
      status: 'DONE',
      content: 'wakeup output',
    },
    { ...done(8, 'Finished.'), input_tokens: 42, output_tokens: 1 },
  ])
  const items: ProviderTimelineItem[] = []
  const result = await settleBrainTurn(
    f.root,
    parent,
    'p',
    (item) => items.push(item),
    {
      acpToolCallIds: ['fg', 'post'],
      foregroundAcp: [
        { type: 'tool_call', callId: 'fg' },
        { type: 'assistant_message', text: 'Waiting for inbox.' },
      ],
      toolSnapshot: (id) => snapshots.get(id),
    },
  )
  assert.deepEqual(completedToolOutputs(items), [['post', 'wakeup output']])
  assert.equal(result.status, 'completed')
  assert.equal(result.wakeupUsage?.inputTokens, 42)
})

test(
  'an ACP post-response tool frame ahead of Brain cannot re-bill the in-RPC wakeup',
  { timeout: 5000 },
  async (t) => {
    const f = await fixture()
    t.after(f.close)
    const tracker = createStepTracker()
    const obs = manualObs(f.root, parent)
    const abort = new AbortController()
    t.after(() => {
      abort.abort()
      return obs.dispose()
    })
    const { foreground, wakeup, answer } = recordedWakeup
    const toolStarts = [
      foreground.updates[2],
      wakeup.updates[3],
      wakeup.updates[5],
      wakeup.updates[9],
    ]
    const snapshots = new Map(
      toolStarts.map(({ sessionUpdate, toolCallId, title, rawInput }) => {
        assert.equal(sessionUpdate, 'tool_call')
        assert.ok(toolCallId && title)
        return [toolCallId, { toolCallId, title, rawInput }] as const
      }),
    )
    await f.put(parent, [
      ...foreground.rows,
      ...wakeup.rows.slice(0, 4),
      { ...done(8, 'Reads finished.'), input_tokens: 40, output_tokens: 1 },
      message(9, 'second-inbox-message', 'Continue after the response'),
    ])
    const items: ProviderTimelineItem[] = []
    const usage = new Map<number, number>()
    let wakeupInput: number | undefined
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        observation: obs,
        acpToolCallIds: ['call_1', 'call_2', 'call_3', 'call_4'],
        foregroundAcp: [
          { type: 'reasoning', text: 'Checking background progress.' },
          { type: 'tool_call', callId: 'call_1' },
          { type: 'assistant_message', text: 'started' },
          { type: 'reasoning', text: 'Checking background progress.' },
          { type: 'tool_call', callId: 'call_2' },
          { type: 'tool_call', callId: 'call_3' },
          { type: 'assistant_message', text: 'Reads finished.' },
        ],
        toolSnapshot: (id) => snapshots.get(id),
        onStep: (step) => {
          tracker.onStep(step)
          usage.set(step, wakeupInput ?? 0)
        },
        onWakeupState: (state) => {
          wakeupInput = state.wakeupUsage?.inputTokens
        },
      }),
    )
    await tracker.waitFor(9)
    assert.equal(usage.get(8), 0)
    assert.deepEqual(items.filter(isTerminalToolCall), [])
    await appendFile(
      f.path(parent),
      encode([
        { ...wakeup.rows[4], step_index: 10 },
        { ...answer.rows[0], step_index: 11 },
        { ...answer.rows[1], step_index: 12 },
      ]),
    )
    await flushObs(obs)
    const result = await running
    assert.equal(result.status, 'completed')
    assert.equal(result.wakeupUsage?.inputTokens, 4814)
    assert.deepEqual(
      items
        .filter(isTerminalToolCall)
        .map(({ callId }) => callId)
        .filter((id) => id === 'call_2' || id === 'call_3'),
      ['call_2', 'call_3'],
    )
  },
)

test(
  'an ACP-ahead turn still verifies the foreground prefix after Brain flushes past foreground tools',
  { timeout: 5000 },
  async (t) => {
    const f = await fixture()
    t.after(f.close)
    const tracker = createStepTracker()
    const obs = manualObs(f.root, parent)
    const abort = new AbortController()
    t.after(() => {
      abort.abort()
      return obs.dispose()
    })
    const fgInput = { Path: '/fixture/fg.txt' }
    const postInput1 = { Path: '/fixture/post-1.txt' }
    const postInput2 = { Path: '/fixture/post-2.txt' }
    const snapshots = new Map([
      ['fg-1', { toolCallId: 'fg-1', title: 'Read', rawInput: fgInput }],
      ['post-1', { toolCallId: 'post-1', title: 'Read', rawInput: postInput1 }],
      ['post-2', { toolCallId: 'post-2', title: 'Read', rawInput: postInput2 }],
    ])
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'work' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ name: 'read_file', args: fgInput }],
      },
      { step_index: 2, type: 'GENERIC', status: 'DONE', content: 'fg data' },
      done(3, 'started'),
      message(4, 'task-in-rpc', 'In-RPC completion'),
      {
        ...done(5, 'in-rpc reply'),
        input_tokens: 40,
        output_tokens: 1,
      },
      message(6, 'task-post-rpc', 'Post-RPC completion'),
      {
        step_index: 7,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ name: 'read_file', args: postInput1 }],
      },
      { step_index: 8, type: 'GENERIC', status: 'DONE', content: 'post 1' },
    ])
    const items: ProviderTimelineItem[] = []
    const usage = new Map<number, number>()
    let wakeupInput: number | undefined
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        observation: obs,
        acpToolCallIds: ['fg-1', 'post-1', 'post-2'],
        foregroundAcp: [
          { type: 'tool_call', callId: 'fg-1' },
          { type: 'assistant_message', text: 'started' },
          { type: 'assistant_message', text: 'in-rpc reply' },
        ],
        toolSnapshot: (id) => snapshots.get(id),
        onStep: (step) => {
          tracker.onStep(step)
          usage.set(step, wakeupInput ?? 0)
        },
        onWakeupState: (state) => {
          wakeupInput = state.wakeupUsage?.inputTokens
        },
      }),
    )
    await tracker.waitFor(8)
    assert.equal(usage.get(5), 0)
    await appendFile(
      f.path(parent),
      encode([
        {
          step_index: 9,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [{ name: 'read_file', args: postInput2 }],
        },
        { step_index: 10, type: 'GENERIC', status: 'DONE', content: 'post 2' },
        { ...done(11, 'post-rpc final'), input_tokens: 99, output_tokens: 3 },
      ]),
    )
    await flushObs(obs)
    const result = await running
    assert.equal(result.status, 'completed')
    assert.equal(result.wakeupUsage?.inputTokens, 99)
    assert.equal(result.wakeupUsage?.outputTokens, 3)
    assert.deepEqual(
      items
        .filter(isTerminalToolCall)
        .map(({ callId, detail }) => [
          callId,
          detail.type === 'read' && detail.content,
        ]),
      [
        ['fg-1', 'fg data'],
        ['post-1', 'post 1'],
        ['post-2', 'post 2'],
      ],
    )
  },
)

test('running empty ACP content followed by status-only completion still recovers Brain result', async (t) => {
  const f = await fixture()
  t.after(f.close)
  const timeline = new Timeline('p')
  const readA = { Path: '/fixture/a.txt' }
  const readB = { Path: '/fixture/b.txt' }
  const startA = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'read-a',
    title: 'read_file',
    status: 'in_progress',
    rawInput: readA,
    content: [],
  })
  assert.ok(startA)
  timeline.track(startA)
  const doneA = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'read-a',
    status: 'completed',
  })
  assert.ok(doneA)
  timeline.track(doneA)
  const startB = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'read-b',
    title: 'read_file',
    status: 'in_progress',
    rawInput: readB,
    content: [],
  })
  assert.ok(startB)
  timeline.track(startB)
  const doneB = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'read-b',
    status: 'completed',
    content: [],
  })
  assert.ok(doneB)
  timeline.track(doneB)
  assert.equal(timeline.isRunning('p:tool:read-a'), false)
  assert.equal(timeline.hasToolResult('read-a'), false)
  assert.equal(timeline.hasToolResult('read-b'), true)
  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'read both' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { name: 'read_file', args: readA },
        { name: 'read_file', args: readB },
      ],
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'alpha' },
    { step_index: 4, type: 'GENERIC', status: 'DONE', content: 'beta' },
    done(5, 'Done.'),
  ])
  const items: ProviderTimelineItem[] = []
  const result = await settleBrainTurn(
    f.root,
    parent,
    'p',
    (item) => items.push(item),
    {
      acpToolCallIds: ['read-a', 'read-b'],
      toolSnapshot: (id) => timeline.toolCallSnapshot(id),
      hasToolResult: (id) => timeline.hasToolResult(id),
      shouldUpdateTool: (item) => timeline.isRunning(item.id),
    },
  )
  assert.equal(result.status, 'completed')
  assert.deepEqual(
    items
      .filter(isTerminalToolCall)
      .map(({ callId, detail }) => [
        callId,
        detail.type === 'read' && detail.content,
      ]),
    [['read-a', 'alpha']],
  )
})

test('anonymous live ask_question GENERIC(DONE) resumes foreground_active instead of staying foreground_idle', () => {
  const node = new BrainNode(parent, 'p', { allowInitialSpawnPrompt: false })
  node.projector.bindLiveToolCallIds([], new Set())
  node.ingest({ step_index: 1, type: 'USER_INPUT', content: 'ask me' })
  node.ingest({
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      {
        name: 'ask_question',
        args: { questions: [{ question: 'Pick?', options: ['A', 'B'] }] },
      },
    ],
  })
  assert.equal(node.promptPhase, 'foreground_idle')
  node.ingest({ step_index: 3, type: 'GENERIC', status: 'DONE', content: 'A' })
  assert.equal(node.promptPhase, 'foreground_active')
  node.ingest(message(4, 'task-bg', 'Background finished'))
  assert.equal(node.promptPhase, 'foreground_active')
  assert.equal(node.wokeUp, false)
})

test('pure [plan review due] notice resumes foreground_active from foreground_idle without entering woken', () => {
  const node = new BrainNode(parent, 'p', { allowInitialSpawnPrompt: false })
  node.ingest({ step_index: 1, type: 'USER_INPUT', content: 'implement plan' })
  node.ingest(done(2, 'Finished implementing.'))
  assert.equal(node.promptPhase, 'foreground_idle')
  node.ingest({
    step_index: 3,
    type: 'SYSTEM_MESSAGE',
    content:
      '<SYSTEM_MESSAGE>\n[plan review due] Review the completed work.\n</SYSTEM_MESSAGE>',
  })
  assert.equal(node.promptPhase, 'foreground_active')
  assert.equal(node.wokeUp, false)
  node.ingest(message(4, 'task-bg', 'Background update during review'))
  assert.equal(node.promptPhase, 'foreground_active')
  assert.equal(node.wokeUp, false)
})

test('declining native elicitation does not suppress a non-reader observation fault', async (t) => {
  const f = await fixture()
  t.after(f.close)
  await f.put(parent, [{ step_index: 1, type: 'USER_INPUT', content: 'work' }])
  const fault = Object.assign(new Error('step callback failed'), {
    code: 'EBUSY',
  })
  await assert.rejects(
    settleBrainTurn(f.root, parent, 'p', () => {}, {
      isWakeupQuestionDeclined: () => true,
      onStep: () => {
        throw fault
      },
    }),
    fault,
  )
})

async function assertCachedWakeupBilled(
  root: string,
  foregroundAcp: readonly ForegroundAcpSegment[],
) {
  const itemsC: ProviderTimelineItem[] = []
  const resC = await settleBrainTurn(
    root,
    parent,
    'p',
    (item) => itemsC.push(item),
    {
      minStepIndex: 0,
      foregroundAcp,
    },
  )
  assert.equal(resC.status, 'completed')
  assert.equal(resC.wokeUp, true)
  assert.deepEqual(resC.wakeupUsage, {
    inputTokens: 0,
    cachedReadTokens: 50000,
    outputTokens: 10,
  })
}

test('in-RPC high-priority continuation is suppressed and excluded from wakeupUsage while genuine post-RPC wakeup usage is included', async () => {
  // Subcase 1: Covered in-RPC continuation with tool calls, retry warning, and foreground reasoning/text.
  const f1 = await fixture()
  try {
    const taskId = `${parent}/task-196`
    await f1.deliver(
      parent,
      taskId,
      `Task id "${taskId}" finished with result:\n42`,
      true,
      'msg-196',
      {
        conversationId: parent,
        stepIndex: 3,
        callId: 'bg-196',
        name: 'run_command',
      },
    )
    await f1.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run and wait' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'bg-196',
            name: 'run_command',
            args: { CommandLine: 'echo 42' },
          },
        ],
        input_tokens: 10,
        output_tokens: 2,
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskId}\n`,
      },
      {
        ...done(4, 'OK'),
        input_tokens: 20,
        output_tokens: 3,
      },
      message(5, taskId, `Task id "${taskId}" finished with result:\n42`),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'fg-read', name: 'read_file', args: { Path: '/out.txt' } },
        ],
        input_tokens: 30,
        output_tokens: 4,
      },
      {
        step_index: 7,
        type: 'GENERIC',
        status: 'DONE',
        content: 'out',
      },
      {
        step_index: 8,
        source: 'SYSTEM',
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        error:
          'The stream was interrupted. Please continue the task you were working on.',
      },
      {
        step_index: 9,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        thinking: 'Summarizing',
        content: 'Done: 42.',
        input_tokens: 40,
        output_tokens: 5,
      },
    ])
    const items1: ProviderTimelineItem[] = []
    const res1 = await settleBrainTurn(
      f1.root,
      parent,
      'p',
      (item) => items1.push(item),
      {
        minStepIndex: 0,
        acpToolCallIds: ['bg-196', 'fg-read'],
        toolSnapshot: () => ({
          toolCallId: 'fg-read',
          title: 'Read',
          rawOutput: 'ACP result',
        }),
        foregroundAcp: [
          { type: 'tool_call', callId: 'bg-196' },
          { type: 'assistant_message', text: 'OK' },
          { type: 'tool_call', callId: 'fg-read' },
          { type: 'reasoning', text: 'partialSummarizing' },
          { type: 'assistant_message', text: 'Done: 42.' },
        ],
      },
    )
    assert.equal(res1.status, 'completed')
    assert.equal(res1.wokeUp, undefined)
    assert.equal(res1.wakeupUsage, undefined)
    assert.deepEqual(
      items1.map(itemTag),
      ['tool_call:bg-196:completed'],
      'in-RPC continuation must emit only unstreamed background tool completion',
    )
  } finally {
    await f1.close()
  }

  // Subcase 2: 100% KV-cache-hit post-RPC wakeup (input_tokens: 0) still bills the wakeup.
  const f2 = await fixture()
  try {
    await f2.deliver(parent, 'bg-cached', 'ping', true)
    await f2.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'cached' },
      { ...done(2, 'Start'), input_tokens: 20, output_tokens: 2 },
      message(3, 'bg-cached', 'ping'),
      {
        ...done(4, 'Cached wakeup reply.'),
        input_tokens: 0,
        cache_read_tokens: 50000,
        output_tokens: 10,
      },
    ])
    await assertCachedWakeupBilled(f2.root, [
      { type: 'assistant_message', text: 'Start' },
    ])
  } finally {
    await f2.close()
  }

  // Subcase 3: Hybrid turn with identical text ("OK") — first OK is in-RPC (covered), second OK is genuine post-RPC wakeup.
  const f3 = await fixture()
  try {
    await f3.deliver(parent, 'other-1', 'first msg', true)
    await f3.deliver(parent, 'other-2', 'second msg', true)
    await f3.put(parent, [
      { step_index: 20, type: 'USER_INPUT', content: 'hybrid' },
      done(21, 'Start'),
      message(22, 'other-1', 'first msg'),
      {
        ...done(23, 'OK'),
        input_tokens: 30,
        cache_read_tokens: 5,
        output_tokens: 4,
      },
      message(24, 'other-2', 'second msg'),
      {
        ...done(25, 'OK'),
        input_tokens: 50,
        cache_read_tokens: 10,
        output_tokens: 6,
      },
    ])
    const items2: ProviderTimelineItem[] = []
    const res2 = await settleBrainTurn(
      f3.root,
      parent,
      'p',
      (item) => items2.push(item),
      {
        minStepIndex: 19,
        foregroundAcp: [{ type: 'assistant_message', text: 'StartOK' }],
      },
    )
    assert.equal(res2.status, 'completed')
    assert.equal(res2.wokeUp, true)
    assert.deepEqual(res2.wakeupUsage, {
      inputTokens: 50,
      cachedReadTokens: 10,
      outputTokens: 6,
    })
    assert.deepEqual(items2, [])
  } finally {
    await f3.close()
  }
})

test('advanceForegroundCursor handles partial offsets, allowSkip boundaries, and tool_call offsets', () => {
  const segments = [
    { type: 'assistant_message' as const, text: 'AlphaBetaGamma' },
    { type: 'tool_call' as const, callId: 'call-1' },
    { type: 'assistant_message' as const, text: 'OK' },
    { type: 'assistant_message' as const, text: 'OK' },
  ]
  const msg = (text: string): ProviderTimelineItem => ({
    type: 'assistant_message',
    id: 'm',
    text,
  })
  const tool = (callId: string): ProviderTimelineItem => ({
    type: 'tool_call',
    id: 't',
    callId,
    name: 'read_file',
    status: 'running',
    error: null,
    detail: { type: 'unknown', input: null, output: null },
  })

  // Partial-offset matches advance sequentially through a merged segment.
  const c1 = advanceForegroundCursor(
    segments,
    { index: 0, offset: 0 },
    msg('Alpha') as Extract<
      ProviderTimelineItem,
      { type: 'assistant_message' }
    >,
    false,
  )
  assert.deepEqual(c1, { index: 0, offset: 5 })

  // allowSkip=false rejects mid-segment mismatch, while allowSkip=true finds the later substring.
  assert.equal(
    advanceForegroundCursor(
      segments,
      c1!,
      msg('Gamma') as Extract<
        ProviderTimelineItem,
        { type: 'assistant_message' }
      >,
      false,
    ),
    undefined,
  )
  const c2 = advanceForegroundCursor(
    segments,
    c1!,
    msg('Gamma') as Extract<
      ProviderTimelineItem,
      { type: 'assistant_message' }
    >,
    true,
  )
  assert.deepEqual(c2, { index: 1, offset: 0 })

  // tool_call requires offset === 0 when allowSkip=false, but can skip ahead when allowSkip=true.
  assert.equal(
    advanceForegroundCursor(
      segments,
      { index: 0, offset: 5 },
      tool('call-1') as Extract<ProviderTimelineItem, { type: 'tool_call' }>,
      false,
    ),
    undefined,
  )
  const c3 = advanceForegroundCursor(
    segments,
    { index: 0, offset: 5 },
    tool('call-1') as Extract<ProviderTimelineItem, { type: 'tool_call' }>,
    true,
  )
  assert.deepEqual(c3, { index: 2, offset: 0 })

  // Repeated identical text consumes one segment at a time and stops after exhaustion.
  const c4 = advanceForegroundCursor(
    segments,
    c3!,
    msg('OK') as Extract<ProviderTimelineItem, { type: 'assistant_message' }>,
    false,
  )
  assert.deepEqual(c4, { index: 3, offset: 0 })
  const c5 = advanceForegroundCursor(
    segments,
    c4!,
    msg('OK') as Extract<ProviderTimelineItem, { type: 'assistant_message' }>,
    false,
  )
  assert.deepEqual(c5, { index: 4, offset: 0 })
  assert.equal(
    advanceForegroundCursor(
      segments,
      c5!,
      msg('OK') as Extract<ProviderTimelineItem, { type: 'assistant_message' }>,
      false,
    ),
    undefined,
  )
})

test('re-waking a child across turns isolates prior failed grandchildren while retaining current-turn grandchildren across multi-wakeups', async () => {
  const f = await fixture()
  const grandchild2 = '55555555-5555-4555-8555-555555555555'
  const abort = new AbortController()
  let handle: BrainWatchHandle | undefined
  let running: Promise<unknown> | undefined
  try {
    // Turn 1: parent -> child -> grandchild (fails with ERROR_MESSAGE).
    const parentTurn1 = invoke(child)
    const childTurn1: Row[] = [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'gc1', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandchild }),
      },
      done(3, 'waiting on gc1'),
    ]
    await f.put(parent, parentTurn1)
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 3, 'invoke')
    await f.put(child, childTurn1)
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 2, 'gc1')
    await f.put(grandchild, [
      message(0, child, 'sub-task'),
      { step_index: 1, type: 'ERROR_MESSAGE', content: 'Grandchild 1 crashed' },
    ])

    const childEvents: ProviderEvent[] = []
    handle = watchBrain(f.root, child, 'child', (e) => childEvents.push(e), 5)
    await handle.ready
    assert.ok(
      childEvents.some(
        (e) =>
          e.type === 'session.turn' &&
          e.state === 'failed' &&
          /Grandchild 1 crashed/.test(e.error?.message ?? ''),
      ),
    )

    const turn1 = await settleBrainTurn(f.root, parent, 'p', () => {}, {
      minStepIndex: -1,
      pollIntervalMs: 5,
    })
    assert.equal(turn1.status, 'failed')
    assert.match(turn1.error ?? '', /Grandchild 1 crashed/)

    // Turn 2: parent re-wakes child via send_message; child spawns grandchild2 during wakeup #1,
    // then receives intra-turn wakeup #2 without touching grandchild1 or re-touching grandchild2.
    const parentTurn2: Row[] = [
      ...parentTurn1,
      { step_index: 5, type: 'USER_INPUT', content: 'continue without gc1' },
      ...report(child, 'wakeup 1', 6),
    ]
    await f.put(parent, parentTurn2)
    await f.deliver(child, parent, 'wakeup 1', true, 'w1', {
      conversationId: parent,
      stepIndex: 7,
      callId: 'report-6',
      name: 'send_message',
    })
    const childTurn2Wake1: Row[] = [
      ...childTurn1,
      message(4, parent, 'wakeup 1'),
      {
        step_index: 5,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'gc2', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 6,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandchild2 }),
      },
      ...report(parent, 'started gc2', 7),
    ]
    await f.subagent(child, grandchild2, 'SUBAGENT_STATE_ALIVE', 6, 'gc2')
    await f.put(grandchild2, [
      message(0, child, 'second sub-task'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'cmd', name: 'run_command', args: { CommandLine: 'sleep 1' } },
        ],
      },
    ])
    await f.rewrite(child, childTurn2Wake1)
    await f.deliver(parent, child, 'started gc2', true, 'c-w1')

    // Intra-turn wakeup #2 from parent while grandchild2 is still running.
    await appendFile(
      f.path(parent),
      encode([
        message(9, child, 'started gc2'),
        ...report(child, 'wakeup 2', 10),
      ]),
    )
    await f.deliver(child, parent, 'wakeup 2', true, 'w2', {
      conversationId: parent,
      stepIndex: 11,
      callId: 'report-10',
      name: 'send_message',
    })
    await appendFile(
      f.path(child),
      encode([message(10, parent, 'wakeup 2'), done(11, 'ack wakeup 2')]),
    )

    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: 4,
        pollIntervalMs: 5,
      }),
    )
    // Finish grandchild2 -> child -> parent (deliver inbox messages before sender completion rows).
    await f.deliver(child, grandchild2, 'gc2 finished', true, 'gc2-done')
    await appendFile(
      f.path(grandchild2),
      encode([
        { step_index: 2, type: 'GENERIC', status: 'DONE', content: 'ok' },
        ...report(child, 'gc2 finished', 3),
      ]),
    )
    await f.deliver(parent, child, 'turn 2 complete', true, 'c-w2')
    await appendFile(
      f.path(child),
      encode([
        message(12, grandchild2, 'gc2 finished'),
        ...report(parent, 'turn 2 complete', 13),
      ]),
    )
    await appendFile(
      f.path(parent),
      encode([message(13, child, 'turn 2 complete'), done(14, 'done turn 2')]),
    )

    assert.equal(((await running) as { status: string }).status, 'completed')
    await handle!.flush()
    assert.equal(handle!.isRunning(), false)
    const childTurns = turnIdsAndStates(childEvents)
    assert.deepEqual(childTurns, [
      'child:turn:1:started',
      'child:turn:1:failed',
      'child:turn:2:started',
      'child:turn:2:completed',
    ])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
    abort.abort()
    await running
    handle?.()
    await f.close()
  }
})

test('re-waking a child after Turn-1 retry timeout succeeds without Turn-1 retry carry-over and waits on inbox read-skew', async () => {
  const f = await fixture()
  const abort = new AbortController()
  let handle: BrainWatchHandle | undefined
  let running: Promise<unknown> | undefined
  try {
    await f.deliver(child, parent, 'initial task', true, 'init', {
      conversationId: parent,
      stepIndex: 3,
      callId: 'invoke',
      name: 'invoke_subagent',
    })
    // Turn 1: parent spawns child and sends wakeup #1; child ingests it and expires on a retry warning.
    const parentTurn1: Row[] = [...invoke(child), ...report(child, 'wake 1', 5)]
    await f.put(parent, parentTurn1)
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 3, 'invoke')
    await f.deliver(child, parent, 'wake 1', true, 'u1', {
      conversationId: parent,
      stepIndex: 6,
      callId: 'send-1',
      name: 'send_message',
    })
    const childTurn1: Row[] = [
      message(0, parent, 'initial task'),
      done(1, 'ready'),
      message(2, parent, 'wake 1'),
      interrupted(3, '2020-01-01T00:00:00Z'),
    ]
    await f.put(child, childTurn1)

    const childEvents: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'child',
      (e) => childEvents.push(e),
      5,
      undefined,
      undefined,
      undefined,
      40,
    )
    await handle.ready
    assert.ok(
      childEvents.some(
        (e) =>
          e.type === 'session.turn' &&
          e.turnId === 'child:turn:1' &&
          e.state === 'failed',
      ),
    )

    const turn1 = await settleBrainTurn(f.root, parent, 'p', () => {}, {
      minStepIndex: -1,
      pollIntervalMs: 5,
      retryTimeoutMs: 40,
    })
    assert.equal(turn1.status, 'failed')
    assert.equal(turn1.stalled, true)

    // Turn 2: parent logs completed send_message BEFORE u2.json appears in child inbox (read-skew).
    const parentTurn2: Row[] = [
      ...parentTurn1,
      { step_index: 8, type: 'USER_INPUT', content: 'retry in turn 2' },
      ...report(child, 'wake 2', 9),
    ]
    await f.put(parent, parentTurn2)

    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: 7,
        pollIntervalMs: 5,
        retryTimeoutMs: 40,
      }),
    )
    // Deliver u2 and complete child + parent in Turn 2 without clearing retryStep=3 except via childMinStep scoping.
    await f.deliver(child, parent, 'wake 2', true, 'u2', {
      conversationId: parent,
      stepIndex: 10,
      callId: 'send-2',
      name: 'send_message',
    })
    await f.deliver(parent, child, 'recovered in turn 2', true, 'c-done', {
      conversationId: child,
      stepIndex: 6,
      callId: 'send-done',
      name: 'send_message',
    })
    await appendFile(
      f.path(child),
      encode([
        message(4, parent, 'wake 2'),
        ...report(parent, 'recovered in turn 2', 5),
      ]),
    )
    await appendFile(
      f.path(parent),
      encode([message(12, child, 'recovered in turn 2'), done(13, 'all done')]),
    )

    assert.equal(((await running) as { status: string }).status, 'completed')
    await handle!.flush()
    assert.equal(handle!.isRunning(), false)
    const childTurns = turnIdsAndStates(childEvents)
    assert.deepEqual(childTurns, [
      'child:turn:1:started',
      'child:turn:1:failed',
      'child:turn:2:started',
      'child:turn:2:completed',
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { retryTimeoutMs: 40 }),
      false,
    )
  } finally {
    abort.abort()
    await running
    handle?.()
    await f.close()
  }
})

test('watchBrain fresh replay on nested Turn-2 re-wakeup isolates Turn-1 failed great-grandchild and allows peer-woken children to settle', async () => {
  const f = await fixture()
  const greatGrand1 = randomUUID()
  const greatGrand2 = randomUUID()
  const peerChild = randomUUID()
  const handles: BrainWatchHandle[] = []
  try {
    // Turn 1: parent -> child -> grandchild -> greatGrand1 (failed), plus parent -> peerChild (completed).
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 2,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(peerChild, parent, 'init peer', true, 'init-p', {
      conversationId: parent,
      stepIndex: 3,
      callId: 'spawn-p',
      name: 'invoke_subagent',
    })
    await f.deliver(grandchild, child, 'init gc', true, 'init-gc', {
      conversationId: child,
      stepIndex: 2,
      callId: 'spawn-gc',
      name: 'invoke_subagent',
    })
    await f.deliver(greatGrand1, grandchild, 'init gg1', true, 'init-gg1', {
      conversationId: grandchild,
      stepIndex: 2,
      callId: 'spawn-gg1',
      name: 'invoke_subagent',
    })

    // Turn 2: parent (USER_INPUT step 5) wakes child (step 7), child wakes grandchild (step 5) and peerChild (peer wakeup!).
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'spawn-c', name: 'invoke_subagent', args: {} },
          { id: 'spawn-p', name: 'invoke_subagent', args: {} },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: child }),
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: peerChild }),
      },
      done(4, 'turn 1 end'),
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'wake child turn 2', 6),
    ])
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 2, 'spawn-c')
    await f.subagent(parent, peerChild, 'SUBAGENT_STATE_ALIVE', 3, 'spawn-p')

    await f.deliver(child, parent, 'wake child turn 2', true, 'w-c2', {
      conversationId: parent,
      stepIndex: 7,
      callId: 'report-6',
      name: 'send_message',
    })
    await f.put(child, [
      message(0, parent, 'init child'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'spawn-gc', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandchild }),
      },
      done(3, 'waiting turn 1'),
      message(4, parent, 'wake child turn 2'),
      ...report(grandchild, 'wake gc turn 2', 5),
    ])
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 2, 'spawn-gc')

    await f.deliver(grandchild, child, 'wake gc turn 2', true, 'w-gc2', {
      conversationId: child,
      stepIndex: 6,
      callId: 'report-5',
      name: 'send_message',
    })
    await f.deliver(greatGrand2, grandchild, 'init gg2', true, 'init-gg2', {
      conversationId: grandchild,
      stepIndex: 6,
      callId: 'spawn-gg2',
      name: 'invoke_subagent',
    })
    await f.put(grandchild, [
      message(0, child, 'init gc'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'spawn-gg1', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: greatGrand1 }),
      },
      done(3, 'gc turn 1'),
      message(4, child, 'wake gc turn 2'),
      {
        step_index: 5,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'spawn-gg2', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 6,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: greatGrand2 }),
      },
      done(7, 'gc turn 2 done'),
    ])
    await f.subagent(
      grandchild,
      greatGrand1,
      'SUBAGENT_STATE_ALIVE',
      2,
      'spawn-gg1',
    )
    await f.subagent(
      grandchild,
      greatGrand2,
      'SUBAGENT_STATE_ALIVE',
      6,
      'spawn-gg2',
    )
    await f.put(greatGrand1, [
      message(0, grandchild, 'init gg1'),
      { step_index: 1, type: 'ERROR_MESSAGE', content: 'gg1 failed in turn 1' },
    ])
    await f.put(greatGrand2, [
      message(0, grandchild, 'init gg2'),
      done(1, 'gg2 succeeded in turn 2'),
    ])

    // Fresh watcher on grandchild (simulating session/load replay without any active parent turn).
    const gcEvents: ProviderEvent[] = []
    const gcHandle = watchBrain(
      f.root,
      grandchild,
      'gc',
      (e) => gcEvents.push(e),
      5,
    )
    handles.push(gcHandle)
    await gcHandle.ready
    assert.deepEqual(turnStates(gcEvents), ['started', 'completed'])

    // Peer-woken child: woken by sibling `child` rather than `parent`, must settle cleanly without hanging.
    await f.put(peerChild, [
      message(0, parent, 'init peer'),
      done(1, 'peer turn 1 done'),
    ])
    const peerCollector = createEventCollector()
    const peerHandle = watchBrain(
      f.root,
      peerChild,
      'peer',
      peerCollector.emit,
      5,
    )
    handles.push(peerHandle)
    await peerHandle.ready
    await f.deliver(peerChild, child, 'peer ping', true, 'peer-msg', {
      conversationId: child,
      stepIndex: 8,
      callId: 'peer-call',
      name: 'send_message',
    })
    await appendFile(
      f.path(peerChild),
      encode([message(2, child, 'peer ping'), done(3, 'peer turn 2 done')]),
    )
    await peerCollector.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.turnId === 'peer:turn:2' &&
        e.state === 'completed',
    )
  } finally {
    closeHandles(handles)
    await f.close()
  }
})

test('follow-up send_message to a child stalled in retryPending never deadlocks settleBrainTurn or masks watchBrain failures', async () => {
  const f = await fixture()
  const handles: BrainWatchHandle[] = []
  const sibling = '99999999-9999-4999-8999-999999999999'
  const spawnPair = (target: string, start: number, callId: string): Row[] => [
    {
      step_index: start,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ id: callId, name: 'invoke_subagent', args: {} }],
    },
    {
      step_index: start + 1,
      type: 'GENERIC',
      status: 'DONE',
      content: JSON.stringify({ conversationId: target }),
    },
  ]
  const sendPair = (
    target: string,
    text: string,
    start: number,
    callId: string,
  ): Row[] => [
    {
      step_index: start,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: callId,
          name: 'send_message',
          args: { Recipient: target, Message: text },
        },
      ],
    },
    {
      step_index: start + 1,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Message sent',
    },
  ]
  try {
    // Turn 1: parent spawns child (step 2) and sibling (step 4); both complete Turn 1.
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 2)
    await f.subagent(parent, sibling, 'SUBAGENT_STATE_ALIVE', 4)
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 2,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(sibling, parent, 'init sibling', true, 'init-s', {
      conversationId: parent,
      stepIndex: 4,
      callId: 'spawn-s',
      name: 'invoke_subagent',
    })
    const parentTurn1: Row[] = [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      ...spawnPair(child, 1, 'spawn-c'),
      ...spawnPair(sibling, 3, 'spawn-s'),
      done(5, 'turn 1 done'),
    ]
    await f.put(parent, parentTurn1)
    await f.put(child, [message(0, parent, 'init child'), done(1, 'c1 done')])
    await f.put(sibling, [
      message(0, parent, 'init sibling'),
      done(1, 's1 done'),
    ])

    const childEvents: ProviderEvent[] = []
    const childWatch = watchBrain(
      f.root,
      child,
      'child',
      (e) => childEvents.push(e),
      5,
      () => true,
      undefined,
      undefined,
      40,
    )
    handles.push(childWatch)
    await childWatch.ready

    // Turn 2: parent wakes child (w1 at step 8) and sibling (step 10).
    // Child ingests w1 (step 2), hits retryPending at step 3, and then parent sends a 2nd wakeup w2 (step 14).
    await f.deliver(child, parent, 'wake 1', true, 'w1', {
      conversationId: parent,
      stepIndex: 8,
      callId: 'w1-call',
      name: 'send_message',
    })
    await f.deliver(sibling, parent, 'wake s', true, 'ws', {
      conversationId: parent,
      stepIndex: 10,
      callId: 'ws-call',
      name: 'send_message',
    })
    await f.deliver(parent, sibling, 's2 report', true, 's2-rep')
    await appendFile(
      f.path(sibling),
      encode([message(2, parent, 'wake s'), done(3, 's2 done')]),
    )
    await f.deliver(child, parent, 'wake 2', false, 'w2', {
      conversationId: parent,
      stepIndex: 14,
      callId: 'w2-call',
      name: 'send_message',
    })
    await f.put(parent, [
      ...parentTurn1,
      { step_index: 6, type: 'USER_INPUT', content: 'turn 2' },
      ...sendPair(child, 'wake 1', 7, 'w1-call'),
      ...sendPair(sibling, 'wake s', 9, 'ws-call'),
      done(11, 'waiting'),
      message(12, sibling, 's2 report'),
      ...sendPair(child, 'wake 2', 13, 'w2-call'),
      done(15, 'waiting on child'),
    ])
    await appendFile(
      f.path(child),
      encode([
        message(2, parent, 'wake 1'),
        interrupted(3, '2020-01-01T00:00:00Z'),
      ]),
    )

    const settled = await settleBrainTurn(f.root, parent, 'p', () => {}, {
      minStepIndex: 5,
      pollIntervalMs: 5,
      retryTimeoutMs: 40,
      stallTimeoutMs: 200,
    })
    assert.equal(settled.status, 'failed')
    assert.equal(settled.stalled, true)
    assert.match(settled.error ?? '', /No retry recovery was observed/)

    await childWatch.flush()
    assert.deepEqual(turnIdsAndStates(childEvents), [
      'child:turn:1:started',
      'child:turn:1:completed',
      'child:turn:2:started',
      'child:turn:2:failed',
    ])

    // Same-turn spawn + follow-up send_message while child has expired retryPending or replay mode (isParentTurnActive=false).
    const sameTurnChild = 'aaaaaaaa-1111-4111-8111-111111111111'
    await f.subagent(parent, sameTurnChild, 'SUBAGENT_STATE_ALIVE', 17)
    await f.deliver(sameTurnChild, parent, 'init st', true, 'init-st', {
      conversationId: parent,
      stepIndex: 17,
      callId: 'spawn-st',
      name: 'invoke_subagent',
    })
    await f.deliver(sameTurnChild, parent, 'followup st', false, 'fu-st', {
      conversationId: parent,
      stepIndex: 19,
      callId: 'fu-st-call',
      name: 'send_message',
    })
    await f.put(sameTurnChild, [
      message(0, parent, 'init st'),
      interrupted(1, '2020-01-01T00:00:00Z'),
    ])
    await appendFile(
      f.path(parent),
      encode([
        ...spawnPair(sameTurnChild, 16, 'spawn-st'),
        ...sendPair(sameTurnChild, 'followup st', 18, 'fu-st-call'),
      ]),
    )
    const stEvents: ProviderEvent[] = []
    const stWatch = watchBrain(
      f.root,
      sameTurnChild,
      'st',
      (e) => stEvents.push(e),
      5,
      () => false,
      undefined,
      undefined,
      40,
    )
    handles.push(stWatch)
    await stWatch.ready
    assert.deepEqual(turnStates(stEvents), ['started', 'failed'])
  } finally {
    closeHandles(handles)
    await f.close()
  }
})

test('terminal ERROR_MESSAGE in parent does not hide live subagents re-woken earlier in the same turn', async () => {
  const f = await fixture()
  try {
    // Turn 1: parent spawns child (step 3), both finish Turn 1.
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 3)
    await f.deliver(child, parent, 'init', true, 'init-msg', {
      conversationId: parent,
      stepIndex: 3,
      callId: 'invoke',
      name: 'invoke_subagent',
    })
    const turn1 = invoke(child)
    await f.put(parent, turn1)
    await f.put(child, [message(0, parent, 'init'), done(1, 'c1 done')])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)

    // Turn 2: parent re-wakes child (step 7), child starts running, then parent crashes with ERROR_MESSAGE (step 9).
    await f.deliver(child, parent, 'turn 2 work', true, 'w2-msg', {
      conversationId: parent,
      stepIndex: 7,
      callId: 'report-6',
      name: 'send_message',
    })
    await appendFile(
      f.path(child),
      encode([
        message(2, parent, 'turn 2 work'),
        { step_index: 3, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
      ]),
    )
    await f.put(parent, [
      ...turn1,
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'turn 2 work', 6),
      { step_index: 9, type: 'ERROR_MESSAGE', content: 'Parent step crashed' },
    ])
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      true,
      'live re-woken child must remain visible to hasActiveBackgroundWork after parent ERROR_MESSAGE',
    )

    // Child finishes foreground turn after starting a standing IsDaemon schedule.
    await appendFile(
      f.path(child),
      encode([
        {
          step_index: 4,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'daemon-1',
              name: 'schedule',
              args: {
                CronExpression: '*/5 * * * *',
                Prompt: 'tick',
                IsDaemon: true,
              },
            },
          ],
        },
        {
          step_index: 5,
          type: 'GENERIC',
          status: 'RUNNING',
          content: `Tool is running as a background task with task id: ${child}/task-5\n`,
        },
        done(6, 'daemon armed'),
      ]),
    )
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      true,
      'idle reaping check must still see standing daemon in re-woken child after parent ERROR_MESSAGE',
    )

    // A server restart notice stops all subagents and daemons, clearing active work.
    await appendFile(
      f.path(parent),
      encode([
        message(
          10,
          'system',
          '[Notice] All your subagents and background tasks have been stopped due to server restart.',
        ),
      ]),
    )
    assert.equal(
      await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
      false,
    )
  } finally {
    await f.close()
  }
})

test('watchBrain does not advance turnFloor past child ERROR_MESSAGE on grandchild report in the same turn', async (t) => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    const events: ProviderEvent[] = []
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 2,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 2, 'spawn-gc')
    await f.put(child, [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        content:
          'The stream was interrupted. Please continue the task you were working on.',
      },
    ])
    handle = watchBrain(
      f.root,
      child,
      'child',
      (e) => events.push(e),
      60_000,
      undefined,
      undefined,
      undefined,
      200,
    )
    await handle.ready
    assert.equal(handle.isRunning(), true)
    assert.equal(handle.resumePoint().afterStep, 1)
    // The retry warning arms its deadline on its own read pass, before the report lands.
    await appendFile(
      f.path(child),
      encode([message(2, grandchild, 'late grandchild report')]),
    )
    await handle.flush()
    assert.equal(handle.resumePoint().afterStep, 2)
    assert.equal(handle.isRunning(), true)
    t.mock.timers.tick(200)
    await handle.flush()
    assert.equal(handle.isRunning(), false)
    assert.ok(
      events.some((e) => e.type === 'session.turn' && e.state === 'failed'),
      'grandchild report must not erase child retry-timeout failure',
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('SubagentRegistry.flushActive flushes newly discovered nested grandchild watchers', async () => {
  const f = await fixture()
  const events: ProviderEvent[] = []
  const registry = new SubagentRegistry({
    sessionId: 's',
    cwd: f.root,
    brainRoot: f.root,
    capabilities: ['session.subsession'],
    emit: (event) => events.push(event),
    getNativeId: () => parent,
    isParentTurnActive: () => true,
    isLoading: () => false,
    isUnavailable: () => false,
  })
  try {
    await f.put(child, [message(0, parent, 'work')])
    await f.put(grandchild, [
      message(0, child, 'gc work'),
      { step_index: 1, type: 'ERROR_MESSAGE', content: 'gc failed' },
    ])
    registry.linkSubsession('s', parent, {
      type: 'tool_call',
      id: 'spawn',
      callId: 'spawn',
      name: 'invoke_subagent',
      detail: { type: 'sub_agent', childSessionId: child, log: '' },
      status: 'completed',
      error: null,
    })
    await registry.waitNestedReady()
    // Write grandchild subagent metadata and spawn tool call after initial ready so flushActive discovers and flushes it.
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 1, 'gc-call')
    await f.put(child, [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'gc-call', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandchild }),
      },
      done(3, 'child done'),
    ])
    await registry.flushActive()

    assert.ok(
      events.some(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === `s:child:${grandchild}` &&
          e.state === 'failed',
      ),
      'flushActive must discover and flush nested grandchild watchers',
    )
  } finally {
    await registry.cancelAll()
    await f.close()
  }
})

test('watchBrain preserves fatal ERROR_MESSAGE when grandchild report follows in the same read', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const events: ProviderEvent[] = []
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 2,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 2, 'spawn-gc')
    await f.put(child, [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'ERROR_MESSAGE',
        content: 'Fatal child error before grandchild report',
      },
      message(2, grandchild, 'late grandchild report'),
    ])
    handle = watchBrain(f.root, child, 'child', (e) => events.push(e), 50)
    await handle.ready
    assert.equal(handle.isRunning(), false)
    assert.ok(
      events.some(
        (e) =>
          e.type === 'session.turn' &&
          e.state === 'failed' &&
          e.error?.message === 'Fatal child error before grandchild report',
      ),
      'fatal ERROR_MESSAGE must not be erased by trailing grandchild SYSTEM_MESSAGE',
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('watchBrain excludes a Turn-1 failed sibling when later root turns leave the grandchild unwoken', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const oldGc = '44444444-4444-4444-8444-444444444444'
    await f.put(parent, [
      ...invoke(child),
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'tagged t2', 6),
      ...report(child, 'untagged t2', 9),
      done(12, 't2'),
      { step_index: 13, type: 'USER_INPUT', content: 'unrelated turn 3' },
      done(14, 't3'),
    ])
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 2,
      callId: 'invoke',
      name: 'invoke_subagent',
    })
    await f.deliver(child, parent, 'tagged t2', true, 't2-c', {
      conversationId: parent,
      stepIndex: 7,
      callId: 'send-c-1',
      name: 'send_message',
    })
    await f.deliver(child, parent, 'untagged t2', true, 'u2-c')
    await f.put(child, [
      message(0, parent, 'init'),
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
        content: JSON.stringify({ conversationId: grandchild }),
      },
      done(4, 'child t1'),
      message(5, parent, 'tagged t2'),
      message(6, parent, 'untagged t2'),
      ...report(grandchild, 'gc t2', 7),
      done(10, 'child t2'),
    ])
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 2, 'invoke')
    await f.deliver(grandchild, child, 'init gc', true, 'init-gc', {
      conversationId: child,
      stepIndex: 2,
      callId: 'invoke',
      name: 'invoke_subagent',
    })
    await f.deliver(grandchild, child, 'gc t2', true, 'wake-gc', {
      conversationId: child,
      stepIndex: 8,
      callId: 'send-gc',
      name: 'send_message',
    })
    await f.subagent(
      grandchild,
      oldGc,
      'SUBAGENT_STATE_ALIVE',
      2,
      'invoke-old-gc',
    )
    await f.deliver(oldGc, grandchild, 'init old gc', true, 'init-old-gc', {
      conversationId: grandchild,
      stepIndex: 2,
      callId: 'invoke-old-gc',
      name: 'invoke_subagent',
    })
    await f.put(oldGc, [
      message(0, grandchild, 'old gc work'),
      {
        step_index: 1,
        type: 'ERROR_MESSAGE',
        content: 'old descendant failure',
      },
    ])
    await f.put(grandchild, [
      message(0, child, 'init gc'),
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          { id: 'invoke-old-gc', name: 'invoke_subagent', args: {} },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: oldGc }),
      },
      done(4, 'gc t1'),
      message(5, child, 'gc t2'),
      done(6, 'gc ok'),
    ])

    const unrelated: ProviderEvent[] = []
    handle = watchBrain(f.root, grandchild, 'gc', (e) => unrelated.push(e), 50)
    await handle.ready
    assert.ok(
      unrelated.some(
        (e) => e.type === 'session.turn' && e.state === 'completed',
      ),
      'fresh descendant watcher must complete without inheriting Turn-1 failed sibling',
    )
    assert.equal(
      unrelated.some((e) => e.type === 'session.turn' && e.state === 'failed'),
      false,
      'unrelated later root Turn-3 USER_INPUT must not leak old Turn-1 failure',
    )
    handle()
    handle = undefined

    // Root Turn 3 now wakes child B while grandchild C stays unwoken.
    await appendFile(
      f.path(parent),
      encode([...report(child, 'wake b in t3', 15), done(18, 't3')]),
    )
    await f.deliver(child, parent, 'wake b in t3', true, 't3-c', {
      conversationId: parent,
      stepIndex: 16,
      callId: 'send-c-3',
      name: 'send_message',
    })
    await appendFile(
      f.path(child),
      encode([
        message(11, parent, 'wake b in t3'),
        done(12, 'child t3 without waking gc'),
      ]),
    )

    const woken: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      grandchild,
      'gc-woken',
      (e) => woken.push(e),
      50,
      () => true,
    )
    await handle.ready
    assert.ok(
      woken.some((e) => e.type === 'session.turn' && e.state === 'completed'),
      'fresh descendant watcher must complete even after Root Turn 3 wakes child B',
    )
    assert.equal(
      woken.some((e) => e.type === 'session.turn' && e.state === 'failed'),
      false,
      'Root Turn 3 waking child B must not resurrect Turn-1 failure on grandchild C',
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('child initial spawn prompt after step-0 system reminder projects user_message without inflating consumedMessages', async () => {
  const f = await fixture()
  const abort = new AbortController()
  let handle: BrainWatchHandle | undefined
  let running: Promise<unknown> | undefined
  try {
    const reminder = {
      step_index: 0,
      type: 'SYSTEM_MESSAGE',
      content:
        '<SYSTEM_MESSAGE>\nPlugin environment reminder\n</SYSTEM_MESSAGE>',
    }
    const childNode = new BrainNode(child, 'child')
    assert.deepEqual(childNode.ingest(reminder), [])
    assert.equal(childNode.consumedMessages, 0)
    const spawnItems = childNode.ingest(message(1, parent, 'Audit repository'))
    assert.deepEqual(spawnItems, [
      {
        type: 'user_message',
        id: 'child:step:1',
        text: 'Audit repository',
      },
    ])
    assert.equal(childNode.consumedMessages, 0)

    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.subagent(child, grandchild, 'SUBAGENT_STATE_ALIVE', 3, 'sub-invoke')
    await f.put(child, [
      reminder,
      message(1, parent, 'Audit repository'),
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ id: 'sub-invoke', name: 'invoke_subagent', args: {} }],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandchild }),
      },
      done(4, 'waiting for grandchild'),
    ])
    await f.put(grandchild, [
      message(0, child, 'check details'),
      ...report(child, 'grandchild report', 1),
    ])
    // Grandchild report is marked read in child inbox before child flushes its wakeup step.
    await f.deliver(child, grandchild, 'grandchild report', true, 'gc-report')

    const childEvents: ProviderEvent[] = []
    handle = watchBrain(f.root, child, 'child', (e) => childEvents.push(e), 5)
    await handle.ready
    assert.ok(
      childEvents.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'user_message' &&
          e.item.id === 'child:step:1' &&
          e.item.text === 'Audit repository',
      ),
    )
    assert.equal(
      handle.isRunning(),
      true,
      'step-1 spawn prompt must not count as a consumed wakeup message',
    )

    assert.equal(
      await hasActiveBackgroundWork(f.root, parent),
      true,
      'a child still running its wakeup turn blocks parent settlement',
    )
    running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: -1,
        pollIntervalMs: 5,
      }),
    )

    await f.deliver(parent, child, 'child final report', true, 'c-report')
    await appendFile(
      f.path(child),
      encode([
        message(5, grandchild, 'grandchild report'),
        ...report(parent, 'child final report', 6),
      ]),
    )
    await appendFile(
      f.path(parent),
      encode([message(5, child, 'child final report'), done(6, 'all done')]),
    )
    assert.equal(((await running) as { status: string }).status, 'completed')

    // Root session (allowInitialSpawnPrompt = false) treats a step-0 UUID [Message] as a wakeup, not an initial spawn prompt.
    const rootNode = new BrainNode(parent, 'p', {
      allowInitialSpawnPrompt: false,
    })
    assert.deepEqual(rootNode.ingest(message(0, child, 'child wakeup')), [])
    assert.equal(rootNode.consumedMessages, 1)
    const rootProjector = new BrainTranscriptProjector(parent, undefined, false)
    assert.deepEqual(
      rootProjector.project(message(0, child, 'child wakeup')),
      [],
    )
  } finally {
    abort.abort()
    await running
    handle?.()
    await f.close()
  }
})

test('historical question-continuation USER_INPUT preserves turnStartStep and touchedSubagents', () => {
  const spawnAndAsk = (startStep: number): Row[] => [
    { step_index: startStep, type: 'USER_INPUT', content: 'start turn' },
    {
      step_index: startStep + 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { id: 'spawn-1', name: 'invoke_subagent', args: {} },
        {
          id: 'ask-1',
          name: 'ask_question',
          args: { questions: [{ question: 'Proceed?', options: ['Yes'] }] },
        },
      ],
    },
    {
      step_index: startStep + 2,
      type: 'GENERIC',
      status: 'DONE',
      content: JSON.stringify({ conversationId: child }),
    },
  ]

  // A persisted continuation follows an unlogged question.
  const replayNode = new BrainNode(parent, 'p', {
    allowInitialSpawnPrompt: false,
  })
  ingestAll(replayNode, spawnAndAsk(10))
  assert.equal(replayNode.turnStartStep, 10)
  assert.equal(replayNode.projector.hasPendingQuestion(), true)
  assert.deepEqual([...replayNode.projector.touchedSubagentIds()], [child])

  replayNode.ingest({
    step_index: 13,
    type: 'USER_INPUT',
    content: 'Yes',
  })
  assert.equal(
    replayNode.turnStartStep,
    10,
    'question-continuation USER_INPUT must preserve turnStartStep when pending question is unconsumed',
  )
  assert.deepEqual(
    [...replayNode.projector.touchedSubagentIds()],
    [child],
    'question-continuation USER_INPUT must preserve touchedSubagents',
  )

  // A subsequent new turn USER_INPUT advances turnStartStep and clears touchedSubagents.
  replayNode.ingest(done(14, 'done'))
  replayNode.ingest({
    step_index: 15,
    type: 'USER_INPUT',
    content: 'new turn',
  })
  assert.equal(replayNode.turnStartStep, 15)
  assert.deepEqual([...replayNode.projector.touchedSubagentIds()], [])

  // Stateless replay of woken ask_question preserves node.wokeUp and works when a sibling tool precedes ask_question.
  const wokenReplayNode = new BrainNode(parent, 'p', {
    allowInitialSpawnPrompt: false,
  })
  ingestAll(wokenReplayNode, [
    { step_index: 30, type: 'USER_INPUT', content: 'start' },
    done(31, 'waiting'),
    message(32, child, 'child finished'),
    {
      step_index: 33,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { id: 'bg-1', name: 'run_command', args: { CommandLine: 'echo ok' } },
        {
          id: 'ask-2',
          name: 'ask_question',
          args: { questions: [{ question: 'Continue?', options: ['Yes'] }] },
        },
      ],
    },
  ])
  assert.equal(wokenReplayNode.wokeUp, true)
  assert.equal(wokenReplayNode.projector.hasPendingQuestion(), true)
  wokenReplayNode.ingest({
    step_index: 34,
    type: 'USER_INPUT',
    content: 'Yes',
  })
  assert.equal(wokenReplayNode.turnStartStep, 30)
  assert.equal(
    wokenReplayNode.wokeUp,
    true,
    'stateless replay of woken question continuation must preserve node.wokeUp',
  )
})

test('initial spawn prompt quoting task results or batched with a followup message projects only the initial prompt', () => {
  const childNode = new BrainNode(child, 'c')
  const quotedContent =
    'Analyze this log:\nsender=task-1 content=Task id "task-1" finished with result:\nok'
  const batchedRow: Row = {
    step_index: 0,
    type: 'SYSTEM_MESSAGE',
    content: `<SYSTEM_MESSAGE>\n1. [Message] timestamp=2026-11-08T17:00:00Z sender=${parent} priority=MESSAGE_PRIORITY_HIGH content=${quotedContent}\n2. [Message] timestamp=2026-11-08T17:00:01Z sender=${parent} priority=MESSAGE_PRIORITY_HIGH content=followup\n</SYSTEM_MESSAGE>`,
  }
  const items = childNode.ingest(batchedRow)
  assert.deepEqual(items, [
    {
      type: 'user_message',
      id: 'c:step:0',
      text: quotedContent,
    },
  ])
  assert.equal(childNode.consumedMessages, 1)

  // Root countRowMessages(row, false) counts step-0 UUID wakeup without subtracting 1.
  assert.equal(countRowMessages(message(0, child, 'root wakeup'), false), 1)
})

test('same-turn second parent wakeup preserves the child terminal failure', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'first wake', 3),
      ...report(child, 'second wake', 6),
    ])
    await f.deliver(child, parent, 'init', true, 'init', {
      conversationId: parent,
      stepIndex: 0,
      name: 'invoke_subagent',
      callId: 'spawn',
    })
    await f.deliver(child, parent, 'first', true, 'first', {
      conversationId: parent,
      stepIndex: 4,
      name: 'send_message',
      callId: 'first',
    })
    await f.deliver(child, parent, 'second', true, 'second', {
      conversationId: parent,
      stepIndex: 7,
      name: 'send_message',
      callId: 'second',
    })
    await f.put(child, [
      message(0, parent, 'init'),
      done(1),
      message(2, parent, 'first'),
      { step_index: 3, type: 'ERROR_MESSAGE', content: 'same-turn fatal' },
      message(4, parent, 'second'),
    ])
    const events: ProviderEvent[] = []
    handle = watchBrain(f.root, child, 'child', (e) => events.push(e), 50)
    await handle.ready
    assert.ok(
      events.some(
        (e) =>
          e.type === 'session.turn' &&
          e.state === 'failed' &&
          e.error?.message === 'same-turn fatal',
      ),
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('isWokenActive and captureRuntimeEpoch fence pre-restart artifacts while detecting post-epoch work', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const staleParent = '44444444-4444-4444-8444-444444444444'
  const wokenRows: Row[] = [
    { step_index: 0, type: 'USER_INPUT', content: 'start' },
    done(1, 'fg'),
    message(2, child, 'wakeup'),
    { step_index: 3, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
  ]
  const wokenNode = new BrainNode(parent, 'p')
  ingestAll(wokenNode, wokenRows)
  assert.equal(wokenNode.isWokenWorking(), false)
  assert.equal(wokenNode.isWokenActive(), true)
  await f.put(parent, wokenRows)
  await f.deliver(parent, child, 'wakeup', true, 'm-woken')
  assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

  await f.put(child, [
    message(0, staleParent, 'work'),
    { step_index: 1, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
  ])
  await f.subagent(staleParent, child, 'SUBAGENT_STATE_ALIVE', 1, 'inv-1')
  const racedChild = '77777777-7777-4777-8777-777777777777'
  await f.subagent(
    staleParent,
    racedChild,
    'SUBAGENT_STATE_ALIVE',
    99,
    'inv-raced',
  )
  await f.deliver(staleParent, child, 'unread', false, 'msg-unread')
  await f.deliver(staleParent, child, 'deficit', true, 'msg-deficit')
  await f.put(staleParent, [
    { step_index: 0, type: 'USER_INPUT', content: 'old' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-1',
          name: 'run_command',
          args: { CommandLine: 'sleep 30' },
        },
        {
          id: 'cron-1',
          name: 'schedule',
          args: {
            CronExpression: '0 * * * *',
            IsDaemon: true,
            Prompt: 'tick',
          },
        },
        {
          id: 'inv-1',
          name: 'invoke_subagent',
          args: { Role: 'Researcher' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${staleParent}/task-1\n`,
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${staleParent}/task-2\n`,
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      content: `Launched subagent ${child}`,
    },
    done(5, 'old done'),
  ])
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
    }),
    true,
  )

  const epoch = await captureRuntimeEpoch(f.root, staleParent)
  assert.ok(epoch)
  assert.equal(epoch.afterStepIndex, 5)
  assert.equal(epoch.preEpochReadIds.has('msg-deficit'), true)
  assert.equal(epoch.ignoredMessages.has('msg-unread'), true)
  assert.equal(epoch.ignoredSubagentIds.has(child), true)
  assert.equal(epoch.ignoredSubagentIds.has(racedChild), true)
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )

  await f.deliver(staleParent, child, 'unread', true, 'msg-unread')
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  const clearedReadSettled = await alive(
    settleBrainTurn(f.root, staleParent, 'sp-read', () => {}, {
      pollIntervalMs: 5,
      stallTimeoutMs: 50,
      runtimeEpoch: epoch,
    }),
  )
  assert.equal(clearedReadSettled.status, 'completed')

  await f.deliver(staleParent, child, 'fresh', true, 'msg-fresh')
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    true,
  )

  await appendFile(
    f.path(staleParent),
    encode([
      message(6, child, 'fresh'),
      done(7, 'woken done'),
      { step_index: 8, type: 'USER_INPUT', content: 'turn 2' },
      done(9, 'turn 2 done'),
    ]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  const settled = await alive(
    settleBrainTurn(f.root, staleParent, 'sp', () => {}, {
      pollIntervalMs: 5,
      stallTimeoutMs: 50,
      runtimeEpoch: epoch,
    }),
  )
  assert.equal(settled.status, 'completed')

  await appendFile(
    f.path(staleParent),
    encode([
      {
        step_index: 10,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'cron-new',
            name: 'schedule',
            args: {
              CronExpression: '*/5 * * * *',
              Prompt: 'fresh',
              IsDaemon: true,
            },
          },
        ],
      },
      {
        step_index: 11,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${staleParent}/task-3\n`,
      },
      done(12, 'cron scheduled'),
    ]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: false,
      runtimeEpoch: epoch,
    }),
    false,
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, staleParent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    true,
  )
})

test('drained pre-epoch ignored wakeup that logs post-epoch cannot offset a later unflushed fresh wakeup', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  await f.deliver(parent, child, 'already-read', true, 'msg-old')
  await f.deliver(parent, child, 'unread', false, 'msg-unread')
  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'old' },
    done(2, 'old done'),
  ])

  const epoch = await captureRuntimeEpoch(f.root, parent)
  assert.ok(epoch)
  assert.equal(epoch.afterStepIndex, 2)
  assert.equal(epoch.preEpochReadIds.has('msg-old'), true)
  assert.equal(epoch.ignoredMessages.has('msg-unread'), true)

  // Post-epoch turn drains the pre-epoch unread wakeup AND writes its SYSTEM_MESSAGE row.
  await f.deliver(parent, child, 'unread', true, 'msg-unread')
  await appendFile(
    f.path(parent),
    encode([
      { step_index: 3, type: 'USER_INPUT', content: 'turn 1' },
      message(4, child, 'unread'),
      done(5, 'unread handled'),
    ]),
  )

  // A later fresh wakeup is marked read before its SYSTEM_MESSAGE flushes; it must stay unprocessed.
  await f.deliver(parent, child, 'fresh', true, 'msg-fresh')
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    true,
  )

  await appendFile(
    f.path(parent),
    encode([message(6, child, 'fresh'), done(7, 'fresh done')]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  const settled = await alive(
    settleBrainTurn(f.root, parent, 'p-drained', () => {}, {
      pollIntervalMs: 5,
      stallTimeoutMs: 50,
      runtimeEpoch: epoch,
    }),
  )
  assert.equal(settled.status, 'completed')
})

test('monotonic captureRuntimeEpoch preserves epoch fence across an O_TRUNC 0-byte rewrite window', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 1, 'inv-1')
  await f.deliver(parent, child, 'old', true, 'msg-read')
  await f.deliver(parent, child, 'unread', false, 'msg-unread')
  const rows: Row[] = [
    { step_index: 1, type: 'USER_INPUT', content: 'old' },
    message(2, child, 'old'),
    done(3, 'done'),
  ]
  await f.put(parent, rows)

  const epoch1 = await captureRuntimeEpoch(f.root, parent)
  assert.ok(epoch1)
  assert.equal(epoch1.afterStepIndex, 3)

  await f.rewrite(parent, rows, async () => {
    const duringTruncate = await captureRuntimeEpoch(f.root, parent, {
      minAfterStep: epoch1.afterStepIndex,
      previous: epoch1,
    })
    assert.ok(duringTruncate)
    assert.equal(duringTruncate.afterStepIndex, 3)
    assert.equal(duringTruncate.preEpochReadIds.has('msg-read'), true)
    assert.equal(duringTruncate.ignoredMessages.has('msg-unread'), true)
    assert.equal(duringTruncate.ignoredSubagentIds.has(child), true)
  })
})

test('no-LF pre-epoch SYSTEM_MESSAGE completed later stays fenced and does not skew post-epoch wakeups', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  await f.deliver(parent, child, 'late', true, 'msg-late')
  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'old' },
    done(2, 'old'),
  ])
  await appendFile(f.path(parent), JSON.stringify(message(3, child, 'late')))

  const epoch = await captureRuntimeEpoch(f.root, parent)
  assert.ok(epoch)
  assert.equal(epoch.afterStepIndex, 3)
  assert.equal(epoch.preEpochReadIds.has('msg-late'), true)

  await appendFile(
    f.path(parent),
    '\n' +
      encode([
        { step_index: 4, type: 'USER_INPUT', content: 'next' },
        done(5, 'next done'),
      ]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  const settled = await alive(
    settleBrainTurn(f.root, parent, 'p-nolf', () => {}, {
      pollIntervalMs: 5,
      stallTimeoutMs: 50,
      runtimeEpoch: epoch,
    }),
  )
  assert.equal(settled.status, 'completed')

  await f.deliver(parent, child, 'after', true, 'msg-after')
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    true,
  )
  await appendFile(
    f.path(parent),
    encode([message(6, child, 'after'), done(7, 'after done')]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
})

class GatedChildReader extends TranscriptReader {
  private readonly shouldGate: (nativeId: string) => boolean
  private readonly onEnter: () => void
  private readonly gate: Promise<void>

  constructor(
    shouldGate: (nativeId: string) => boolean,
    onEnter: () => void,
    gate: Promise<void>,
  ) {
    super()
    this.shouldGate = shouldGate
    this.onEnter = onEnter
    this.gate = gate
  }

  override async read(root: string, nativeId: string) {
    if (this.shouldGate(nativeId)) {
      this.onEnter()
      await this.gate
    }
    return super.read(root, nativeId)
  }
}

function createGatedChildReader(targetId: string) {
  let armed = false
  let resolveEntered!: () => void
  const entered = new Promise<void>((resolve) => {
    resolveEntered = resolve
  })
  let releaseGate!: () => void
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve
  })
  return {
    arm() {
      armed = true
    },
    entered,
    release() {
      releaseGate()
    },
    createTranscriptReader(): TranscriptReader {
      return new GatedChildReader(
        (id) => {
          if (!armed || id !== targetId) return false
          armed = false
          return true
        },
        resolveEntered,
        gate,
      )
    },
  }
}

test(
  'forceStopAll after slow child cancel unblocks SubagentRegistry and isolates later child turns from the stale cancelAll loop',
  { timeout: 10_000 },
  async (t) => {
    const f = await fixture()
    const collector = createEventCollector()
    const readerGate = createGatedChildReader(child)
    const child2 = '44444444-4444-4444-8444-444444444444'
    const registry = new SubagentRegistry({
      sessionId: 'p',
      cwd: f.root,
      brainRoot: f.root,
      capabilities: ['session.subsession'],
      emit: collector.emit,
      getNativeId: () => parent,
      isParentTurnActive: () => true,
      isLoading: () => false,
      isUnavailable: () => false,
      createTranscriptReader: readerGate.createTranscriptReader,
    })
    t.after(async () => {
      readerGate.release()
      registry.closeAll()
      await f.close()
    })

    await f.put(child, [
      message(0, parent, 'work'),
      { step_index: 1, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    registry.linkSubsession('p', parent, {
      type: 'tool_call',
      id: 'spawn-1',
      callId: 'spawn-1',
      name: 'invoke_subagent',
      status: 'completed',
      error: null,
      detail: { type: 'sub_agent', childSessionId: child, log: '' },
    })
    await registry.waitCurrentReady()
    assert.equal(registry.hasRunningChildren(), true)

    readerGate.arm()
    const staleCancel = registry.cancelAll()
    await readerGate.entered
    registry.forceStopAll()

    const msgItem: ProviderTimelineItem = {
      type: 'tool_call',
      id: 'msg-child2',
      callId: 'msg-child2',
      name: 'send_message',
      status: 'completed',
      error: null,
      detail: { type: 'sub_agent', childSessionId: child2, log: '' },
    }
    await f.subagent(parent, child2)
    assert.equal(await registry.prepareMessageItem(msgItem), true)
    assert.deepEqual(msgItem.detail, {
      type: 'sub_agent',
      childSessionId: child2,
      log: '',
    })

    await f.put(child2, [
      message(0, parent, 'work 2'),
      { step_index: 1, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    registry.linkSubsession('p', parent, msgItem)
    await registry.waitCurrentReady()
    assert.equal(registry.hasChild(child2), true)
    assert.equal(registry.hasRunningChildren(), true)

    readerGate.release()
    await staleCancel
    assert.equal(registry.hasRunningChildren(), true)

    await appendFile(f.path(child2), encode([done(2, 'child2 finished')]))
    await collector.wait(
      (event) =>
        event.type === 'session.turn' &&
        event.sessionId === `p:child:${child2}` &&
        event.state === 'completed',
    )
    assert.equal(
      collector.events.some(
        (event) =>
          event.type === 'session.turn' &&
          event.sessionId === `p:child:${child2}` &&
          event.state === 'canceled',
      ),
      false,
    )
  },
)

type RecordedFileRead = {
  position: number | null
  length: number
  bytesRead: number
}

type RecordedTranscriptPass = {
  reset: boolean
  stepIndices: number[]
  fileReads: RecordedFileRead[]
}

function createRecordingReaderFactory() {
  const passes: RecordedTranscriptPass[] = []
  let created = 0
  let activeReads: RecordedFileRead[] = []
  const recordingOpen: typeof open = async (path, flags, mode) => {
    const handle = await open(path, flags, mode)
    return {
      stat: () => handle.stat(),
      close: () => handle.close(),
      read: async (
        buffer: Buffer,
        offset: number,
        length: number,
        position: number | null,
      ) => {
        const result = await handle.read(buffer, offset, length, position)
        activeReads.push({ position, length, bytesRead: result.bytesRead })
        return result
      },
    } as unknown as Awaited<ReturnType<typeof open>>
  }
  class RecordingTranscriptReader extends TranscriptReader {
    constructor() {
      super(recordingOpen)
      created++
    }
    override async read(root: string, nativeId: string) {
      const fileReads: RecordedFileRead[] = []
      activeReads = fileReads
      const result = await super.read(root, nativeId)
      passes.push({
        reset: result.reset,
        stepIndices: result.rows.map((row) => Number(row.step_index)),
        fileReads,
      })
      return result
    }
  }
  return {
    passes,
    createdCount: () => created,
    createReader: () => new RecordingTranscriptReader(),
  }
}

test('BackgroundWorkProbe reads incrementally across appends and tracks background task lifecycle', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const spy = createRecordingReaderFactory()
  const probe = new BackgroundWorkProbe(spy.createReader)
  const taskId = `${parent}/task-bg-1`

  const initialRows: Row[] = [
    { step_index: 0, type: 'USER_INPUT', content: 'x'.repeat(16_384) },
    done(1, 'idle'),
  ]
  const initialSize = Buffer.byteLength(encode(initialRows))
  await f.put(parent, initialRows)

  assert.equal(await probe.hasActiveWork(f.root, parent), false)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes.length, 1)
  assert.equal(spy.passes[0].reset, true)
  assert.deepEqual(spy.passes[0].stepIndices, [0, 1])
  assert.deepEqual(spy.passes[0].fileReads, [
    { position: 0, length: initialSize, bytesRead: initialSize },
  ])

  const startTaskBytes = encode([
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-1',
          name: 'run_command',
          args: { CommandLine: 'sleep 30' },
        },
      ],
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${taskId}\n`,
    },
  ])
  const startTaskSize = Buffer.byteLength(startTaskBytes)
  await appendFile(f.path(parent), startTaskBytes)

  assert.equal(await probe.hasActiveWork(f.root, parent), true)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes.length, 2)
  assert.equal(spy.passes[1].reset, false)
  assert.deepEqual(spy.passes[1].stepIndices, [2, 3])
  assert.deepEqual(spy.passes[1].fileReads.at(-1), {
    position: initialSize,
    length: startTaskSize,
    bytesRead: startTaskSize,
  })

  const finishTaskBytes = encode([
    {
      step_index: 4,
      type: 'SYSTEM_MESSAGE',
      content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-08T17:00:00Z sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nCommand exited with code 0.\n</SYSTEM_MESSAGE>`,
    },
    done(5, 'all done'),
  ])
  const finishTaskSize = Buffer.byteLength(finishTaskBytes)
  await appendFile(f.path(parent), finishTaskBytes)

  assert.equal(await probe.hasActiveWork(f.root, parent), false)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes.length, 3)
  assert.equal(spy.passes[2].reset, false)
  assert.deepEqual(spy.passes[2].stepIndices, [4, 5])
  assert.deepEqual(spy.passes[2].fileReads.at(-1), {
    position: initialSize + startTaskSize,
    length: finishTaskSize,
    bytesRead: finishTaskSize,
  })
})

test('BackgroundWorkProbe respects includeDaemons, recovers from O_TRUNC rewrites, and resets on runtimeEpoch change', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const spy = createRecordingReaderFactory()
  const probe = new BackgroundWorkProbe(spy.createReader)
  const daemonTaskId = `${parent}/task-daemon`
  const rewriteTaskId = `${parent}/task-rewrite`

  await f.put(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'schedule cron' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'cron-1',
          name: 'schedule',
          args: {
            CronExpression: '*/5 * * * *',
            IsDaemon: true,
            Prompt: 'tick',
          },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${daemonTaskId}\n`,
    },
    done(3, 'scheduled'),
  ])

  assert.equal(
    await probe.hasActiveWork(f.root, parent, { includeDaemons: false }),
    false,
  )
  assert.equal(
    await probe.hasActiveWork(f.root, parent, { includeDaemons: true }),
    true,
  )
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes[1].reset, false)
  assert.deepEqual(spy.passes[1].stepIndices, [])

  // In-place O_TRUNC rewrite replacing the daemon with a non-daemon background task.
  await f.rewrite(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'rewritten' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-rw',
          name: 'run_command',
          args: { CommandLine: 'sleep 10' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${rewriteTaskId}\n`,
    },
  ])

  assert.equal(await probe.hasActiveWork(f.root, parent), true)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes[2].reset, true)
  assert.deepEqual(spy.passes[2].stepIndices, [0, 1, 2])

  // Capturing a new runtimeEpoch fences the pre-epoch background task and resets probe state.
  const epoch = await captureRuntimeEpoch(f.root, parent)
  assert.ok(epoch)
  assert.equal(
    await probe.hasActiveWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  assert.equal(spy.createdCount(), 2)
  assert.equal(spy.passes[3].reset, true)

  // Post-epoch unread/read wakeup tracking works incrementally and survives O_TRUNC under the same epoch.
  await f.deliver(parent, child, 'fresh', true, 'msg-fresh')
  assert.equal(
    await probe.hasActiveWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    true,
  )
  assert.equal(spy.createdCount(), 2)
  assert.equal(spy.passes[4].reset, false)

  await appendFile(
    f.path(parent),
    encode([message(3, child, 'fresh'), done(4, 'handled')]),
  )
  assert.equal(
    await probe.hasActiveWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  assert.equal(spy.createdCount(), 2)
  assert.equal(spy.passes[5].reset, false)
  assert.deepEqual(spy.passes[5].stepIndices, [3, 4])

  await f.rewrite(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'rewritten' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-rw',
          name: 'run_command',
          args: { CommandLine: 'sleep 10' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${rewriteTaskId}\n`,
    },
    message(3, child, 'fresh'),
    done(4, 'handled after rewrite'),
  ])
  assert.equal(
    await probe.hasActiveWork(f.root, parent, {
      includeDaemons: true,
      runtimeEpoch: epoch,
    }),
    false,
  )
  assert.equal(spy.createdCount(), 2)
  assert.equal(spy.passes[6].reset, true)
})

test('BackgroundWorkProbe.sync warms the cursor and serializes concurrent calls on the lane', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const spy = createRecordingReaderFactory()
  const probe = new BackgroundWorkProbe(spy.createReader)
  const taskId = `${parent}/task-sync`

  const initialRows: Row[] = [
    { step_index: 0, type: 'USER_INPUT', content: 'y'.repeat(16_384) },
    done(1, 'warm'),
  ]
  const initialSize = Buffer.byteLength(encode(initialRows))
  await f.put(parent, initialRows)

  await probe.sync(f.root, parent)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes.length, 1)
  assert.equal(spy.passes[0].reset, true)
  assert.deepEqual(spy.passes[0].stepIndices, [0, 1])

  assert.equal(await probe.hasActiveWork(f.root, parent), false)
  assert.equal(spy.createdCount(), 1)
  assert.equal(spy.passes.length, 2)
  assert.equal(spy.passes[1].reset, false)
  assert.deepEqual(spy.passes[1].stepIndices, [])
  const probeBytesRead = spy.passes[1].fileReads.reduce(
    (sum, r) => sum + r.bytesRead,
    0,
  )
  assert.ok(
    probeBytesRead < initialSize,
    `expected warmed hasActiveWork to avoid rescanning full ${initialSize}-byte transcript (read ${probeBytesRead})`,
  )

  // Concurrent sync() + hasActiveWork() serialize without interleaving on the shared reader.
  const readerGate = createGatedChildReader(parent)
  let inFlightReads = 0
  let maxConcurrentReads = 0
  const serializedOrder: string[] = []
  const gatedProbe = new BackgroundWorkProbe(() => {
    const inner = readerGate.createTranscriptReader()
    return new (class extends TranscriptReader {
      override async read(root: string, nativeId: string) {
        inFlightReads++
        maxConcurrentReads = Math.max(maxConcurrentReads, inFlightReads)
        try {
          return await inner.read(root, nativeId)
        } finally {
          inFlightReads--
        }
      }
    })()
  })
  t.after(() => readerGate.release())
  readerGate.arm()
  const syncPromise = gatedProbe
    .sync(f.root, parent)
    .then(() => serializedOrder.push('sync'))
  await readerGate.entered
  await appendFile(
    f.path(parent),
    encode([
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'bg-sync',
            name: 'run_command',
            args: { CommandLine: 'sleep 20' },
          },
        ],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${taskId}\n`,
      },
    ]),
  )
  const activePromise = gatedProbe.hasActiveWork(f.root, parent).then((res) => {
    serializedOrder.push('hasActiveWork')
    return res
  })
  readerGate.release()
  const [, active] = await Promise.all([syncPromise, activePromise])
  assert.equal(active, true)
  assert.equal(maxConcurrentReads, 1)
  assert.deepEqual(serializedOrder, ['sync', 'hasActiveWork'])
})

test('BackgroundWorkProbe fails closed on corrupt appended JSONL and recovers from raced empty-reset rewrites', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const parent = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'
  const taskId = 'bg-corrupt-guard'

  // 1. Corrupt appended JSONL line on a warm probe must reject (fail closed) rather than resetting node to false.
  const corruptProbe = new BackgroundWorkProbe()
  await f.put(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'start' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-1',
          name: 'run_command',
          args: { CommandLine: 'sleep 30' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${taskId}\n`,
    },
  ])
  assert.equal(await corruptProbe.hasActiveWork(f.root, parent), true)

  await appendFile(f.path(parent), '{"step_index":3,"type":"broken"\n')
  await assert.rejects(corruptProbe.hasActiveWork(f.root, parent), SyntaxError)

  // 2. Raced mid-O_TRUNC rewrite returning { rows: [], reset: true } on first read retries once and ingests replacement rows.
  const replacementRows = [
    { step_index: 0, type: 'USER_INPUT', content: 'rewritten' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'bg-2',
          name: 'run_command',
          args: { CommandLine: 'sleep 30' },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${taskId}\n`,
    },
  ]
  const readResponses: Array<Awaited<ReturnType<TranscriptReader['read']>>> = [
    {
      rows: [
        { step_index: 0, type: 'USER_INPUT', content: 'initial' },
        done(1, 'idle'),
      ],
      reset: true,
      mtimeMs: T0,
    },
    { rows: [], reset: true, mtimeMs: T0 },
    { rows: replacementRows, reset: true, mtimeMs: T0 },
  ]
  const racedProbe = new BackgroundWorkProbe(
    () =>
      new (class extends TranscriptReader {
        override async read() {
          return readResponses.shift()!
        }
      })(),
  )
  assert.equal(await racedProbe.hasActiveWork(f.root, parent), false)
  assert.equal(await racedProbe.hasActiveWork(f.root, parent), true)
  assert.equal(readResponses.length, 0)
})

test('schedule with IsDaemon: true is a standing daemon while non-daemon CronExpression stays blocking and counts down MaxIterations', async (t) => {
  const f = await fixture()
  t.after(() => f.close())

  // 1. Recurring CronExpression with IsDaemon: true settles immediately, reports daemon-only work, and decrements on MaxIterations.
  const cronDaemonId = `${parent}/task-cron-daemon`
  await f.put(parent, [
    { step_index: 0, type: 'USER_INPUT', content: 'start daemon cron' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'cron-daemon',
          name: 'schedule',
          args: {
            CronExpression: '*/5 * * * *',
            IsDaemon: true,
            Prompt: 'poll status',
            MaxIterations: 2,
          },
        },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${cronDaemonId}\n`,
    },
    done(3, 'Daemon cron scheduled.'),
  ])

  assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
    true,
  )
  const settledDaemon = await settleBrainTurn(f.root, parent, 'p', () => {}, {
    minStepIndex: -1,
  })
  assert.equal(settledDaemon.status, 'completed')

  await appendFile(
    f.path(parent),
    encode([message(4, cronDaemonId, 'poll status'), done(5, 'Tick #1')]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
    true,
  )

  await appendFile(
    f.path(parent),
    encode([message(6, cronDaemonId, 'poll status'), done(7, 'Tick #2')]),
  )
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
    false,
  )

  // 2. Non-daemon CronExpression (IsDaemon omitted) stays blocking in background across Tick #1 and clears after Tick #2.
  const cronOmittedId = `${parent}/task-cron-omitted`
  await appendFile(
    f.path(parent),
    encode([
      { step_index: 8, type: 'USER_INPUT', content: 'start non-daemon cron' },
      {
        step_index: 9,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'cron-omitted',
            name: 'schedule',
            args: {
              CronExpression: '*/5 * * * *',
              Prompt: 'poll status',
              MaxIterations: 2,
              TimerCondition: 'any',
            },
          },
        ],
      },
      {
        step_index: 10,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${cronOmittedId}\n`,
      },
      done(11, 'Blocking cron scheduled.'),
    ]),
  )
  assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

  // Unrelated sender does not early-cancel a cron, and Tick #1 decrements remainingIterations (2 -> 1) without deleting it.
  await appendFile(
    f.path(parent),
    encode([
      message(12, 'unrelated-sender', 'hello'),
      message(13, cronOmittedId, 'poll status'),
      done(14, 'Tick #1'),
    ]),
  )
  assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

  // Tick #2 reaches MaxIterations (1 -> deleted), allowing the turn to settle cleanly.
  await appendFile(
    f.path(parent),
    encode([message(15, cronOmittedId, 'poll status'), done(16, 'Tick #2')]),
  )
  assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  const settledOmitted = await settleBrainTurn(f.root, parent, 'p', () => {}, {
    minStepIndex: 7,
  })
  assert.equal(settledOmitted.status, 'completed')

  // 3. Unlimited non-daemon CronExpression (IsDaemon: false) stays blocking across ticks until manage_task(kill).
  const cronFalseId = `${parent}/task-cron-false`
  await appendFile(
    f.path(parent),
    encode([
      { step_index: 17, type: 'USER_INPUT', content: 'start unlimited cron' },
      {
        step_index: 18,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'cron-false',
            name: 'schedule',
            args: {
              CronExpression: '0 * * * *',
              IsDaemon: false,
              Prompt: 'hourly check',
            },
          },
        ],
      },
      {
        step_index: 19,
        type: 'GENERIC',
        status: 'RUNNING',
        content: `Tool is running as a background task with task id: ${cronFalseId}\n`,
      },
      done(20, 'Hourly cron scheduled.'),
      message(21, cronFalseId, 'hourly check'),
      done(22, 'Hourly tick #1'),
    ]),
  )
  assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

  await appendFile(
    f.path(parent),
    encode([
      { step_index: 23, type: 'USER_INPUT', content: 'stop hourly cron' },
      {
        step_index: 24,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'kill-cron',
            name: 'manage_task',
            args: { Action: 'kill', TaskId: cronFalseId },
          },
        ],
      },
      {
        step_index: 25,
        type: 'GENERIC',
        status: 'DONE',
        content: `Killed task ${cronFalseId}`,
      },
      done(26, 'Stopped hourly cron.'),
    ]),
  )
  assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  assert.equal(
    await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
    false,
  )

  // 4. Whitespace-only CronExpression and one-shot DurationSeconds timer register as blocking background tasks and clear on tick or TimerCondition.
  const blankCronProjector = new BrainTranscriptProjector(parent)
  blankCronProjector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      {
        id: 'cron-blank',
        name: 'schedule',
        args: { CronExpression: '   ', Prompt: 'not a cron' },
      },
    ],
  })
  blankCronProjector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${parent}/task-blank\n`,
  })
  assert.equal(blankCronProjector.hasBackground(), true)
  assert.equal(blankCronProjector.hasDaemons(), false)

  const timerProjector = new BrainTranscriptProjector(parent)
  timerProjector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      {
        id: 'oneshot-timer',
        name: 'schedule',
        args: {
          DurationSeconds: 60,
          Prompt: 'wake up once',
          TimerCondition: 'any',
        },
      },
    ],
  })
  timerProjector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${parent}/task-timer\n`,
  })
  assert.equal(timerProjector.hasBackground(), true)
  assert.equal(timerProjector.hasDaemons(), false)
  const canceledTimerItems = timerProjector.project(
    message(3, 'other-agent', 'wake early'),
  )
  assert.equal(timerProjector.hasBackground(), false)
  assert.equal(canceledTimerItems[0]?.type, 'tool_call')
  assert.equal(
    canceledTimerItems[0]?.type === 'tool_call' && canceledTimerItems[0].status,
    'canceled',
  )
})

test('isRestartNotice matches background-only recovery variant and clears background and daemon tasks', () => {
  const bgOnlyNotice = message(
    4,
    'system',
    '[Notice] All your background tasks have been stopped due to server restart.',
  )
  assert.equal(isRestartNotice(bgOnlyNotice), true)
  assert.equal(
    isRestartNotice(
      message(
        4,
        child,
        '[Notice] All your background tasks have been stopped due to server restart.',
      ),
    ),
    false,
  )

  const projector = new BrainTranscriptProjector(parent)
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      { id: 'bg-1', name: 'run_command', args: { CommandLine: 'watch' } },
      {
        id: 'daemon-1',
        name: 'run_command',
        args: { CommandLine: 'serve', IsDaemon: true },
      },
    ],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${parent}/task-bg\n`,
  })
  projector.project({
    step_index: 3,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${parent}/task-daemon\n`,
  })
  assert.equal(projector.hasBackground(), true)
  assert.equal(projector.hasDaemons(), true)
  projector.project(bgOnlyNotice)
  assert.equal(projector.hasBackground(), false)
  assert.equal(projector.hasDaemons(), false)
})

test('1.5.16+ intra-RPC wakeup with NotificationTimeoutSeconds, Brain output recovery, and silent trailing PLANNER_RESPONSE avoids double-counting', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const taskId = `${parent}/task-notify`

  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'run build' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'cmd-1',
          name: 'run_command',
          args: { CommandLine: 'build', NotificationTimeoutSeconds: 30 },
        },
      ],
      input_tokens: 100,
      output_tokens: 10,
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'RUNNING',
      content: `Tool is running as a background task with task id: ${taskId}\n`,
    },
    { ...done(4, 'Started build.'), input_tokens: 120, output_tokens: 5 },
    message(5, taskId, 'Background command is still running after 30s: build'),
    { ...done(6, 'Still running.'), input_tokens: 140, output_tokens: 5 },
  ])
  // NotificationTimeoutSeconds progress notice must not clear the running background task.
  assert.equal(await hasActiveBackgroundWork(f.root, parent), true)

  await appendFile(
    f.path(parent),
    encode([
      message(7, taskId, `Task id "${taskId}" finished with result:\nok`),
      {
        step_index: 8,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'read-1',
            name: 'read_file',
            args: { Path: '/tmp/report.txt' },
          },
        ],
        input_tokens: 160,
        output_tokens: 8,
      },
      {
        step_index: 9,
        type: 'GENERIC',
        status: 'DONE',
        content: 'report body',
      },
      {
        step_index: 10,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        input_tokens: 180,
        output_tokens: 2,
      },
    ]),
  )
  const foregroundAcp: ForegroundAcpSegment[] = [
    { type: 'tool_call', callId: 'cmd-1' },
    { type: 'assistant_message', text: 'Started build.' },
    { type: 'assistant_message', text: 'Still running.' },
    { type: 'tool_call', callId: 'read-1' },
  ]
  const snapshots = new Map([
    [
      'cmd-1',
      {
        toolCallId: 'cmd-1',
        title: 'run_command',
        rawInput: { CommandLine: 'build', NotificationTimeoutSeconds: 30 },
      },
    ],
    [
      'read-1',
      {
        toolCallId: 'read-1',
        title: 'read_file',
        rawInput: { Path: '/tmp/report.txt' },
      },
    ],
  ])
  const intraItems: ProviderTimelineItem[] = []
  const intra = await settleBrainTurn(
    f.root,
    parent,
    'p',
    (item) => intraItems.push(item),
    {
      minStepIndex: 0,
      acpToolCallIds: ['cmd-1', 'read-1'],
      foregroundAcp,
      hasPostRpcAcp: () => false,
      toolSnapshot: (id) => snapshots.get(id),
    },
  )
  assert.equal(intra.status, 'completed')
  assert.equal(intra.wokeUp, undefined)
  assert.equal(intra.wakeupUsage, undefined)
  const completedCmd = intraItems.find(
    (item) => item.type === 'tool_call' && item.callId === 'cmd-1',
  )
  assert.ok(
    completedCmd?.type === 'tool_call' &&
      completedCmd.status === 'completed' &&
      completedCmd.detail.type === 'shell',
  )
  assert.equal(completedCmd.detail.output, 'ok')
  const recoveredRead = intraItems.find(
    (item) => item.type === 'tool_call' && item.callId === 'read-1',
  )
  assert.ok(
    recoveredRead?.type === 'tool_call' &&
      recoveredRead.status === 'completed' &&
      recoveredRead.detail.type === 'read',
  )
  assert.equal(recoveredRead.detail.content, 'report body')

  // When post-RPC ACP activity was observed, a silent trailing step is still billed as post-RPC wakeup.
  const post = await settleBrainTurn(f.root, parent, 'p', () => {}, {
    minStepIndex: 0,
    acpToolCallIds: ['cmd-1', 'read-1'],
    foregroundAcp,
    hasPostRpcAcp: () => true,
    toolSnapshot: (id) => snapshots.get(id),
  })
  assert.equal(post.status, 'completed')
  assert.equal(post.wokeUp, true)
  assert.deepEqual(post.wakeupUsage, {
    inputTokens: 180,
    cachedReadTokens: 0,
    outputTokens: 2,
  })
})

test('settleBrainTurn resets stall watchdog after a host-sleep clock jump and still stalls on normal increments', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })

  const items: ProviderTimelineItem[] = []
  const obs = new BrainTurnObservation(
    f.root,
    parent,
    'p',
    -1,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(item),
    },
    200,
  )
  const abort = new AbortController()
  t.after(async () => {
    abort.abort()
    await obs.dispose()
  })

  await f.put(parent, [
    { step_index: 1, type: 'USER_INPUT', content: 'run' },
    { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
  ])

  const steps = createStepTracker()
  const step2Seen = steps.waitFor(2)
  let settled = false
  const running = alive(
    settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
      signal: abort.signal,
      observation: obs,
      pollIntervalMs: 200,
      stallTimeoutMs: 12_000,
      onStep: steps.onStep,
    }),
  ).then((result) => {
    settled = true
    return result
  })
  t.after(async () => {
    abort.abort()
    await obs.dispose()
    await running.catch(() => undefined)
  })

  await step2Seen
  assert.equal(settled, false)

  // Single wall-clock jump > expectedPollMs (200) + 10_000 exceeds stallTimeoutMs (12_000) but resets lastActivity.
  t.mock.timers.tick(15_200)
  await flushObs(obs)
  assert.equal(
    settled,
    false,
    'wall-clock jump beyond poll interval + 10s must reset stall watchdog instead of failing',
  )

  // Advancing in normal increments (< 10_200ms each) past stallTimeoutMs (12_000ms) stalls as expected.
  t.mock.timers.tick(5_000)
  await flushObs(obs)
  assert.equal(settled, false)

  t.mock.timers.tick(5_000)
  await flushObs(obs)
  assert.equal(settled, false)

  t.mock.timers.tick(2_000)
  await flushObs(obs)
  const result = await running
  assert.equal(result.status, 'failed')
  assert.equal(result.stalled, true)
  assert.match(String(result.error), /idle for 12s without a final response/)
  assert.deepEqual(items.map(errorIdOrType), ['p:step:2:stalled'])
})

test('daemon run_command completes its tool card on SYSTEM_MESSAGE result and multiline headers cannot match tasks', () => {
  const projector = new BrainTranscriptProjector(parent)
  const daemonTaskId = `${parent}/task-daemon`
  const timerTaskId = `${parent}/task-timer`

  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [
      {
        id: 'daemon-cmd',
        name: 'run_command',
        args: { CommandLine: 'serve --port 8080', IsDaemon: true },
      },
      {
        id: 'timer-1',
        name: 'schedule',
        args: {
          DurationSeconds: 60,
          TimerCondition: 'peer-1',
          Prompt: 'check',
        },
      },
    ],
  })
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${daemonTaskId}\n`,
  })
  projector.project({
    step_index: 3,
    type: 'GENERIC',
    status: 'RUNNING',
    content: `Tool is running as a background task with task id: ${timerTaskId}\n`,
  })
  assert.equal(projector.hasDaemons(), true)
  assert.equal(projector.hasBackground(), true)

  // Multiline body containing `[Message]\n...sender=peer-1` must not match as a single-line header.
  const ignored = projector.project({
    step_index: 4,
    type: 'SYSTEM_MESSAGE',
    content:
      '<SYSTEM_MESSAGE>\n[Message]\ntimestamp=2026-11-26T10:00:00Z sender=peer-1 content=not a header\n</SYSTEM_MESSAGE>',
  })
  assert.deepEqual(ignored, [])
  assert.equal(projector.hasBackground(), true)
  assert.equal(projector.hasDaemons(), true)

  const finished = projector.project({
    step_index: 5,
    type: 'SYSTEM_MESSAGE',
    content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-26T10:00:05Z sender=${daemonTaskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${daemonTaskId}" finished with result:\nCommand exited with code 0.\n\nCommand output:\nstopped cleanly\n</SYSTEM_MESSAGE>`,
  })
  assert.equal(projector.hasDaemons(), false)
  assert.equal(projector.hasBackground(), true)
  assert.equal(finished.length, 1)
  assert.equal(finished[0].type, 'tool_call')
  assert.equal(finished[0].callId, 'daemon-cmd')
  assert.equal(finished[0].status, 'completed')
  assert.deepEqual(finished[0].detail, {
    type: 'shell',
    command: 'serve --port 8080',
    cwd: undefined,
    output: 'Command exited with code 0.\n\nCommand output:\nstopped cleanly',
    exitCode: 0,
  })
})

test('readSubagents recovers spawn metadata after a transiently torn invoke_subagent message file', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  const childId = randomUUID()
  const subDir = join(f.root, parent, '.system_generated/subagents')
  await writeFileAtomic(
    join(subDir, `${childId}.json`),
    JSON.stringify({
      conversationId: childId,
      spawnStepIndex: 0,
      state: 'SUBAGENT_STATE_ALIVE',
    }),
  )

  const msgDir = join(f.root, childId, '.system_generated/messages')
  await mkdir(msgDir, { recursive: true })
  await f.deliver(childId, parent, 'later wakeup', false, 'wake-1', {
    conversationId: parent,
    stepIndex: 9,
    callId: 'send-call',
    name: 'send_message',
  })
  await writeFile(join(msgDir, 'init-1.json'), '{')

  const [before] = await readSubagents(f.root, parent)
  assert.equal(before.conversationId, childId)
  assert.equal(before.spawnCallId, undefined)
  assert.equal(before.spawnStepIndex, 0)

  await f.deliver(childId, parent, 'spawn task', true, 'init-1', {
    conversationId: parent,
    stepIndex: 4,
    callId: 'spawn-call-1',
    name: 'invoke_subagent',
  })

  const [after] = await readSubagents(f.root, parent)
  assert.equal(after.conversationId, childId)
  assert.equal(after.spawnCallId, 'spawn-call-1')
  assert.equal(after.spawnStepIndex, 4)
})

test('settleBrainTurn surfaces hidden SQLite errors immediately without stalling across USER_INPUT, GENERIC DONE, and retryPending', async (t) => {
  const f = await fixture()
  t.after(() => f.close())

  // 1. Hidden 404 right after USER_INPUT fails immediately without stalled: true.
  const s1 = randomUUID()
  await f.put(s1, [{ step_index: 1, type: 'USER_INPUT', content: 'hi' }])
  const res1 = await settleBrainTurn(f.root, s1, 's1', () => {}, {
    pollIntervalMs: 5,
    stallTimeoutMs: 120_000,
    readHiddenError: async () =>
      'NOT_FOUND (404): Requested entity was not found.',
  })
  assert.equal(res1.status, 'failed')
  assert.equal(res1.stalled, undefined)
  assert.equal(res1.error, 'NOT_FOUND (404): Requested entity was not found.')

  // 2. Hidden 401 after GENERIC DONE fails immediately without stalled: true.
  const s2 = randomUUID()
  await f.put(s2, [
    { step_index: 1, type: 'USER_INPUT', content: 'run' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ id: 'c1', name: 'run_command', args: {} }],
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'ok' },
  ])
  const res2 = await settleBrainTurn(f.root, s2, 's2', () => {}, {
    pollIntervalMs: 5,
    stallTimeoutMs: 120_000,
    readHiddenError: async () => 'UNAUTHENTICATED (401): invalid credentials',
  })
  assert.equal(res2.status, 'failed')
  assert.equal(res2.stalled, undefined)
  assert.equal(res2.error, 'UNAUTHENTICATED (401): invalid credentials')

  // 3. Hidden terminal error arriving while retryPending is active fails immediately instead of waiting retryTimeoutMs.
  const s3 = randomUUID()
  await f.put(s3, [
    { step_index: 1, type: 'USER_INPUT', content: 'retry me' },
    {
      step_index: 2,
      source: 'SYSTEM',
      type: 'ERROR_MESSAGE',
      status: 'DONE',
      error: 'API error (attempt 8): request failed: EOF',
    },
  ])
  const res3 = await settleBrainTurn(f.root, s3, 's3', () => {}, {
    pollIntervalMs: 5,
    retryTimeoutMs: 15 * 60_000,
    readHiddenError: async () =>
      'agent executor error: generating and executing: request failed: EOF',
  })
  assert.equal(res3.status, 'failed')
  assert.equal(res3.stalled, undefined)
  assert.equal(
    res3.error,
    'agent executor error: generating and executing: request failed: EOF',
  )
})

test('settleBrainTurn ignores stale readHiddenError when transcript settles mid-check and still stalls on genuine idle', async (t) => {
  const f = await fixture()
  t.after(() => f.close())
  t.mock.timers.enable({ apis: ['Date'], now: 1_000_000 })

  // 1. Transcript settles while readHiddenError is in flight: foldVersion invalidates the stale check.
  const sRace = randomUUID()
  await f.put(sRace, [{ step_index: 1, type: 'USER_INPUT', content: 'race' }])
  const obsRace = new BrainTurnObservation(
    f.root,
    sRace,
    'race',
    -1,
    { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
    60_000,
  )
  t.after(() => obsRace.dispose())
  let releaseHidden!: (value: string | undefined) => void
  let markEntered!: () => void
  const hiddenStarted = new Promise<void>((resolve) => {
    markEntered = resolve
  })
  const racePromise = settleBrainTurn(f.root, sRace, 'race', () => {}, {
    observation: obsRace,
    stallTimeoutMs: 120_000,
    readHiddenError: () => {
      markEntered()
      return new Promise<string | undefined>((resolve) => {
        releaseHidden = resolve
      })
    },
  })
  await hiddenStarted
  await f.put(sRace, [
    { step_index: 1, type: 'USER_INPUT', content: 'race' },
    done(2, 'actually completed'),
  ])
  await flushObs(obsRace)
  releaseHidden('NOT_FOUND (404): stale error')
  const raceResult = await racePromise
  assert.equal(raceResult.status, 'completed')

  // 2. Genuine stall with no hidden error still triggers final readHiddenError check and stalls cleanly.
  const sStall = randomUUID()
  await f.put(sStall, [{ step_index: 1, type: 'USER_INPUT', content: 'hang' }])
  const obsStall = new BrainTurnObservation(
    f.root,
    sStall,
    'stall',
    -1,
    { isCurrent: () => true, onUsage: () => {}, onItem: () => {} },
    60_000,
  )
  t.after(() => obsStall.dispose())
  let hiddenCalls = 0
  const stallPromise = settleBrainTurn(f.root, sStall, 'stall', () => {}, {
    observation: obsStall,
    stallTimeoutMs: 500,
    readHiddenError: async () => {
      hiddenCalls += 1
      return undefined
    },
  })
  await flushObs(obsStall)
  assert.equal(hiddenCalls, 1)
  t.mock.timers.tick(500)
  await flushObs(obsStall)
  const stallResult = await stallPromise
  assert.equal(stallResult.status, 'failed')
  assert.equal(stallResult.stalled, true)
  assert.equal(hiddenCalls, 2)
})
