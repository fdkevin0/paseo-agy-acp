import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import {
  mkdtemp,
  mkdir,
  open,
  readFile,
  writeFile,
  appendFile,
  rename,
  rm,
  symlink,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import {
  advanceForegroundCursor,
  BrainNode,
  BrainTranscriptProjector,
  countRowMessages,
  hasActiveBackgroundWork,
  readInboxState,
  readLastStepIndex,
  readSubagents,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
} from '../server/brain.ts'
import { waitUntil } from './helpers.ts'
import { SubagentRegistry } from '../server/subagents.ts'
import {
  readTurnBoundaryBefore,
  resolveChildTurnState,
  type TurnBoundaryCache,
} from '../server/brain-turns.ts'
import type {
  ProviderEvent,
  ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'

const parent = '11111111-1111-4111-8111-111111111111'
const child = '22222222-2222-4222-8222-222222222222'
const grandchild = '33333333-3333-4333-8333-333333333333'
type Row = Record<string, unknown>
const done = (step_index: number, content = 'finished'): Row => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  content,
})
const message = (step_index: number, sender: string, content: string): Row => ({
  step_index,
  type: 'SYSTEM_MESSAGE',
  content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-08T17:00:00Z sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=${content}\n</SYSTEM_MESSAGE>`,
})
const invoke = (id: string): Row[] => [
  { step_index: 1, type: 'USER_INPUT', content: 'work' },
  {
    step_index: 2,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ id: 'invoke', name: 'invoke_subagent', args: {} }],
  },
  {
    step_index: 3,
    type: 'GENERIC',
    status: 'DONE',
    content: JSON.stringify({ conversationId: id }),
  },
  done(4, 'waiting'),
]
const report = (recipient: string, text: string, start = 1): Row[] => [
  {
    step_index: start,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      {
        id: `report-${start}`,
        name: 'send_message',
        args: {
          Recipient: recipient,
          Message: text,
        },
      },
    ],
  },
  {
    step_index: start + 1,
    type: 'GENERIC',
    status: 'DONE',
    content: 'Message sent',
  },
  done(start + 2, 'I sent the report.'),
]
const encode = (rows: Row[]) =>
  rows.map((row) => JSON.stringify(row)).join('\n') + '\n'
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'agy-brain-test-'))
  const path = (id: string) =>
    join(root, id, '.system_generated/logs/transcript_full.jsonl')
  async function put(id: string, rows: Row[]) {
    await mkdir(join(root, id, '.system_generated/logs'), { recursive: true })
    await writeFile(path(id), encode(rows))
  }
  /** Rewrite like agy's WriteFullTranscript: O_TRUNC on the same inode, optionally pausing while empty. */
  async function rewrite(
    id: string,
    rows: Row[],
    whileEmpty?: () => Promise<void>,
  ) {
    const file = await open(path(id), 'r+')
    try {
      await file.truncate(0)
      await whileEmpty?.()
      await file.write(encode(rows), 0)
    } finally {
      await file.close()
    }
  }
  async function subagent(
    parentId: string,
    childId: string,
    state = 'SUBAGENT_STATE_ALIVE',
    spawnStepIndex = 3,
    spawnCallId = 'invoke',
  ) {
    const dir = join(root, parentId, '.system_generated/subagents')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, `${childId}.json`),
      JSON.stringify({
        conversationId: childId,
        subagentDescriptor: {
          typeName: 'research',
          role: 'Researcher',
        },
        spawnStepIndex,
        state,
      }),
    )
    await deliver(childId, parentId, 'work', true, 'init', {
      conversationId: parentId,
      stepIndex: spawnStepIndex,
      callId: spawnCallId,
      name: 'invoke_subagent',
    })
  }
  async function deliver(
    recipientId: string,
    sender: string,
    content: string,
    read = true,
    msgId: string = randomUUID(),
    tool?: {
      conversationId: string
      stepIndex: number
      callId: string
      name: string
    },
  ) {
    const dir = join(root, recipientId, '.system_generated/messages')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, `${msgId}.json`),
      JSON.stringify({
        id: msgId,
        sender,
        recipient: recipientId,
        priority: 'MESSAGE_PRIORITY_HIGH',
        content,
        timestamp: new Date().toISOString(),
        ...(tool
          ? {
              sourceMetadata: {
                tool: {
                  conversationId: tool.conversationId,
                  stepIndex: tool.stepIndex,
                  toolCall: {
                    id: tool.callId,
                    name: tool.name,
                    argumentsJson: '{}',
                  },
                },
              },
            }
          : {}),
      }),
    )
    const readPath = join(dir, 'read.json')
    const current = await readFile(readPath, 'utf8').then(
      (raw) => JSON.parse(raw) as Record<string, boolean>,
      () => ({}) as Record<string, boolean>,
    )
    if (read) current[msgId] = true
    else delete current[msgId]
    await writeFile(readPath, JSON.stringify(current))
    return msgId
  }
  return {
    root,
    path,
    put,
    rewrite,
    subagent,
    deliver,
    close: () => rm(root, { recursive: true, force: true }),
  }
}
/** settleBrainTurn's poll timer is unref'd; hold the event loop open while awaiting it. */
function alive<T>(promise: Promise<T>): Promise<T> {
  const keepAlive = setInterval(() => {}, 1000)
  return promise.finally(() => clearInterval(keepAlive))
}
function until(predicate: () => boolean) {
  return waitUntil(predicate, 1000, () => 'Expected event did not arrive')
}

test('parent waits for the terminal child report, then its own response, without timestamp assumptions', async () => {
  const f = await fixture()
  const abort = new AbortController()
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
    const items: ProviderTimelineItem[] = []
    let settled = false
    running = settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
      signal: abort.signal,
      minStepIndex: -1,
      pollIntervalMs: 5,
    }).then((result) => {
      settled = true
      return result
    })
    await delay(40)
    assert.equal(settled, false, 'an unfinished child cannot settle the parent')
    await f.deliver(
      parent,
      child,
      'Report quotes: All your subagents and background tasks have been',
      true,
      progressMsgId,
    )
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
    await delay(40)
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
    await delay(40)
    assert.equal(
      settled,
      false,
      'unread final report in parent inbox must block parent completion',
    )
    await f.deliver(parent, child, 'Final report', true, finalMsgId)
    await appendFile(
      f.path(parent),
      encode([message(7, child, 'Final report')]),
    )
    await delay(40)
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
    await until(() => settled)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: {
        inputTokens: 100,
        outputTokens: 20,
        cachedReadTokens: 30,
      },
      wokeUp: true,
      pendingReplay: true,
      wakeupUsage: {
        inputTokens: 100,
        outputTokens: 20,
        cachedReadTokens: 30,
      },
      carriedWakeupUsage: {
        inputTokens: 100,
        outputTokens: 20,
        cachedReadTokens: 30,
      },
    })
    assert.ok(
      items.some(
        (item) => item.type === 'assistant_message' && item.text === 'Summary',
      ),
    )
  } finally {
    abort.abort()
    await running
    await f.close()
  }
})

test('nested child completion uses delivered reports and sending to parent never creates a dependency cycle', async () => {
  const f = await fixture()
  const events: ProviderEvent[] = []
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
    stop = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await until(() =>
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      ),
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
      pendingReplay: true,
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
  for (const row of invoke(child)) projector.project(row)
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
  const events: ProviderEvent[] = []
  let stop: BrainWatchHandle | undefined
  try {
    await f.put(child, [])
    const bytes = Buffer.from(encode([done(1, '你好')]))
    const split = bytes.indexOf(Buffer.from('你')) + 1
    await writeFile(f.path(child), bytes.subarray(0, split))
    stop = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await delay(30)
    assert.equal(events.length, 0)
    await appendFile(f.path(child), bytes.subarray(split))
    const textSeen = (text: string) =>
      events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === text,
      )
    await until(() => textSeen('你好'))
    await until(() =>
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      ),
    )
    await writeFile(
      f.path(child) + '.new',
      encode([done(2, 'replacement is longer')]),
    )
    await rename(f.path(child) + '.new', f.path(child))
    await until(() => textSeen('replacement is longer'))
    await writeFile(f.path(child), encode([done(3, 'short')]))
    await until(() => textSeen('short'))
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
    await until(() =>
      events
        .slice(beforeFollowUp)
        .some(
          (event) => event.type === 'session.turn' && event.state === 'failed',
        ),
    )
    assert.deepEqual(
      events
        .slice(beforeFollowUp)
        .map((e) =>
          e.type === 'session.turn'
            ? `turn:${e.state}`
            : e.type === 'timeline.item'
              ? `${e.item.type}:${'status' in e.item ? e.item.status : ''}`
              : e.type,
        ),
      [
        'turn:started',
        'user_message:',
        'tool_call:running',
        'error:',
        'tool_call:failed',
        'turn:failed',
      ],
    )
    const countBeforeCancel = events.length
    // cancel() drains rows already written, then ignores everything after it resolves.
    await stop.cancel()
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
    await delay(30)
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
  for (const type of ['CHECKPOINT', 'SYSTEM_MESSAGE', 'UNKNOWN'])
    assert.deepEqual(
      projector.project({
        type,
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
  const events: ProviderEvent[] = []
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
    stop = watchBrain(f.root, parent, 'p', (event) => events.push(event), 5)
    await until(() =>
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'failed',
      ),
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
      pendingReplay: true,
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

test('retrying an owned child waits for inbox read instead of accepting old DONE or ERROR', async () => {
  for (const previous of [
    done(1),
    { step_index: 1, type: 'ERROR_MESSAGE', content: 'Previous error' },
  ]) {
    const f = await fixture()
    const abort = new AbortController()
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
      let settled = false
      running = settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        minStepIndex: -1,
        pollIntervalMs: 5,
      }).then((result) => {
        settled = true
        return result
      })
      await delay(40)
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
      await until(() => settled)
      assert.deepEqual(await running, {
        status: 'completed',
        lastStepUsage: undefined,
      })
    } finally {
      abort.abort()
      await running
      await f.close()
    }
  }
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
      pendingReplay: true,
    })
    const completedTools = items.filter(
      (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
        i.type === 'tool_call' && i.status === 'completed',
    )
    assert.deepEqual(
      completedTools.map((t) => [
        t.callId,
        t.detail.type === 'shell' ? t.detail.output?.trim() : '',
      ]),
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
    let settled = false
    running = settleBrainTurn(f.root, parent, 'p', () => {}, {
      signal: abort.signal,
      minStepIndex: -1,
      pollIntervalMs: 5,
    }).then((res) => {
      settled = true
      return res
    })
    await delay(40)
    assert.equal(
      settled,
      false,
      'second read message not yet in transcript must block settlement despite quoted [Message] in first report',
    )
    await appendFile(
      f.path(parent),
      encode([message(7, child, 'Second report'), done(8, 'All done')]),
    )
    await until(() => settled)
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: undefined,
      wokeUp: true,
      pendingReplay: true,
    })
  } finally {
    abort.abort()
    await running
    await f.close()
  }
})

test('shouldEmitPreWakeupTool suppresses completed foreground tools while emitting post-wakeup items and identical OK replies', async () => {
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
    const result = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => items.push(item),
      {
        minStepIndex: 0,
        shouldEmitPreWakeupTool: (item) => item.callId === 'bg-cmd',
      },
    )
    assert.equal(result.status, 'completed')
    assert.equal(
      items.some((i) => i.type === 'tool_call' && i.callId === 'fg-edit'),
      false,
      'already-completed foreground tool call must not be re-emitted',
    )
    assert.deepEqual(
      items.map((i) =>
        i.type === 'tool_call'
          ? `${i.type}:${i.callId}:${i.status}`
          : `${i.type}:${'text' in i ? i.text : ''}`,
      ),
      [
        'tool_call:bg-cmd:completed',
        'tool_call:post-read:running',
        'tool_call:post-read:completed',
        'assistant_message:OK',
      ],
    )

    // When emitForeground is true (fallback if user_message_chunk never arrived), foreground items are recovered without duplicating user_message
    const recovered: ProviderTimelineItem[] = []
    await appendFile(
      f.path(parent),
      encode([
        { step_index: 10, type: 'USER_INPUT', content: 'next turn' },
        {
          step_index: 11,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          thinking: 'Think',
          content: 'Recovered reply',
          tool_calls: [{ id: 'rec-tool', name: 'read_file', args: {} }],
        },
        {
          step_index: 12,
          type: 'GENERIC',
          status: 'DONE',
          content: 'read output',
        },
        done(13, 'Final reply'),
      ]),
    )
    const recResult = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => recovered.push(item),
      { minStepIndex: 9, emitForeground: true },
    )
    assert.equal(recResult.status, 'completed')
    assert.deepEqual(
      recovered.map((i) =>
        i.type === 'tool_call'
          ? `${i.type}:${i.callId}:${i.status}`
          : `${i.type}:${'text' in i ? i.text : ''}`,
      ),
      [
        'reasoning:Think',
        'assistant_message:Recovered reply',
        'tool_call:rec-tool:running',
        'tool_call:rec-tool:completed',
        'assistant_message:Final reply',
      ],
    )
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
    items.flatMap((i) =>
      i.type === 'tool_call' && i.status !== 'running'
        ? [`${i.callId}:${i.status}`]
        : [],
    ),
    ['canceled:canceled', 'interrupted:canceled', 'failed:failed'],
  )
  assert.equal(projector.hasPending(), false, 'no call is left to misattribute')
  assert.equal(projector.hasBackground(), false)
})

test('an in-place O_TRUNC rewrite re-delivers changed steps once and drops unlogged rows', async () => {
  const f = await fixture()
  const abort = new AbortController()
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
    const items: ProviderTimelineItem[] = []
    let settled = false
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        signal: abort.signal,
        pollIntervalMs: 5,
        shouldEmitPreWakeupTool: () => true,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await delay(40)
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
    await delay(40)
    assert.equal(settled, false, 'a GENERATING row is superseded, not final')
    await appendFile(f.path(parent), encode([done(4, 'All done')]))
    assert.equal((await running).status, 'completed')
    assert.deepEqual(
      items.map((i) =>
        i.type === 'tool_call' ? `${i.callId}:${i.status}` : i.type,
      ),
      ['cmd:completed'],
      'the rewritten DONE step is delivered once; unchanged steps are not repeated',
    )
  } finally {
    abort.abort()
    await f.close()
  }
})

test('settlement waits for a late USER_INPUT flush within the grace period', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [{ step_index: 0, type: 'USER_INPUT', content: 'old' }])
    let settled = false
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        minStepIndex: 0,
        pollIntervalMs: 5,
        transcriptGraceMs: 2000,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await delay(40)
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
    assert.deepEqual(await running, {
      status: 'completed',
      lastStepUsage: { inputTokens: 10, cachedReadTokens: 5, outputTokens: 2 },
    })
  } finally {
    await f.close()
  }
})

test('settlement without grace completes immediately when no turn row exists', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [{ step_index: 0, type: 'USER_INPUT', content: 'old' }])
    const startedAt = Date.now()
    assert.deepEqual(
      await settleBrainTurn(f.root, parent, 'p', () => {}, { minStepIndex: 0 }),
      { status: 'completed' },
    )
    assert.ok(Date.now() - startedAt < 500)
  } finally {
    await f.close()
  }
})

test('a missing transcript never throws and only waits when the session directory exists', async () => {
  const f = await fixture()
  try {
    let startedAt = Date.now()
    assert.deepEqual(
      await settleBrainTurn(f.root, parent, 'p', () => {}, {
        transcriptGraceMs: 2000,
      }),
      { status: 'completed' },
    )
    assert.ok(Date.now() - startedAt < 500, 'no Brain directory: no grace')
    // Earlier subagents and no transcript used to throw ENOENT.
    await f.subagent(parent, child)
    startedAt = Date.now()
    assert.deepEqual(
      await alive(
        settleBrainTurn(f.root, parent, 'p', () => {}, {
          pollIntervalMs: 5,
          transcriptGraceMs: 100,
        }),
      ),
      { status: 'completed' },
    )
    assert.ok(Date.now() - startedAt >= 100, 'directory exists: grace applies')
  } finally {
    await f.close()
  }
})

test('settlement stops waiting on an idle transcript without a final response', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ])
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        pollIntervalMs: 5,
        stallTimeoutMs: 50,
      }),
    )
    assert.equal(result.status, 'failed')
    assert.match(result.error ?? '', /idle for 0s without a final response/)
    assert.deepEqual(
      items.map((i) => (i.type === 'error' ? i.id : i.type)),
      ['p:step:2:stalled'],
    )
  } finally {
    await f.close()
  }
})

test('a settled ALIVE child does not suspend the parent stall timeout', async () => {
  const f = await fixture()
  try {
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
    const items: ProviderTimelineItem[] = []
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 50,
      }),
    )
    assert.equal(result.status, 'failed')
    assert.ok(
      items.some((i) => i.type === 'error' && i.id.endsWith(':stalled')),
    )
  } finally {
    await f.close()
  }
})

test('woken-turn LLM generation after SYSTEM_MESSAGE or completed GENERIC uses wokenStallTimeoutMs and stalls if that cap expires', async () => {
  const f = await fixture()
  try {
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
    const items: ProviderTimelineItem[] = []
    let settled = false
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 25,
        wokenStallTimeoutMs: 200,
      }),
    ).then((res) => {
      settled = true
      return res
    })
    await delay(70)
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
    const result = await running
    assert.equal(result.status, 'completed')
    assert.equal(
      items.some((i) => i.type === 'error'),
      false,
    )
    assert.ok(
      items.some(
        (i) =>
          i.type === 'assistant_message' &&
          i.text === 'Final synthesis complete.',
      ),
    )

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
    const stalledItems: ProviderTimelineItem[] = []
    const wokenStallStartedAt = Date.now()
    const stalledResult = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => stalledItems.push(item), {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 20,
        wokenStallTimeoutMs: 80,
      }),
    )
    const wokenStallElapsed = Date.now() - wokenStallStartedAt
    assert.equal(stalledResult.status, 'failed')
    assert.equal(stalledResult.stalled, true)
    assert.ok(
      wokenStallElapsed >= 70,
      `expected woken stall to wait for wokenStallTimeoutMs (80ms), got ${wokenStallElapsed}ms`,
    )
    assert.ok(
      stalledItems.some(
        (i) => i.type === 'error' && i.id === 'p:step:7:stalled',
      ),
    )
  } finally {
    await f.close()
  }
})

test('unmatched foreground tool calls and permanent consumed-message drift still stall at stallTimeoutMs', async () => {
  const f = await fixture()
  try {
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
    const orphanStartedAt = Date.now()
    const orphanResult = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 30,
        wokenStallTimeoutMs: 500,
      }),
    )
    const orphanElapsed = Date.now() - orphanStartedAt
    assert.equal(orphanResult.status, 'failed')
    assert.equal(orphanResult.stalled, true)
    assert.ok(
      orphanElapsed < 250,
      `expected orphan foreground call to stall at stallTimeoutMs (30ms), got ${orphanElapsed}ms`,
    )

    // 2. Permanent gap between inbox.consumedNonUserCount and rendered [Message] sender= lines
    await f.deliver(parent, child, 'Unrendered message', true, 'drift')
    await f.put(parent, [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      done(2, 'Done without rendering inbox message'),
    ])
    const driftStartedAt = Date.now()
    const driftResult = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 30,
        wokenStallTimeoutMs: 500,
      }),
    )
    const driftElapsed = Date.now() - driftStartedAt
    assert.equal(driftResult.status, 'failed')
    assert.equal(driftResult.stalled, true)
    assert.ok(
      driftElapsed < 250,
      `expected consumed-message drift to stall at stallTimeoutMs (30ms), got ${driftElapsed}ms`,
    )
  } finally {
    await f.close()
  }
})

test('a restart notice inside a woken turn keeps projecting the woken response', async () => {
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
      done(5, 'Woken answer after notice'),
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
    assert.ok(
      items.some(
        (i) =>
          i.type === 'assistant_message' &&
          i.text === 'Woken answer after notice',
      ),
    )

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

for (const wait of ['approval', 'background task'] as const)
  test(`a pending ${wait} suspends the stall timeout`, async () => {
    const f = await fixture()
    const abort = new AbortController()
    try {
      const taskId = `${parent}/task-3`
      await f.put(
        parent,
        wait === 'approval'
          ? [
              { step_index: 1, type: 'USER_INPUT', content: 'run' },
              { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
            ]
          : [
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
                content: `Tool is running as a background task with task id: ${taskId}\n`,
              },
              done(4, 'Waiting'),
            ],
      )
      let settled = false
      const running = alive(
        settleBrainTurn(f.root, parent, 'p', () => {}, {
          signal: abort.signal,
          pollIntervalMs: 5,
          stallTimeoutMs: 20,
          isWaitingOnUser: () => wait === 'approval',
        }),
      ).then((result) => {
        settled = true
        return result
      })
      await delay(100)
      assert.equal(settled, false, `a pending ${wait} is not a stall`)
      abort.abort()
      assert.equal((await running).status, 'canceled')
    } finally {
      await f.close()
    }
  })

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
  const events: ProviderEvent[] = []
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(child, [message(0, parent, 'work'), done(1, 'First reply')])
    handle = watchBrain(
      f.root,
      child,
      'c',
      (event) => events.push(event),
      5,
      () => true,
      { afterStep: 1, turnSeq: 3 },
    )
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
    await until(() =>
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
    )
    assert.deepEqual(
      events.flatMap((e) =>
        e.type === 'session.turn'
          ? [`${e.turnId}:${e.state}`]
          : e.type === 'timeline.item' && e.item.type === 'assistant_message'
            ? [e.item.text]
            : [],
      ),
      ['c:turn:4:started', 'Second reply', 'c:turn:4:completed'],
    )
    assert.equal(handle.isStopped(), false)
    await handle.cancel()
    assert.equal(handle.isStopped(), true)
  } finally {
    handle?.()
    await f.close()
  }
})

for (const emptyWindowMs of [0, 50]) {
  test(
    `a child watcher does not restart turns across an observed ${emptyWindowMs}ms rewrite window`,
    { timeout: 5000 },
    async () => {
      const f = await fixture()
      const events: ProviderEvent[] = []
      let handle: BrainWatchHandle | undefined
      let polls = 0
      const afterRead = async () => {
        const target = polls + 2
        // Metadata is read before the transcript; two sequential polls guarantee an intervening transcript read.
        await until(() => polls >= target)
      }
      try {
        const rows = [message(0, parent, 'work'), done(1, 'Reply')]
        await f.put(child, rows)
        handle = watchBrain(
          f.root,
          child,
          'c',
          (event) => events.push(event),
          5,
          undefined,
          undefined,
          () => {
            polls++
          },
        )
        await handle.ready
        const before = events.length
        await f.rewrite(
          child,
          [...rows, { step_index: 2, type: 'CHECKPOINT' }],
          async () => {
            await afterRead()
            if (emptyWindowMs > 0) await delay(emptyWindowMs)
          },
        )
        await afterRead()
        assert.equal(
          events.length,
          before,
          'unchanged steps are not new activity',
        )
        await appendFile(
          f.path(child),
          encode([message(3, parent, 'New work'), done(4, 'New reply')]),
        )
        await afterRead()
        assert.deepEqual(
          events.flatMap((e) =>
            e.type === 'timeline.item' && e.item.type === 'assistant_message'
              ? [e.item.text]
              : [],
          ),
          ['Reply', 'New reply'],
        )
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
          afterRead,
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
    },
  )
}

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
      assert.deepEqual(
        events.flatMap((e) => (e.type === 'session.turn' ? [e.state] : [])),
        ['started', 'canceled'],
      )
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
        },
      )
      await handle.ready
      const file = await open(f.path(child), 'r+')
      try {
        await file.truncate(0)
        const target = polls + 2
        await until(() => polls >= target)
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
      assert.deepEqual(
        events.flatMap((e) =>
          e.type === 'timeline.item' && e.item.type === 'assistant_message'
            ? [e.item.text]
            : [],
        ),
        ['old', 'NEW COMPLETE ROW'],
      )
      assert.deepEqual(
        events.flatMap((e) => (e.type === 'session.turn' ? [e.state] : [])),
        ['started', 'canceled'],
      )
    } finally {
      handle?.()
      await f.close()
    }
  },
)

test('a rewrite observed mid-O_TRUNC does not end an anchored turn', async () => {
  const f = await fixture()
  const abort = new AbortController()
  try {
    const rows = [
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      { step_index: 2, type: 'PLANNER_RESPONSE', status: 'RUNNING' },
    ]
    await f.put(parent, rows)
    let settled = false
    const running = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: abort.signal,
        pollIntervalMs: 5,
      }),
    ).then((result) => {
      settled = true
      return result
    })
    await delay(30)
    await f.rewrite(parent, [...rows, done(3, 'ok')], async () => {
      await delay(30)
      assert.equal(settled, false, 'an empty file mid-rewrite is not "no turn"')
    })
    assert.equal((await running).status, 'completed')
  } finally {
    abort.abort()
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

test('stopped calls never linger: restart notices and new responses drop them', () => {
  for (const interruption of [[stoppedNotice(2)], []]) {
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
      items.flatMap((i) =>
        i.type === 'tool_call' && i.status === 'completed' ? [i.callId] : [],
      ),
      ['fresh'],
      'the result belongs to the current response',
    )
    assert.equal(projector.hasPending(), false)
  }
})

test('a child woken after an interrupted tool call completes its new turn', async () => {
  const f = await fixture()
  const events: ProviderEvent[] = []
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
    handle = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await until(() =>
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('resuming after a drained cancel neither repeats nor skips child rows', async () => {
  const f = await fixture()
  const first: ProviderEvent[] = []
  const second: ProviderEvent[] = []
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
      (e) => second.push(e),
      5,
      () => true,
      old.resumePoint(),
    )
    await resumed.ready
    await appendFile(
      f.path(child),
      encode([message(2, parent, 'again'), done(3, 'After resume')]),
    )
    await until(() =>
      second.some((e) => e.type === 'session.turn' && e.state === 'completed'),
    )
    const replies = (events: ProviderEvent[]) =>
      events.flatMap((e) =>
        e.type === 'timeline.item' && e.item.type === 'assistant_message'
          ? [e.item.text]
          : [],
      )
    assert.deepEqual(replies(first), ['Before cancel'])
    assert.deepEqual(replies(second), ['After resume'])
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

test('a woken ask_question is not background work and closes without a question handler', async () => {
  const f = await fixture()
  try {
    const rows = await loadWokenQuestionFixture()
    // Steps 0..5: background run_command wakes the session, and step 5 calls ask_question with no GENERIC row yet.
    await f.put(parent, rows.slice(0, 6))

    // Waiting on user question in a woken turn is NOT active background work (must not relaunch on interrupt)
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)

    // Without onQuestion, settleBrainTurn completes immediately instead of stalling for 15m
    const directItems: ProviderTimelineItem[] = []
    const direct = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => directItems.push(item),
      { minStepIndex: -1, pollIntervalMs: 5, stallTimeoutMs: 30 },
    )
    assert.equal(direct.status, 'completed')
    assert.equal(direct.wokeUp, true)
    assert.ok(
      directItems.some(
        (i) =>
          i.type === 'tool_call' &&
          i.name === 'ask_question' &&
          i.status === 'completed',
      ),
    )
  } finally {
    await f.close()
  }
})

test('answering a woken ask_question continues the turn and records the answer', async () => {
  const f = await fixture()
  try {
    const rows = await loadWokenQuestionFixture()
    await f.put(parent, rows.slice(0, 6))

    // With onQuestion answering and appending agy's GENERIC + PLANNER_RESPONSE (steps 6..7), settleBrainTurn continues and emits the final reply
    const contItems: ProviderTimelineItem[] = []
    const cont = await settleBrainTurn(
      f.root,
      parent,
      'p',
      (item) => contItems.push(item),
      {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 50,
        onQuestion: async (q) => {
          assert.equal(q.wokeUp, true)
          assert.equal(q.questions.length, 1)
          assert.equal(
            q.questions[0].question,
            'Background check finished. Which option?',
          )
          await appendFile(f.path(parent), encode(rows.slice(6)))
          return {
            status: 'answered',
            answerText: '(Recommended) Option 1',
          }
        },
      },
    )
    assert.equal(cont.status, 'completed')
    assert.equal(cont.wokeUp, true)
    const completedAsk = contItems.findLast(
      (i) =>
        i.type === 'tool_call' &&
        i.name === 'ask_question' &&
        i.status === 'completed',
    )
    assert.ok(
      completedAsk?.type === 'tool_call' &&
        completedAsk.detail.type === 'plain_text' &&
        completedAsk.detail.text?.includes('Answer:\n(Recommended) Option 1'),
    )
    assert.deepEqual(
      contItems.flatMap((i) =>
        i.type === 'assistant_message' ? [i.text] : [],
      ),
      ['Done: (Recommended) Option 1'],
    )
  } finally {
    await f.close()
  }
})

test('a woken question answered through elicitation waits for agy to reply to the answer', async () => {
  const f = await fixture()
  try {
    const rows = await loadWokenQuestionFixture()
    await f.put(parent, rows.slice(0, 6))
    const items: ProviderTimelineItem[] = []
    const settled = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        minStepIndex: -1,
        pollIntervalMs: 5,
        stallTimeoutMs: 1000,
        onQuestion: async () => {
          // agy logs the tool result and its reply only after the elicitation response, so later than this return.
          setTimeout(() => {
            void appendFile(
              f.path(parent),
              encode([rows[6], done(7, 'Done: (Recommended) Option 1')]),
            )
          }, 60)
          return { status: 'answered', answerText: '(Recommended) Option 1' }
        },
      }),
    )
    assert.equal(settled.status, 'completed')
    assert.deepEqual(
      items.flatMap((i) => (i.type === 'assistant_message' ? [i.text] : [])),
      ['Done: (Recommended) Option 1'],
    )
  } finally {
    await f.close()
  }
})

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
  assert.equal(p2.consumePendingQuestion()?.tool.callId, 'ask')

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
  assert.deepEqual(
    checkpointed.flatMap((i) =>
      i.type === 'tool_call' &&
      i.status === 'completed' &&
      i.detail.type !== 'unknown'
        ? [
            [
              i.callId,
              i.detail.type === 'shell'
                ? i.detail.output
                : i.detail.type === 'plain_text'
                  ? i.detail.text
                  : '',
            ],
          ]
        : [],
    ),
    [
      ['ask', 'Choose\n  1. A\n  2. B\n\nAnswer:\nUser chose A'],
      ['cmd', 'hi'],
    ],
  )
})

test('a leading ask_question fires onQuestion before its sibling has a GENERIC row', async () => {
  const f = await fixture()
  try {
    // Same PLANNER_RESPONSE [ask_question, run_command] with no GENERIC rows yet still fires onQuestion immediately
    await f.put(parent, [
      {
        step_index: 0,
        type: 'USER_INPUT',
        content: '<USER_REQUEST>Ask then run</USER_REQUEST>',
      },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Need confirmation before running command:',
        tool_calls: [
          {
            id: 'ask-first',
            name: 'ask_question',
            args: {
              questions: [{ question: 'Run echo?', options: ['Yes', 'No'] }],
            },
          },
          {
            id: 'cmd-second',
            name: 'run_command',
            args: { CommandLine: 'echo hi' },
          },
        ],
      },
    ])
    const leadingAskItems: ProviderTimelineItem[] = []
    let leadingAskTriggered = false
    const leadingAskSettled = await alive(
      settleBrainTurn(
        f.root,
        parent,
        'p',
        (item) => leadingAskItems.push(item),
        {
          minStepIndex: -1,
          pollIntervalMs: 5,
          stallTimeoutMs: 80,
          onQuestion: async (q) => {
            leadingAskTriggered = true
            assert.equal(q.tool.callId, 'ask-first')
            assert.equal(q.questions[0]?.question, 'Run echo?')
            await appendFile(
              f.path(parent),
              encode([
                {
                  step_index: 2,
                  type: 'GENERIC',
                  status: 'DONE',
                  content: 'Yes',
                },
                {
                  step_index: 3,
                  type: 'GENERIC',
                  status: 'DONE',
                  content: 'hi',
                },
                done(4, 'Command completed.'),
              ]),
            )
            return { status: 'answered', answerText: 'Yes' }
          },
        },
      ),
    )
    assert.equal(leadingAskTriggered, true)
    assert.equal(leadingAskSettled.status, 'completed')
    const completedCmdSecond = leadingAskItems.find(
      (i): i is Extract<ProviderTimelineItem, { type: 'tool_call' }> =>
        i.type === 'tool_call' &&
        i.callId === 'cmd-second' &&
        i.status === 'completed',
    )
    assert.ok(
      completedCmdSecond?.detail.type === 'shell' &&
        completedCmdSecond.detail.output === 'hi',
    )
  } finally {
    await f.close()
  }
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

test('foreground turn aborted by user permission denial settles immediately without stalling', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', status: 'DONE', content: 'echo 1' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ name: 'run_command', args: { CommandLine: 'echo 1' } }],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'ERROR',
        content:
          'Encountered error in step execution: permission check failed for unsandboxed "echo 1": user denied permission to execute',
      },
    ])
    const items: ProviderTimelineItem[] = []
    const started = Date.now()
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        pollIntervalMs: 5,
        stallTimeoutMs: 5000,
        acpToolCallIds: ['call_denied_0'],
        deniedToolCallIds: new Set(['call_denied_0']),
      }),
    )
    assert.equal(result.status, 'completed')
    assert.ok(Date.now() - started < 1000)

    // Untrusted tool error output containing permission-denial wording without a recorded denial must not settle early.
    const spoofed = parent.replace(/^1/, '3')
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
    const spoofResult = await alive(
      settleBrainTurn(f.root, spoofed, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 60,
        acpToolCallIds: ['call_not_denied'],
        deniedToolCallIds: new Set(),
      }),
    )
    assert.equal(spoofResult.status, 'failed')

    // agy 1.5.5 returns end_turn without flushing the GENERIC row when permission is denied.
    const unlogged = parent.replace(/^1/, '2')
    await f.put(unlogged, [
      { step_index: 0, type: 'USER_INPUT', status: 'DONE', content: 'echo 2' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [{ name: 'run_command', args: { CommandLine: 'echo 2' } }],
      },
    ])
    const t2 = Date.now()
    const result2 = await alive(
      settleBrainTurn(f.root, unlogged, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 5000,
        acpToolCallIds: ['call_denied_1'],
        deniedToolCallIds: new Set(['call_denied_1']),
      }),
    )
    assert.equal(result2.status, 'completed')
    assert.ok(Date.now() - t2 < 1000)
  } finally {
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

test('API/stream retries recover after 4s and multiple interruptions; unfinished response stays pending', async () => {
  const f = await fixture()
  const items: ProviderTimelineItem[] = []
  try {
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
    let finished = false
    const pending = alive(
      settleBrainTurn(f.root, parent, 'p', (item) => items.push(item), {
        pollIntervalMs: 5,
        stallTimeoutMs: 10_000,
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await delay(4000)
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
    await delay(25)
    assert.equal(finished, false)
    await appendFile(f.path(parent), encode([done(5)]))
    assert.equal((await pending).status, 'completed')
    assert.equal(
      items.some((item) => item.type === 'error'),
      false,
    )
  } finally {
    await f.close()
  }
})

test('root retry exhaustion is bounded even with retained background work; cancellation wins', async () => {
  const f = await fixture()
  try {
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
    const result = await alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 30,
        retryTimeoutMs: 30,
      }),
    )
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
        pollIntervalMs: 5,
      }),
    )
    controller.abort()
    assert.equal((await pending).status, 'canceled')
  } finally {
    await f.close()
  }
})

test('child retry recovers and delivers its report before parent completion', async () => {
  const f = await fixture()
  try {
    await f.put(parent, invoke(child))
    await f.subagent(parent, child)
    await f.put(child, [interrupted(1)])
    assert.equal(await hasActiveBackgroundWork(f.root, parent), true)
    let finished = false
    const pending = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        pollIntervalMs: 5,
        stallTimeoutMs: 1000,
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await delay(25)
    assert.equal(finished, false)
    await appendFile(f.path(child), encode(report(parent, 'report', 2)))
    await f.deliver(parent, child, 'report')
    await appendFile(
      f.path(parent),
      encode([message(5, child, 'report'), done(6)]),
    )
    assert.equal((await pending).status, 'completed')
    assert.equal(await hasActiveBackgroundWork(f.root, parent), false)
  } finally {
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

test('watcher retries recover without failed events, while cancellation and exhaustion stay distinct', async () => {
  const f = await fixture()
  const handles: BrainWatchHandle[] = []
  try {
    for (const outcome of ['recover', 'cancel', 'exhaust'] as const) {
      await f.put(child, [interrupted(1)])
      const events: ProviderEvent[] = []
      const handle = watchBrain(
        f.root,
        child,
        'c',
        (event) => events.push(event),
        5,
        undefined,
        undefined,
        undefined,
        80,
      )
      handles.push(handle)
      await handle.ready
      assert.equal(handle.isRunning(), true)
      if (outcome === 'recover') {
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
        await delay(15)
        assert.equal(handle.isRunning(), true)
        await appendFile(f.path(child), encode([done(3)]))
      } else if (outcome === 'cancel') await handle.cancel()
      await until(() =>
        events.some(
          (e) =>
            e.type === 'session.turn' &&
            e.state ===
              (outcome === 'recover'
                ? 'completed'
                : outcome === 'cancel'
                  ? 'canceled'
                  : 'failed'),
        ),
      )
      assert.equal(
        events.some(
          (e) => e.type === 'timeline.item' && e.item.type === 'error',
        ),
        false,
      )
      handle()
    }
  } finally {
    for (const handle of handles) handle()
    await f.close()
  }
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

test('a missing transcript during retry cannot report success', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    await f.put(child, [interrupted(1)])
    const events: ProviderEvent[] = []
    handle = watchBrain(
      f.root,
      child,
      'c',
      (event) => events.push(event),
      5,
      () => false,
      undefined,
      undefined,
      50,
    )
    await handle.ready
    await rm(f.path(child))
    await until(() =>
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

test('an explicit error status is never classified as a retry warning', () => {
  const projector = new BrainTranscriptProjector('retry')
  assert.equal(
    projector.project({ ...interrupted(1), status: 'ERROR' })[0].type,
    'error',
  )
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

test('root retry wait uses its own bounded policy and can recover with a quiet DONE', async () => {
  const f = await fixture()
  const controller = new AbortController()
  let pending: ReturnType<typeof settleBrainTurn> | undefined
  try {
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
    let finished = false
    pending = alive(
      settleBrainTurn(f.root, parent, 'p', () => {}, {
        signal: controller.signal,
        pollIntervalMs: 5,
        stallTimeoutMs: 5,
        retryTimeoutMs: 1000,
      }),
    ).then((result) => {
      finished = true
      return result
    })
    await delay(25)
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
    assert.equal((await pending).status, 'completed')
  } finally {
    controller.abort()
    await pending
    await f.close()
  }
})

test('expired retries do not hide retained processes, daemon policy, unread messages or active descendants', async () => {
  const f = await fixture()
  try {
    const expired = interrupted(4, '2020-01-01T00:00:00Z')
    for (const IsDaemon of [false, true]) {
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
          content:
            'Tool is running as a background task with task id: retained',
        },
        expired,
      ])
      assert.equal(await hasActiveBackgroundWork(f.root, parent), !IsDaemon)
      assert.equal(
        await hasActiveBackgroundWork(f.root, parent, { includeDaemons: true }),
        true,
      )
    }
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

test('child registry forwards the shared retry policy to its watcher', async () => {
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
    await f.put(child, [
      interrupted(1, new Date(Date.now() - 1000).toISOString()),
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
        onStep: (step) => {
          if (step === 3) controller.abort()
        },
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

test('completed checkpoint uses native compaction without guessing trigger or token counts and keeps tool offsets', async () => {
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
    { type: 'compaction', id: 'p:step:93:compaction', status: 'completed' },
  ])
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
  for (const status of [undefined, 'RUNNING', 'CANCELED', 'ERROR']) {
    assert.deepEqual(
      new BrainTranscriptProjector('p').project({ ...checkpoint, status }),
      [],
    )
  }
})

test('foreground and wakeup project checkpoints without using checkpoint tokens as model usage', async () => {
  for (const wakeup of [false, true]) {
    const f = await fixture()
    try {
      const checkpoint = { ...(await checkpointFixture()), step_index: 4 }
      await f.put(parent, [
        { step_index: 1, type: 'USER_INPUT', content: 'work' },
        { ...done(2, 'waiting'), input_tokens: 100, output_tokens: 5 },
        ...(wakeup ? [message(3, 'system', 'continue')] : []),
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
          },
        ],
      )
      assert.equal(result.lastStepUsage?.inputTokens, 10)
      assert.equal(result.lastStepUsage?.outputTokens, 2)
      assert.equal(result.wakeupUsage?.inputTokens, wakeup ? 10 : undefined)
      assert.equal(result.wakeupUsage?.outputTokens, wakeup ? 2 : undefined)
    } finally {
      await f.close()
    }
  }
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
      { type: 'compaction', id: 'p:step:93:compaction', status: 'completed' },
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
    const compactions = () =>
      events.flatMap((event) =>
        event.type === 'timeline.item' && event.item.type === 'compaction'
          ? [event]
          : [],
      )
    assert.equal(compactions().length, 1)
    assert.equal(compactions()[0].sessionId, 'child')
    await f.rewrite(
      child,
      [...rows, { step_index: 3, type: 'CHECKPOINT', status: 'RUNNING' }],
      async () => {
        await delay(50)
      },
    )
    await delay(25)
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
    await until(() =>
      events.some(
        (event) =>
          event.type === 'timeline.item' && event.item.type === 'compaction',
      ),
    )
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
    const projected = events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'compaction'
        ? [event]
        : [],
    )
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

test('a completed checkpoint retains background and daemon task tracking', async () => {
  const checkpoint = await checkpointFixture()
  for (const IsDaemon of [false, true]) {
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
        shouldEmitPreWakeupTool: () => false,
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
        shouldEmitPreWakeupTool: () => false,
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
    assert.equal(
      items.some(
        (i) => i.type === 'assistant_message' && i.text === 'Task ready.',
      ),
      true,
    )
  } finally {
    await f.close()
  }
})

test('in-RPC high-priority continuation is suppressed and excluded from wakeupUsage while genuine post-RPC wakeup is emitted', async () => {
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
        shouldEmitPreWakeupTool: (item) => item.callId === 'bg-196',
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
      items1.map((i) =>
        i.type === 'tool_call'
          ? `${i.type}:${i.callId}:${i.status}`
          : `${i.type}:${'text' in i ? i.text : ''}`,
      ),
      ['tool_call:bg-196:completed'],
      'in-RPC continuation must emit only unstreamed background tool completion',
    )
  } finally {
    await f1.close()
  }

  // Subcase 2: 100% KV-cache-hit post-RPC wakeup (input_tokens: 0) is never swallowed, even with emitForeground.
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
    for (const emitForeground of [false, true]) {
      const itemsC: ProviderTimelineItem[] = []
      const resC = await settleBrainTurn(
        f2.root,
        parent,
        'p',
        (item) => itemsC.push(item),
        {
          minStepIndex: 0,
          emitForeground,
          foregroundAcp: emitForeground
            ? []
            : [{ type: 'assistant_message', text: 'Start' }],
        },
      )
      assert.equal(resC.status, 'completed')
      assert.equal(resC.wokeUp, true)
      assert.deepEqual(resC.wakeupUsage, {
        inputTokens: 0,
        cachedReadTokens: 50000,
        outputTokens: 10,
      })
      assert.equal(
        itemsC.some(
          (i) =>
            i.type === 'assistant_message' && i.text === 'Cached wakeup reply.',
        ),
        true,
      )
    }
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
    assert.deepEqual(
      items2.map((i) => `${i.type}:${'text' in i ? i.text : ''}`),
      ['assistant_message:OK'],
    )
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

    let turn2Settled = false
    running = settleBrainTurn(f.root, parent, 'p', () => {}, {
      signal: abort.signal,
      minStepIndex: 4,
      pollIntervalMs: 5,
    }).then((res) => {
      turn2Settled = true
      return res
    })
    await delay(30)
    assert.equal(
      turn2Settled,
      false,
      'Turn 2 must neither fail on Turn 1 grandchild1 nor settle before Turn 2 grandchild2 completes',
    )

    // Finish grandchild2 -> child -> parent.
    await appendFile(
      f.path(grandchild2),
      encode([
        { step_index: 2, type: 'GENERIC', status: 'DONE', content: 'ok' },
        ...report(child, 'gc2 finished', 3),
      ]),
    )
    await f.deliver(child, grandchild2, 'gc2 finished', true, 'gc2-done')
    await appendFile(
      f.path(child),
      encode([
        message(12, grandchild2, 'gc2 finished'),
        ...report(parent, 'turn 2 complete', 13),
      ]),
    )
    await f.deliver(parent, child, 'turn 2 complete', true, 'c-w2')
    await appendFile(
      f.path(parent),
      encode([message(13, child, 'turn 2 complete'), done(14, 'done turn 2')]),
    )

    await waitUntil(() => turn2Settled)
    assert.equal(((await running) as { status: string }).status, 'completed')
    await waitUntil(() => !handle!.isRunning())
    const childTurns = childEvents.flatMap((e) =>
      e.type === 'session.turn' ? [`${e.turnId}:${e.state}`] : [],
    )
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

test('re-waking a child after Turn-1 retry timeout succeeds without Turn-1 retry carry-over and waits on untagged inbox read-skew', async () => {
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
    // Turn 1: parent spawns child and sends untagged wakeup #1; child ingests it and expires on a retry warning.
    const parentTurn1: Row[] = [
      ...invoke(child),
      ...report(child, 'untagged wake 1', 5),
    ]
    await f.put(parent, parentTurn1)
    await f.subagent(parent, child, 'SUBAGENT_STATE_ALIVE', 3, 'invoke')
    await f.deliver(child, parent, 'untagged wake 1', true, 'u1')
    const childTurn1: Row[] = [
      message(0, parent, 'initial task'),
      done(1, 'ready'),
      message(2, parent, 'untagged wake 1'),
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

    // Turn 2: parent logs completed untagged send_message BEFORE u2.json appears in child inbox (read-skew).
    const parentTurn2: Row[] = [
      ...parentTurn1,
      { step_index: 8, type: 'USER_INPUT', content: 'retry in turn 2' },
      ...report(child, 'untagged wake 2', 9),
    ]
    await f.put(parent, parentTurn2)

    let turn2Result: { status: string; error?: string } | undefined
    running = settleBrainTurn(f.root, parent, 'p', () => {}, {
      signal: abort.signal,
      minStepIndex: 7,
      pollIntervalMs: 5,
      retryTimeoutMs: 40,
    }).then((res) => {
      turn2Result = res
      return res
    })
    await delay(30)
    assert.equal(
      turn2Result,
      undefined,
      'countPriorWakeups must keep resolveChildTurnState pending during untagged inbox read-skew without failing on Turn-1 retryStep',
    )

    // Deliver u2 and complete child + parent in Turn 2 without clearing retryStep=3 except via childMinStep scoping.
    await f.deliver(child, parent, 'untagged wake 2', true, 'u2')
    await appendFile(
      f.path(child),
      encode([
        message(4, parent, 'untagged wake 2'),
        ...report(parent, 'recovered in turn 2', 5),
      ]),
    )
    await f.deliver(parent, child, 'recovered in turn 2', true, 'c-done')
    await appendFile(
      f.path(parent),
      encode([message(12, child, 'recovered in turn 2'), done(13, 'all done')]),
    )

    await waitUntil(() => turn2Result !== undefined)
    assert.equal(((await running) as { status: string }).status, 'completed')
    await waitUntil(() => !handle!.isRunning())
    const childTurns = childEvents.flatMap((e) =>
      e.type === 'session.turn' ? [`${e.turnId}:${e.state}`] : [],
    )
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
    assert.deepEqual(
      gcEvents.flatMap((e) => (e.type === 'session.turn' ? [e.state] : [])),
      ['started', 'completed'],
    )

    // Peer-woken child: woken by sibling `child` rather than `parent`, must settle cleanly without hanging.
    await f.put(peerChild, [
      message(0, parent, 'init peer'),
      done(1, 'peer turn 1 done'),
    ])
    const peerEvents: ProviderEvent[] = []
    const peerHandle = watchBrain(
      f.root,
      peerChild,
      'peer',
      (e) => peerEvents.push(e),
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
    await waitUntil(() =>
      peerEvents.some(
        (e) =>
          e.type === 'session.turn' &&
          e.turnId === 'peer:turn:2' &&
          e.state === 'completed',
      ),
    )
  } finally {
    for (const h of handles) h()
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
    await appendFile(
      f.path(child),
      encode([
        message(2, parent, 'wake 1'),
        interrupted(3, '2020-01-01T00:00:00Z'),
      ]),
    )
    await appendFile(
      f.path(sibling),
      encode([message(2, parent, 'wake s'), done(3, 's2 done')]),
    )
    await f.deliver(parent, sibling, 's2 report', true, 's2-rep')
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
    assert.deepEqual(
      childEvents.flatMap((e) =>
        e.type === 'session.turn' ? [`${e.turnId}:${e.state}`] : [],
      ),
      [
        'child:turn:1:started',
        'child:turn:1:completed',
        'child:turn:2:started',
        'child:turn:2:failed',
      ],
    )

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
    assert.deepEqual(
      stEvents.flatMap((e) => (e.type === 'session.turn' ? [e.state] : [])),
      ['started', 'failed'],
    )
  } finally {
    for (const h of handles) h()
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

test('recordPreambleWakeups deduplicates completed wakeups by stepIndex when preamble straddles minStepIndex', () => {
  const node = new BrainNode(parent, 'p', { allowInitialSpawnPrompt: false })
  const rows = [
    { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { id: 'w1', name: 'send_message', args: { Recipient: child } },
      ],
    },
    {
      step_index: 2,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Sent',
    },
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        { id: 'w2', name: 'send_message', args: { Recipient: child } },
      ],
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Sent 2',
    },
  ]
  // Simulate BrainTurnObservation having already recorded step 2 (<= minStepIndex 2),
  // while preambleSlice straddles minStepIndex and includes steps 0..4.
  const priorWakeups = new Map<number, string>([[2, child]])
  node.recordPreambleWakeups(
    rows,
    {
      messages: [],
      readIds: new Set(),
      unconsumedHighPriority: [],
      consumedNonUserCount: 0,
      stepCallIds: new Map(),
    },
    [],
    priorWakeups,
  )
  assert.equal(
    node.countPriorWakeups(child, 4),
    2,
    'overlapping step 2 in priorWakeups and preambleSlice must not be double-counted',
  )
  assert.equal(node.countPriorWakeups(child, 2), 1)
})

test('watchBrain does not advance turnFloor past child ERROR_MESSAGE on grandchild report in the same turn', async () => {
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
      50,
      undefined,
      undefined,
      undefined,
      20,
    )
    await handle.ready
    await new Promise((r) => setTimeout(r, 30))
    await f.put(child, [
      message(0, parent, 'work'),
      {
        step_index: 1,
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        content:
          'The stream was interrupted. Please continue the task you were working on.',
      },
      message(2, grandchild, 'late grandchild report'),
    ])
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

test('resolveChildTurnState correlates mixed untagged Turn 1 and tagged Turn 2 wakeups', () => {
  const parentNode = new BrainNode(parent, parent, {
    allowInitialSpawnPrompt: false,
  })
  const childNode = new BrainNode(child, child, {
    allowInitialSpawnPrompt: true,
  })
  childNode.ingest(message(0, parent, 'init'))
  childNode.ingest(message(3, parent, 'untagged turn 1'))
  childNode.ingest(message(6, parent, 'tagged turn 2'))

  const state = resolveChildTurnState(
    parentNode,
    5,
    { conversationId: child, spawnStepIndex: 2, state: 'SUBAGENT_STATE_ALIVE' },
    childNode,
    {
      messages: [
        {
          id: 'u1',
          sender: parent,
          priority: 'MESSAGE_PRIORITY_HIGH',
          tool: { conversationId: parent, stepIndex: undefined },
        },
        {
          id: 't2',
          sender: parent,
          priority: 'MESSAGE_PRIORITY_HIGH',
          tool: { conversationId: parent, stepIndex: 7, name: 'send_message' },
        },
      ],
      readIds: new Set(['u1', 't2']),
      unconsumedHighPriority: [],
      consumedNonUserCount: 2,
      stepCallIds: new Map(),
    },
  )
  assert.deepEqual(state, {
    status: 'active',
    minStepIndex: 5,
    awaitingTurnWakeup: false,
  })
})

test('readTurnBoundaryBefore handles missing, 0-byte, and empty-row transcripts and caches per beforeStep', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })

    await mkdir(join(f.root, parent, '.system_generated/logs'), {
      recursive: true,
    })
    await writeFile(f.path(parent), '')
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })

    await writeFile(f.path(parent), '\n')
    assert.deepEqual(await readTurnBoundaryBefore(f.root, parent, 5, cache), {
      boundary: -1,
      ready: false,
    })
    assert.equal(cache.has(`${parent}:5`), true)

    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      done(3, 't2'),
    ])
    const b1 = await readTurnBoundaryBefore(f.root, parent, 2, cache)
    const b2 = await readTurnBoundaryBefore(f.root, parent, 4, cache)
    assert.deepEqual(b1, { boundary: 0, ready: true })
    assert.deepEqual(b2, { boundary: 2, ready: true })
    assert.equal(cache.has(`${parent}:2`), true)
    assert.equal(cache.has(`${parent}:4`), true)
  } finally {
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
    await Promise.all(registry.cancelAll())
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

test('readTurnBoundaryBefore invalidates cache when a partial wakeup file is completed in place', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      done(3, 't2'),
    ])
    await f.put(child, [
      message(0, parent, 'init'),
      message(1, parent, 'wake t2'),
      done(2, 'child done'),
    ])
    const msgDir = join(f.root, child, '.system_generated/messages')
    await mkdir(msgDir, { recursive: true })
    await f.deliver(child, parent, 'wake t2', true, 'wake-c', {
      conversationId: parent,
      stepIndex: 3,
      callId: 'send-c',
      name: 'send_message',
    })
    const msgPath = join(msgDir, 'init-c.json')
    await writeFile(msgPath, '{')

    const first = await readTurnBoundaryBefore(f.root, child, 2, cache)
    assert.deepEqual(first, { boundary: -1, ready: true })

    // Complete only init-c.json in place without adding/removing files in messages/.
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })

    const second = await readTurnBoundaryBefore(f.root, child, 2, cache)
    assert.deepEqual(second, { boundary: 1, ready: true })
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore resolves mixed tagged and untagged intermediate ancestor boundaries', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'tagged wake t2', 3),
      done(6, 't2'),
    ])
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(child, parent, 'untagged wake', true, 'u1-c')
    await f.deliver(child, parent, 'tagged wake t2', true, 't2-c', {
      conversationId: parent,
      stepIndex: 4,
      callId: 'send-c',
      name: 'send_message',
    })
    await f.put(child, [
      message(0, parent, 'init'),
      message(1, parent, 'untagged wake'),
      message(2, parent, 'tagged wake t2'),
      done(3, 'child done'),
    ])

    const resolved = await readTurnBoundaryBefore(f.root, child, 3, cache)
    assert.deepEqual(resolved, { boundary: 2, ready: true })
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore anchors at first Turn-2 wakeup when tagged Turn-2 precedes untagged Turn-2', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      done(1, 't1'),
      { step_index: 2, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'tagged t2', 3),
      ...report(child, 'untagged t2', 6),
      done(9, 't2'),
    ])
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(child, parent, 'tagged t2', true, 't2-1', {
      conversationId: parent,
      stepIndex: 4,
      callId: 'send-1',
      name: 'send_message',
    })
    await f.deliver(child, parent, 'untagged t2', true, 'u2-2')
    await f.put(child, [
      message(0, parent, 'init'),
      message(1, parent, 'tagged t2'),
      done(2, 'work in t2'),
      message(3, parent, 'untagged t2'),
      done(4, 'child done'),
    ])

    const resolved = await readTurnBoundaryBefore(f.root, child, 4, cache)
    assert.deepEqual(resolved, { boundary: 1, ready: true })
  } finally {
    await f.close()
  }
})

test('readTurnBoundaryBefore resolves fully untagged intermediate ancestor boundaries', async () => {
  const f = await fixture()
  try {
    const cache: TurnBoundaryCache = new Map()
    await f.put(parent, [
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      ...report(child, 'untagged t1', 1),
      done(4, 't1'),
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'untagged t2', 6),
      done(9, 't2'),
    ])
    await f.deliver(child, parent, 'init child', true, 'init-c', {
      conversationId: parent,
      stepIndex: 0,
      callId: 'spawn-c',
      name: 'invoke_subagent',
    })
    await f.deliver(child, parent, 'untagged t1', true, 'u1')
    await f.deliver(child, parent, 'untagged t2', true, 'u2')
    await f.put(child, [
      message(0, parent, 'init'),
      message(1, parent, 'untagged t1'),
      message(2, parent, 'untagged t2'),
      done(3, 'child done'),
    ])

    const resolved = await readTurnBoundaryBefore(f.root, child, 3, cache)
    assert.deepEqual(resolved, { boundary: 2, ready: true })
  } finally {
    await f.close()
  }
})

test('watchBrain excludes Turn-1 failed sibling descendant even after unrelated later root Turn-3 USER_INPUT', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const oldGc = '44444444-4444-4444-8444-444444444444'
    const events: ProviderEvent[] = []
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

    handle = watchBrain(f.root, grandchild, 'gc', (e) => events.push(e), 50)
    await handle.ready
    assert.ok(
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
      'fresh descendant watcher must complete without inheriting Turn-1 failed sibling',
    )
    assert.equal(
      events.some((e) => e.type === 'session.turn' && e.state === 'failed'),
      false,
      'unrelated later root Turn-3 USER_INPUT must not leak old Turn-1 failure',
    )
  } finally {
    handle?.()
    await f.close()
  }
})

test('watchBrain excludes Turn-1 failed sibling when Root Turn 3 wakes child B without waking grandchild C', async () => {
  const f = await fixture()
  let handle: BrainWatchHandle | undefined
  try {
    const oldGc = '44444444-4444-4444-8444-444444444444'
    const events: ProviderEvent[] = []
    await f.put(parent, [
      ...invoke(child),
      { step_index: 5, type: 'USER_INPUT', content: 'turn 2' },
      ...report(child, 'tagged t2', 6),
      ...report(child, 'untagged t2', 9),
      done(12, 't2'),
      { step_index: 13, type: 'USER_INPUT', content: 'turn 3' },
      ...report(child, 'wake b in t3', 14),
      done(17, 't3'),
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
    await f.deliver(child, parent, 'wake b in t3', true, 't3-c', {
      conversationId: parent,
      stepIndex: 15,
      callId: 'send-c-3',
      name: 'send_message',
    })
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
      message(11, parent, 'wake b in t3'),
      done(12, 'child t3 without waking gc'),
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

    handle = watchBrain(
      f.root,
      grandchild,
      'gc',
      (e) => events.push(e),
      50,
      () => true,
    )
    await handle.ready
    assert.ok(
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
      'fresh descendant watcher must complete even after Root Turn 3 wakes child B',
    )
    assert.equal(
      events.some((e) => e.type === 'session.turn' && e.state === 'failed'),
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

    let parentSettled = false
    running = settleBrainTurn(f.root, parent, 'p', () => {}, {
      signal: abort.signal,
      minStepIndex: -1,
      pollIntervalMs: 5,
    }).then((res) => {
      parentSettled = true
      return res
    })
    await delay(30)
    assert.equal(parentSettled, false)

    await appendFile(
      f.path(child),
      encode([
        message(5, grandchild, 'grandchild report'),
        ...report(parent, 'child final report', 6),
      ]),
    )
    await f.deliver(parent, child, 'child final report', true, 'c-report')
    await appendFile(
      f.path(parent),
      encode([message(5, child, 'child final report'), done(6, 'all done')]),
    )
    await waitUntil(() => parentSettled)
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

test('question-continuation USER_INPUT preserves turnStartStep and touchedSubagents across replay and live continuation', () => {
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

  // 1. Replay path (hasPendingQuestion() === true without markQuestionContinuation):
  const replayNode = new BrainNode(parent, 'p', {
    allowInitialSpawnPrompt: false,
  })
  for (const row of spawnAndAsk(10)) replayNode.ingest(row)
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

  // 2. Live continuation path (consumePendingQuestion() + markQuestionContinuation(true)):
  const liveNode = new BrainNode(parent, 'p', {
    allowInitialSpawnPrompt: false,
  })
  for (const row of spawnAndAsk(20)) liveNode.ingest(row)
  assert.ok(liveNode.projector.consumePendingQuestion())
  assert.equal(liveNode.projector.hasPendingQuestion(), false)
  liveNode.markQuestionContinuation(true)
  liveNode.ingest({
    step_index: 23,
    type: 'USER_INPUT',
    content: 'Yes',
  })
  assert.equal(
    liveNode.turnStartStep,
    20,
    'live synthetic question continuation must preserve turnStartStep',
  )
  assert.deepEqual([...liveNode.projector.touchedSubagentIds()], [child])

  // 3. Stateless replay of woken ask_question preserves node.wokeUp and works when a sibling tool precedes ask_question.
  const wokenReplayNode = new BrainNode(parent, 'p', {
    allowInitialSpawnPrompt: false,
  })
  for (const row of [
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
  ] satisfies Row[]) {
    wokenReplayNode.ingest(row)
  }
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
    for (const [id, stepIndex] of [
      ['first', 4],
      ['second', 7],
    ] as const)
      await f.deliver(child, parent, id, true, id, {
        conversationId: parent,
        stepIndex,
        name: 'send_message',
        callId: id,
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
