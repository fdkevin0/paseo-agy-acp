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
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import {
  BrainTranscriptProjector,
  hasActiveBackgroundWork,
  readLastStepIndex,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
} from '../server/brain.ts'
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
async function until(predicate: () => boolean) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return
    await delay(10)
  }
  assert.fail('Expected event did not arrive')
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
      wakeupUsage: {
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
      taskCallIds: new Map([[bgTaskId, 'late-call-id']]),
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

test('a child watcher does not restart turns when its transcript is rewritten in place', async () => {
  const f = await fixture()
  const events: ProviderEvent[] = []
  let handle: BrainWatchHandle | undefined
  try {
    const rows = [message(0, parent, 'work'), done(1, 'Reply')]
    await f.put(child, rows)
    handle = watchBrain(f.root, child, 'c', (event) => events.push(event), 5)
    await until(() =>
      events.some((e) => e.type === 'session.turn' && e.state === 'completed'),
    )
    const before = events.length
    await f.rewrite(child, [...rows, { step_index: 2, type: 'CHECKPOINT' }])
    await delay(50)
    assert.equal(events.length, before, 'unchanged steps are not new activity')
  } finally {
    handle?.()
    await f.close()
  }
})

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

test('woken ask_question does not trigger false-positive hasActiveBackgroundWork and settles via onQuestion continuation or decline', async () => {
  const f = await fixture()
  try {
    await f.put(parent, [
      ...invoke(child),
      message(5, child, 'Subagent finished'),
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Subagent finished. Please choose:',
        tool_calls: [
          {
            id: 'ask-woken',
            name: 'default_api:ask_question',
            args: {
              questions: [
                {
                  question: 'Which option?',
                  options: ['(Recommended) Option 1', 'Option 2'],
                  is_multi_select: false,
                },
              ],
            },
          },
        ],
      },
    ])
    await f.put(child, [message(0, parent, 'work'), done(1, 'Child done')])

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
          i.callId === 'ask-woken' &&
          i.name === 'ask_question' &&
          i.status === 'completed',
      ),
    )

    // With onQuestion answering and appending a continuation USER_INPUT + PLANNER_RESPONSE, settleBrainTurn continues and emits the final reply
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
          assert.equal(q.questions[0].question, 'Which option?')
          await appendFile(
            f.path(parent),
            encode([
              {
                step_index: 8,
                type: 'USER_INPUT',
                content: '<USER_REQUEST>(Recommended) Option 1</USER_REQUEST>',
              },
              done(9, 'Executed Option 1.'),
            ]),
          )
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
        i.callId === 'ask-woken' &&
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
      ['Subagent finished. Please choose:', 'Executed Option 1.'],
    )

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

    // Same PLANNER_RESPONSE [ask_question, run_command] with no GENERIC rows yet still fires onQuestion immediately
    await f.rewrite(parent, [
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
