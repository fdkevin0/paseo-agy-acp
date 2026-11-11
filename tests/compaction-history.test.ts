import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'
import {
  ProviderEventSchema,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { Timeline } from '../server/mapping.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'

const native = '11111111-1111-4111-8111-111111111111'
const child = '22222222-2222-4222-8222-222222222222'
const response = (step_index: number, content: string) => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  content,
})
const checkpoint = (step_index: number) => ({
  step_index,
  type: 'CHECKPOINT',
  status: 'DONE',
  created_at: '2026-01-01T00:00:01Z',
})
const text = (value: string) => ({
  sessionUpdate: 'agent_message_chunk',
  content: { type: 'text', text: value },
})
const read = {
  sessionUpdate: 'tool_call',
  toolCallId: 'read-example',
  title: 'read_file',
  status: 'completed',
  rawInput: { Path: '/example' },
  rawOutput: 'read',
}
const brainRead = [
  {
    step_index: 3,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      { id: 'read-example', name: 'read_file', args: { Path: '/example' } },
    ],
  },
  { step_index: 4, type: 'GENERIC', status: 'DONE', content: 'read' },
]
const rows = [
  response(1, 'BEFORE'),
  checkpoint(2),
  ...brainRead,
  response(5, 'AFTER'),
]
// Synthetic SDK wire fixtures; no private conversation or guessed native fields.
async function replay(
  rows: object[],
  updates: object[],
  history: 'replay' | 'skip' = 'replay',
  childRows?: object[],
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-compaction-history-'))
  const root = join(home, '.gemini/antigravity-cli/brain')
  async function put(id: string, rows: object[]) {
    const dir = join(root, id, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, 'transcript_full.jsonl'),
      rows.map((row) => JSON.stringify(row) + '\n').join(''),
    )
  }
  await put(native, rows)
  await writeFile(join(home, 'history-updates.json'), JSON.stringify(updates))
  if (childRows) {
    await put(child, childRows)
    const dir = join(root, native, '.system_generated/subagents')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, `${child}.json`),
      JSON.stringify({
        conversationId: child,
        state: 'SUBAGENT_STATE_ALIVE',
        spawnStepIndex: 4,
      }),
    )
  }
  const events: ProviderEvent[] = []
  const manager = new SessionManager({
    launch: {
      command: process.execPath,
      args: [
        fileURLToPath(new URL('./compaction-history-acp.ts', import.meta.url)),
      ],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['session.subsession'],
    emit: (event) => events.push(structuredClone(event)),
  })
  try {
    await manager.open({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history,
      persistence: { version: 1, data: { sessionId: native } },
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    return events
  } finally {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  }
}
const timeline = (events: ProviderEvent[]) =>
  events.flatMap((event) =>
    event.type === 'timeline.item' && event.sessionId === 's' ? [event] : [],
  )
const labels = (events: ProviderEvent[]) =>
  timeline(events).map(({ item }) =>
    item.type === 'assistant_message' ? item.text : item.type,
  )

test('SDK load places compaction between its exact history anchors and preserves ACP tool data', async () => {
  const events = await replay(rows, [text('BEFORE'), read, text('AFTER')])
  const order = labels(events)
  assert.ok(order.indexOf('BEFORE') < order.indexOf('compaction'))
  assert.ok(order.indexOf('compaction') < order.indexOf('tool_call'))
  assert.ok(order.indexOf('tool_call') < order.indexOf('AFTER'))
  const compact = timeline(events).find(
    ({ item }) => item.type === 'compaction',
  )!
  assert.equal(compact.timestamp, '2026-01-01T00:00:01Z')
  assert.equal(compact.item.id, 's:step:2:compaction')
  const tool = timeline(events).find(
    ({ item }) => item.type === 'tool_call',
  )!.item
  assert.ok(tool.type === 'tool_call')
  assert.equal(tool.callId, 'read-example')
  assert.equal(tool.detail.type, 'read')
  assert.deepEqual(
    await replay(rows, [text('BEFORE'), read, text('AFTER')], 'skip').then(
      timeline,
    ),
    [],
  )
})

test('ACP-only content blocks one gap, not other provable checkpoints, and nothing is dropped', async () => {
  const extra = {
    sessionUpdate: 'agent_thought_chunk',
    content: { type: 'text', text: 'ACP-only reasoning' },
  }
  const events = await replay(
    [...rows, checkpoint(6)],
    [text('BEFORE'), extra, read, text('AFTER')],
  )
  const items = timeline(events).map(({ item }) => item)
  assert.deepEqual(
    items
      .filter((item) => item.type === 'assistant_message')
      .map((item) => item.text),
    ['BEFORE', 'AFTER'],
  )
  assert.ok(
    items.some(
      (item) => item.type === 'reasoning' && item.text === 'ACP-only reasoning',
    ),
  )
  assert.equal(items.filter((item) => item.type === 'tool_call').length, 1)
  assert.deepEqual(
    items.filter((item) => item.type === 'compaction').map((item) => item.id),
    ['s:step:6:compaction'],
  )
  const notices = items.filter((item) => item.type === 'notification')
  assert.equal(notices.length, 1)
  assert.equal(notices[0].id, 's:step:2:compaction:unlocated')
  assert.equal(notices[0].level, 'info')
  assert.match(notices[0].message, /position is unknown/)
  const notice = timeline(events).find(
    ({ item }) => item.type === 'notification',
  )!
  assert.equal(notice.timestamp, undefined)
  assert.ok(ProviderEventSchema.safeParse(notice).success)
})

test('duplicate full text, fragmented snapshots, partial text and missing native call IDs use explicit fallback', async () => {
  const repeatedRows = [
    response(1, 'Same'),
    checkpoint(2),
    { ...response(3, 'Same'), thinking: 'separator' },
  ]
  const missingIdRows = rows.map((row) =>
    row === brainRead[0]
      ? {
          ...row,
          tool_calls: [{ name: 'read_file', args: { Path: '/example' } }],
        }
      : row,
  )
  for (const [historyRows, updates, expectedTexts] of [
    [
      repeatedRows,
      [
        text('Same'),
        {
          sessionUpdate: 'agent_thought_chunk',
          content: { type: 'text', text: 'separator' },
        },
        text('Same'),
      ],
      ['Same', 'Same'],
    ],
    [
      rows,
      [text('BE'), text('FORE'), read, text('AFTER')],
      ['BE', 'BEFORE', 'AFTER'],
    ],
    [
      rows,
      [text('BEFORE (partial)'), read, text('AFTER')],
      ['BEFORE (partial)', 'AFTER'],
    ],
    [missingIdRows, [text('BEFORE'), read, text('AFTER')], ['BEFORE', 'AFTER']],
    [[checkpoint(1)], [], []],
  ] as const) {
    const events = await replay([...historyRows], [...updates])
    const items = timeline(events).map(({ item }) => item)
    assert.equal(items.filter((item) => item.type === 'compaction').length, 0)
    assert.equal(items.filter((item) => item.type === 'notification').length, 1)
    assert.deepEqual(
      items
        .filter((item) => item.type === 'assistant_message')
        .map((item) => item.text),
      expectedTexts,
    )
  }
})

test('conflicting anchors do not reorder ACP content or guess a checkpoint position', async () => {
  const conflicting = [
    response(1, 'BEFORE'),
    response(2, 'AFTER'),
    checkpoint(3),
    ...brainRead.map((row) => ({ ...row, step_index: row.step_index + 1 })),
  ]
  const events = await replay(conflicting, [
    text('BEFORE'),
    read,
    text('AFTER'),
  ])
  assert.deepEqual(labels(events), [
    'BEFORE',
    'tool_call',
    'AFTER',
    'notification',
  ])
})

test('reliable native call IDs outrank conflicting text anchors', async () => {
  const a = { ...read, toolCallId: 'a' }
  const b = { ...read, toolCallId: 'b' }
  const tool = (step_index: number, id: string) => ({
    step_index,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ id, name: 'read_file', args: { Path: '/example' } }],
  })
  const result = (step_index: number) => ({
    step_index,
    type: 'GENERIC',
    status: 'DONE',
    content: 'read',
  })
  const events = await replay(
    [
      tool(0, 'a'),
      result(1),
      response(2, 'X'),
      checkpoint(3),
      tool(4, 'b'),
      result(5),
    ],
    [text('X'), a, b],
  )
  assert.deepEqual(labels(events), [
    'X',
    'tool_call',
    'compaction',
    'tool_call',
  ])
  const tools = timeline(events)
    .filter(({ item }) => item.type === 'tool_call')
    .map(({ item }) => item)
  assert.deepEqual(
    tools.map((item) => item.type === 'tool_call' && item.callId),
    ['a', 'b'],
  )
})

test('unknown ACP tool details, errors and identity survive a conflicting Brain terminal result', async () => {
  const failed = {
    ...read,
    title: 'Vendor.Lookup',
    status: 'failed',
    rawInput: { data: ['example'] },
    rawOutput: {
      structuredContent: { valid: false },
      content: [{ type: 'text', text: 'failure detail' }],
    },
  } as const
  const nativeRows = [
    response(1, 'BEFORE'),
    checkpoint(2),
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      tool_calls: [
        { id: 'read-example', name: failed.title, args: failed.rawInput },
      ],
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      content: 'different Brain result',
    },
    response(5, 'AFTER'),
  ]
  const events = await replay(nativeRows, [
    text('BEFORE'),
    failed,
    text('AFTER'),
  ])
  const tools = timeline(events)
    .filter(({ item }) => item.type === 'tool_call')
    .map(({ item }) => item)
  assert.equal(tools.length, 1)
  const tool = tools[0]
  assert.ok(tool.type === 'tool_call')
  const expected = new Timeline('s').update({
    ...failed,
    sessionUpdate: 'tool_call',
  })
  assert.ok(expected?.type === 'tool_call')
  assert.deepEqual(tool, expected)
  assert.equal(tool.status, 'failed')
  assert.equal(tool.error, JSON.stringify(failed.rawOutput))
  assert.deepEqual(labels(events), [
    'BEFORE',
    'compaction',
    'tool_call',
    'AFTER',
  ])
})

test('unlocated checkpoint revisions produce one standard notice per replay construction', async () => {
  for (let repeat = 0; repeat < 2; repeat++) {
    const events = await replay(
      [
        { ...checkpoint(1), content: 'summary A' },
        { ...checkpoint(1), content: 'summary B' },
      ],
      [text('unmatched ACP history')],
    )
    assert.deepEqual(labels(events), ['unmatched ACP history', 'notification'])
  }
})

test('loaded child checkpoints keep their own session while root history interleaves', async () => {
  const spawn = {
    sessionUpdate: 'tool_call',
    toolCallId: 'spawn-example',
    title: 'invoke_subagent',
    status: 'completed',
    rawOutput: { conversationId: child },
  }
  const root = [
    response(1, 'BEFORE'),
    checkpoint(2),
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ id: 'spawn-example', name: 'invoke_subagent', args: {} }],
    },
    {
      step_index: 4,
      type: 'GENERIC',
      status: 'DONE',
      content: JSON.stringify({ conversationId: child }),
    },
    response(5, 'AFTER'),
  ]
  const events = await replay(
    root,
    [text('BEFORE'), spawn, text('AFTER')],
    'replay',
    [response(0, 'child answer'), checkpoint(1)],
  )
  assert.deepEqual(labels(events), [
    'BEFORE',
    'compaction',
    'tool_call',
    'AFTER',
  ])
  assert.ok(
    events.some(
      (event) =>
        event.type === 'timeline.item' &&
        event.sessionId === `s:child:${child}` &&
        event.item.type === 'compaction',
    ),
  )
  const item = timeline(events).find(
    ({ item }) => item.type === 'tool_call',
  )!.item
  assert.ok(item.type === 'tool_call' && item.detail.type === 'sub_agent')
  assert.equal(item.detail.childSessionId, `s:child:${child}`)
})

test('missing ACP results recover from Brain without losing ACP parameters', async () => {
  const partial = {
    ...read,
    rawInput: { Path: '/acp-path', StartLine: 7 },
    rawOutput: undefined,
  }
  const events = await replay(rows, [text('BEFORE'), partial, text('AFTER')])
  const tools = timeline(events)
    .filter(({ item }) => item.type === 'tool_call')
    .map(({ item }) => item)
  assert.equal(
    tools.length,
    2,
    'original ACP snapshot and the missing-result completion both survive',
  )
  const recovered = tools.at(-1)!
  assert.ok(recovered.type === 'tool_call' && recovered.detail.type === 'read')
  assert.equal(recovered.detail.filePath, '/acp-path')
  assert.equal(recovered.detail.offset, 7)
  assert.equal(recovered.detail.content, 'read')
  assert.equal(recovered.callId, 'read-example')
})
