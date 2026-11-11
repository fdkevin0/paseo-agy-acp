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
import { zSessionUpdate } from '../node_modules/@agentclientprotocol/sdk/dist/schema/zod.gen.js'

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
  metadata: readonly { stepIndex: number; callId: string }[] = [],
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
  if (metadata.length) {
    const dir = join(root, native, '.system_generated/messages')
    await mkdir(dir, { recursive: true })
    const read: Record<string, boolean> = {}
    for (const [index, entry] of metadata.entries()) {
      const id = `metadata-${index}`
      read[id] = true
      await writeFile(
        join(dir, `${id}.json`),
        JSON.stringify({
          id,
          sender: 'user',
          recipient: native,
          content: 'fixture',
          sourceMetadata: {
            tool: {
              conversationId: native,
              stepIndex: entry.stepIndex,
              toolCall: {
                id: entry.callId,
                name: 'read_file',
                argumentsJson: '{}',
              },
            },
          },
        }),
      )
    }
    await writeFile(join(dir, 'read.json'), JSON.stringify(read))
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

test('a positional alias never gains native provenance from a later raw or metadata ID', async () => {
  const tool = (step_index: number, id?: string) => ({
    step_index,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [
      { ...(id ? { id } : {}), name: 'read_file', args: { Path: '/brain' } },
    ],
  })
  const result = {
    step_index: 1,
    type: 'GENERIC',
    status: 'DONE',
    content: 'anonymous result',
  }
  const acp = {
    ...read,
    toolCallId: 'b',
    rawInput: { Path: '/acp' },
    rawOutput: 'ACP result',
  }
  for (const laterIdentity of ['tool_id', 'step_metadata'] as const) {
    const nativeRows = [
      tool(0),
      result,
      checkpoint(2),
      tool(3, laterIdentity === 'tool_id' ? 'b' : undefined),
    ]
    const events = await replay(
      nativeRows,
      [acp],
      'replay',
      undefined,
      laterIdentity === 'step_metadata' ? [{ stepIndex: 4, callId: 'b' }] : [],
    )
    const items = timeline(events).map(({ item }) => item)
    assert.equal(items.filter((item) => item.type === 'compaction').length, 0)
    assert.equal(items.filter((item) => item.type === 'notification').length, 1)
    const actual = items.find((item) => item.type === 'tool_call')!
    assert.ok(actual.type === 'tool_call' && actual.detail.type === 'read')
    assert.equal(actual.detail.content, 'ACP result')
  }
  const control = await replay(
    [tool(0, 'a'), result, checkpoint(2), tool(3, 'b')],
    [acp],
  )
  assert.equal(
    timeline(control).filter(({ item }) => item.type === 'compaction').length,
    0,
  )
  assert.equal(
    timeline(control).filter(({ item }) => item.type === 'notification').length,
    1,
  )
})

test('native provenance survives terminal projection and accepts exact step metadata and native synthetic-looking IDs', async () => {
  for (const source of ['tool_id', 'step_metadata'] as const) {
    const id = 'tool-0-0'
    const nativeRows = [
      {
        step_index: 0,
        type: 'PLANNER_RESPONSE',
        tool_calls: [
          {
            ...(source === 'tool_id' ? { id } : {}),
            name: 'read_file',
            args: { Path: '/brain' },
          },
        ],
      },
      { step_index: 1, type: 'GENERIC', status: 'DONE', content: 'result' },
      checkpoint(2),
    ]
    const events = await replay(
      nativeRows,
      [{ ...read, toolCallId: id }],
      'replay',
      undefined,
      source === 'step_metadata' ? [{ stepIndex: 1, callId: id }] : [],
    )
    assert.deepEqual(labels(events), ['tool_call', 'compaction'])
  }
})

test('running empty content through multiple updates cannot suppress a missing Brain result', async () => {
  const nativeRows = [
    response(0, 'BEFORE'),
    checkpoint(1),
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ id: 'b', name: 'read_file', args: { Path: '/brain' } }],
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Brain recovered',
    },
    response(4, 'AFTER'),
  ]
  for (const content of [undefined, []]) {
    const events = await replay(nativeRows, [
      text('BEFORE'),
      {
        ...read,
        toolCallId: 'b',
        status: 'in_progress',
        rawInput: { Path: '/acp', StartLine: 7 },
        rawOutput: undefined,
        ...(content ? { content } : {}),
      },
      ...(content
        ? [{ sessionUpdate: 'tool_call_update', toolCallId: 'b', content: [] }]
        : []),
      {
        sessionUpdate: 'tool_call_update',
        toolCallId: 'b',
        status: 'completed',
      },
      text('AFTER'),
    ])
    const recovered = timeline(events)
      .map(({ item }) => item)
      .find(
        (item) =>
          item.type === 'tool_call' &&
          item.status === 'completed' &&
          item.detail.type === 'read' &&
          item.detail.content === 'Brain recovered',
      )
    assert.ok(
      recovered?.type === 'tool_call' && recovered.detail.type === 'read',
    )
    assert.equal(recovered.detail.filePath, '/acp')
    assert.equal(recovered.detail.offset, 7)
  }
})

test('result channel replacements track running placeholders independently from explicit terminal values', async () => {
  const nativeRows = [
    response(0, 'BEFORE'),
    checkpoint(1),
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ id: 'b', name: 'read_file', args: { Path: '/brain' } }],
    },
    {
      step_index: 3,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Brain recovered',
    },
    response(4, 'AFTER'),
  ]
  const update = (fields: object) => ({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'b',
    ...fields,
  })
  const start = {
    ...read,
    toolCallId: 'b',
    status: 'in_progress',
    rawInput: { Path: '/acp', StartLine: 7 },
    rawOutput: undefined,
    content: [],
  }
  const cases = [
    {
      middle: [
        update({
          content: [
            {
              type: 'content',
              content: { type: 'text', text: 'temporary progress' },
            },
          ],
        }),
        update({ content: [] }),
      ],
      final: {},
      expected: 'Brain recovered',
    },
    {
      middle: [
        update({ rawOutput: 'temporary raw' }),
        update({ rawOutput: null }),
      ],
      final: {},
      expected: 'temporary raw',
    },
    {
      middle: [update({ rawOutput: 'ACP raw' }), update({ content: [] })],
      final: {},
      expected: 'ACP raw',
    },
    { middle: [], final: { content: [] }, expected: undefined },
    { middle: [], final: { rawOutput: null }, expected: 'Brain recovered' },
    {
      middle: [],
      final: { status: 'failed', rawOutput: null },
      expected: 'Brain recovered',
    },
    {
      middle: [],
      final: { status: 'failed', rawOutput: 'ACP error' },
      expected: 'ACP error',
    },
    { middle: [], final: { rawOutput: '' }, expected: '' },
    { middle: [], final: { rawOutput: 'ACP final' }, expected: 'ACP final' },
    {
      middle: [],
      final: { rawOutput: { structuredContent: { valid: true } } },
      expected: JSON.stringify({ structuredContent: { valid: true } }),
    },
    {
      middle: [],
      final: {
        content: [
          {
            type: 'content',
            content: { type: 'text', text: 'ACP final content' },
          },
        ],
      },
      expected: 'ACP final content',
    },
  ]
  for (const { middle, final, expected } of cases) {
    const events = await replay(nativeRows, [
      text('BEFORE'),
      start,
      ...middle,
      update({ status: 'completed', ...final }),
      text('AFTER'),
    ])
    const completed = timeline(events)
      .map(({ item }) => item)
      .filter((item) => item.type === 'tool_call' && item.status !== 'running')
      .at(-1)
    assert.ok(
      completed?.type === 'tool_call' && completed.detail.type === 'read',
    )
    assert.equal(
      completed.status,
      'status' in final ? final.status : 'completed',
    )
    assert.equal(completed.detail.filePath, '/acp')
    assert.equal(completed.detail.offset, 7)
    assert.equal(completed.detail.content, expected)
  }
})

test('ACP v1 initial null is missing result data and never suppresses history recovery', async (t) => {
  for (const status of ['in_progress', 'completed', 'failed'] as const)
    await t.test(status, async () => {
      const events = await replay(rows, [
        text('BEFORE'),
        {
          ...read,
          status,
          rawInput: { Path: '/acp', StartLine: 7 },
          rawOutput: null,
        },
        ...(status === 'in_progress'
          ? [
              {
                sessionUpdate: 'tool_call_update',
                toolCallId: read.toolCallId,
                status: 'completed',
                rawOutput: null,
              },
            ]
          : []),
        text('AFTER'),
      ])
      const item = timeline(events)
        .map(({ item }) => item)
        .findLast((item) => item.type === 'tool_call')
      assert.ok(item?.type === 'tool_call' && item.detail.type === 'read')
      assert.equal(item.callId, read.toolCallId)
      assert.equal(item.id, `s:tool:${read.toolCallId}`)
      assert.equal(item.status, status === 'failed' ? 'failed' : 'completed')
      assert.equal(item.error, status === 'failed' ? 'Tool failed' : null)
      assert.equal(item.detail.filePath, '/acp')
      assert.equal(item.detail.offset, 7)
      assert.equal(item.detail.content, 'read')
      assert.equal(
        timeline(events).filter(({ item }) => item.type === 'compaction')
          .length,
        1,
      )
    })
})

test('ACP v1 running null cannot erase raw provenance or native input parameters before completion', async (t) => {
  for (const final of [{}, { rawInput: null, rawOutput: null }] as const)
    await t.test(JSON.stringify(final), async () => {
      const events = await replay(rows, [
        text('BEFORE'),
        {
          ...read,
          status: 'in_progress',
          rawInput: { Path: '/acp', StartLine: 7 },
          rawOutput: 'ACP retained',
        },
        {
          sessionUpdate: 'tool_call_update',
          toolCallId: read.toolCallId,
          rawInput: null,
          rawOutput: null,
        },
        {
          sessionUpdate: 'tool_call_update',
          toolCallId: read.toolCallId,
          status: 'completed',
          ...final,
        },
        text('AFTER'),
      ])
      const item = timeline(events)
        .map(({ item }) => item)
        .findLast((item) => item.type === 'tool_call')
      assert.ok(item?.type === 'tool_call' && item.detail.type === 'read')
      assert.equal(item.status, 'completed')
      assert.equal(item.detail.content, 'ACP retained')
      assert.equal(item.detail.filePath, '/acp')
      assert.equal(item.detail.offset, 7)
    })
})

test('non-null raw JSON remains authoritative through null and independent content updates in live and history', async (t) => {
  const name = 'Vendor.custom'
  for (const output of [false, 0, '', [], {}, { nested: null }])
    await t.test(JSON.stringify(output), async () => {
      const updates = [
        {
          ...read,
          title: name,
          status: 'in_progress',
          rawInput: { nested: null },
          rawOutput: 'old',
        },
        {
          sessionUpdate: 'tool_call_update',
          toolCallId: read.toolCallId,
          rawOutput: output,
          content: [],
        },
        {
          sessionUpdate: 'tool_call_update',
          toolCallId: read.toolCallId,
          rawInput: null,
          rawOutput: null,
          content: null,
        },
        {
          sessionUpdate: 'tool_call_update',
          toolCallId: read.toolCallId,
          status: 'completed',
        },
      ].map((update) => zSessionUpdate.parse(update))
      const live = new Timeline('live')
      const liveItem = updates.map((update) => live.update(update)).at(-1)
      assert.ok(
        liveItem?.type === 'tool_call' && liveItem.detail.type === 'unknown',
      )
      assert.deepEqual(liveItem.detail.input, { nested: null })
      assert.deepEqual(liveItem.detail.output, output)
      const nativeRows = rows.map((row) =>
        row.type === 'PLANNER_RESPONSE' && 'tool_calls' in row
          ? {
              ...row,
              tool_calls: [
                { id: read.toolCallId, name, args: { from: 'Brain' } },
              ],
            }
          : row,
      )
      const events = await replay(nativeRows, [
        text('BEFORE'),
        ...updates,
        text('AFTER'),
      ])
      const item = timeline(events)
        .map(({ item }) => item)
        .findLast((item) => item.type === 'tool_call')
      assert.ok(item?.type === 'tool_call' && item.detail.type === 'unknown')
      assert.equal(item.callId, read.toolCallId)
      assert.equal(item.status, 'completed')
      assert.deepEqual(item.detail.input, { nested: null })
      assert.deepEqual(item.detail.output, output)
    })
})

test('a new full ACP snapshot resets both result channels instead of inheriting old null-retained data', async () => {
  const name = 'Vendor.custom'
  const nativeRows = rows.map((row) =>
    row.type === 'PLANNER_RESPONSE' && 'tool_calls' in row
      ? { ...row, tool_calls: [{ id: read.toolCallId, name, args: {} }] }
      : row,
  )
  const events = await replay(nativeRows, [
    text('BEFORE'),
    {
      ...read,
      title: name,
      rawInput: { old: true },
      rawOutput: 'old raw',
      content: [
        { type: 'content', content: { type: 'text', text: 'old content' } },
      ],
    },
    { ...read, title: name, rawInput: null, rawOutput: null },
    text('AFTER'),
  ])
  const item = timeline(events)
    .map(({ item }) => item)
    .findLast((item) => item.type === 'tool_call')
  assert.ok(item?.type === 'tool_call' && item.detail.type === 'unknown')
  assert.deepEqual(item.detail.input, null)
  assert.equal(item.detail.output, 'read')
})

test('null-result history recovery remains intact when the next turn cancels after a complete compaction prefix', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-null-history-cancel-'))
  const dir = join(
    home,
    '.gemini/antigravity-cli/brain',
    native,
    '.system_generated/logs',
  )
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  const encode = (rows: object[]) =>
    rows.map((row) => JSON.stringify(row) + '\n').join('')
  await writeFile(path, encode(rows))
  await writeFile(
    join(home, 'history-updates.json'),
    JSON.stringify([
      text('BEFORE'),
      { ...read, rawInput: { Path: '/acp', StartLine: 7 }, rawOutput: null },
      text('AFTER'),
    ]),
  )
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
    capabilities: ['prompt.message'],
    emit: (event) => events.push(structuredClone(event)),
  })
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
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
    history: 'replay',
    persistence: { version: 1, data: { sessionId: native } },
    config: { cwd: home, env: {}, mcpServers: {}, settings: {}, persist: true },
  })
  const restored = timeline(events)
    .map(({ item }) => item)
    .findLast((item) => item.type === 'tool_call')
  assert.ok(restored?.type === 'tool_call' && restored.detail.type === 'read')
  assert.equal(restored.detail.content, 'read')
  assert.equal(restored.detail.filePath, '/acp')
  assert.equal(restored.detail.offset, 7)
  t.mock.method(session.transport.rpc, 'prompt', async () => {
    await writeFile(
      path,
      encode([
        ...rows,
        { step_index: 6, type: 'USER_INPUT', content: 'next' },
        {
          step_index: 7,
          type: 'PLANNER_RESPONSE',
          input_tokens: 100,
          output_tokens: 2,
        },
        checkpoint(8),
      ]) + '{"step_index":9',
    )
    return { stopReason: 'cancelled' as const }
  })
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'next',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'next' }] },
    },
  })
  assert.equal(session.state.lastKnownStepIndex, 8)
  const usage = events.findLast((event) => event.type === 'session.usage')
  assert.ok(usage?.type === 'session.usage')
  assert.equal(usage.usage.contextWindowUsedTokens, 102)
  assert.equal(
    usage.usage.inputTokens,
    undefined,
    'context-only cancellation does not create a bill',
  )
  assert.deepEqual(
    events.flatMap((event) =>
      event.type === 'session.turn' && event.state !== 'started'
        ? [event.state]
        : [],
    ),
    ['canceled'],
  )
  assert.deepEqual(
    timeline(events)
      .map(({ item }) => item)
      .findLast((item) => item.type === 'tool_call'),
    restored,
  )
})
