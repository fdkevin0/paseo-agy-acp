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
import { BrainTranscriptProjector } from '../server/brain-projector.ts'
import { alignReplayTools } from '../server/history.ts'
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
  const unverifiedInputRows = rows.map((row) =>
    row === brainRead[0]
      ? {
          ...row,
          tool_calls: [{ name: 'read_file', args: { Path: '/different' } }],
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
    [
      unverifiedInputRows,
      [text('BEFORE'), read, text('AFTER')],
      ['BEFORE', 'AFTER'],
    ],
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
  assert.deepEqual(
    item.detail.input,
    {},
    'missing ACP input retains actual Brain args, not the prior ACP snapshot',
  )
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

test('missing or null ACP input retains real Brain read parameters during result recovery', async (t) => {
  const nativeRows = rows.map((row) =>
    row.type === 'PLANNER_RESPONSE' && 'tool_calls' in row
      ? {
          ...row,
          tool_calls: [
            {
              id: read.toolCallId,
              name: 'read_file',
              args: { Path: '/brain', StartLine: 9 },
            },
          ],
        }
      : row.type === 'GENERIC'
        ? { ...row, content: 'Brain recovered' }
        : row,
  )
  for (const input of [{}, { rawInput: null }])
    for (const status of ['completed', 'failed'] as const)
      await t.test(`${JSON.stringify(input)} ${status}`, async () => {
        const packet = zSessionUpdate.parse({
          sessionUpdate: 'tool_call',
          toolCallId: read.toolCallId,
          title: 'read_file',
          status,
          rawOutput: null,
          ...input,
        })
        const events = await replay(nativeRows, [
          text('BEFORE'),
          packet,
          text('AFTER'),
        ])
        const item = timeline(events)
          .map(({ item }) => item)
          .findLast((item) => item.type === 'tool_call')
        assert.ok(item?.type === 'tool_call' && item.detail.type === 'read')
        assert.equal(item.detail.filePath, '/brain')
        assert.equal(item.detail.offset, 9)
        assert.equal(item.detail.content, 'Brain recovered')
        assert.equal(item.callId, read.toolCallId)
        assert.equal(item.id, `s:tool:${read.toolCallId}`)
        assert.equal(item.status, status)
        assert.equal(item.error, status === 'failed' ? 'Tool failed' : null)
      })
})

test('recovered unknown details select actual non-null ACP input, otherwise real Brain JSON including null', async (t) => {
  const brainInput = { from: 'Brain', nested: null }
  const cases: {
    name: string
    packet: object
    brain: unknown
    expected: unknown
  }[] = [
    {
      name: 'missing ACP',
      packet: {},
      brain: brainInput,
      expected: brainInput,
    },
    {
      name: 'null ACP',
      packet: { rawInput: null },
      brain: brainInput,
      expected: brainInput,
    },
    { name: 'actual Brain null', packet: {}, brain: null, expected: null },
    ...[false, 0, '', [], {}, { nested: null }].map((value) => ({
      name: `supplied ${JSON.stringify(value)}`,
      packet: { rawInput: value },
      brain: brainInput,
      expected: value,
    })),
  ]
  for (const entry of cases)
    await t.test(entry.name, async () => {
      const nativeRows = rows.map((row) =>
        row.type === 'PLANNER_RESPONSE' && 'tool_calls' in row
          ? {
              ...row,
              tool_calls: [
                {
                  id: read.toolCallId,
                  name: 'Vendor.custom',
                  args: entry.brain,
                },
              ],
            }
          : row.type === 'GENERIC'
            ? { ...row, content: null }
            : row,
      )
      const packet = zSessionUpdate.parse({
        sessionUpdate: 'tool_call',
        toolCallId: read.toolCallId,
        title: 'Vendor.custom',
        status: 'completed',
        rawOutput: null,
        ...entry.packet,
      })
      const events = await replay(nativeRows, [
        text('BEFORE'),
        packet,
        text('AFTER'),
      ])
      const item = timeline(events)
        .map(({ item }) => item)
        .findLast((item) => item.type === 'tool_call')
      assert.ok(item?.type === 'tool_call' && item.detail.type === 'unknown')
      assert.deepEqual(item.detail.input, entry.expected)
      assert.equal(
        item.detail.output,
        null,
        'actual Brain JSON null is result data, not an ACP patch',
      )
      assert.equal(item.status, 'completed')
    })
})

test('missing ACP input never permits recovered Brain detail to replace a supplied ACP result or error', async (t) => {
  for (const status of ['completed', 'failed'] as const)
    await t.test(status, async () => {
      const output = status === 'failed' ? 'ACP failure' : false
      const nativeRows = rows.map((row) =>
        row.type === 'PLANNER_RESPONSE' && 'tool_calls' in row
          ? {
              ...row,
              tool_calls: [
                {
                  id: read.toolCallId,
                  name: 'Vendor.custom',
                  args: { from: 'Brain' },
                },
              ],
            }
          : row,
      )
      const packet = zSessionUpdate.parse({
        sessionUpdate: 'tool_call',
        toolCallId: read.toolCallId,
        title: 'Vendor.custom',
        status,
        rawInput: null,
        rawOutput: output,
      })
      const events = await replay(nativeRows, [
        text('BEFORE'),
        packet,
        text('AFTER'),
      ])
      const item = timeline(events)
        .map(({ item }) => item)
        .findLast((item) => item.type === 'tool_call')
      assert.ok(item?.type === 'tool_call' && item.detail.type === 'unknown')
      assert.equal(item.detail.input, null)
      assert.equal(item.detail.output, output)
      assert.equal(item.status, status)
      assert.equal(item.error, status === 'failed' ? 'ACP failure' : null)
    })
})

test('real AGY transcript shapes locate checkpoints across anonymous tools, background tasks, wrapped user input, local gaps and forked suffixes', async () => {
  const userChunk = (value: string) => ({
    sessionUpdate: 'user_message_chunk',
    content: { type: 'text', text: value },
  })
  const thoughtChunk = (value: string) => ({
    sessionUpdate: 'agent_thought_chunk',
    content: { type: 'text', text: value },
  })
  const bgTool = {
    sessionUpdate: 'tool_call',
    toolCallId: 'call-bg',
    title: 'run_command',
    status: 'completed',
    rawInput: { CommandLine: 'npm test', WaitMsBeforeAsync: 500 },
    rawOutput: 'pass',
  }
  const nextTool = {
    sessionUpdate: 'tool_call',
    toolCallId: 'call-next',
    title: 'read_file',
    status: 'completed',
    rawInput: { StartLine: 1, Path: '/example' },
    rawOutput: 'ok',
  }
  const nativeRows = [
    {
      step_index: 10,
      type: 'USER_INPUT',
      status: 'DONE',
      content:
        '<USER_REQUEST>\nrun suite\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nlocal time\n</ADDITIONAL_METADATA>',
    },
    checkpoint(11),
    {
      step_index: 12,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      thinking: 'boilerplate thought',
      tool_calls: [
        {
          name: 'default_api:run_command',
          args: { WaitMsBeforeAsync: 500, CommandLine: 'npm test' },
        },
      ],
    },
    {
      step_index: 13,
      type: 'GENERIC',
      status: 'RUNNING',
      content: 'Tool is running as a background task with task id: task-1',
    },
    checkpoint(14),
    {
      step_index: 15,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      thinking: 'boilerplate thought',
      content: 'Waiting for background task.',
    },
    {
      step_index: 16,
      type: 'SYSTEM_MESSAGE',
      status: 'DONE',
      content:
        '[Message] sender=task-1 content=Task id "task-1" finished with result:\npass\n</SYSTEM_MESSAGE>',
    },
    {
      step_index: 17,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [{ name: ' edit_file', args: { Path: '/invalid' } }],
    },
    {
      step_index: 18,
      type: 'GENERIC',
      status: 'INVALID',
      content: 'Error invalid tool call',
    },
    {
      step_index: 19,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          name: 'default_api:read_file',
          args: { Path: '/example', StartLine: 1 },
        },
      ],
    },
    { step_index: 20, type: 'GENERIC', status: 'DONE', content: 'ok' },
    checkpoint(21),
  ]
  const parentPrefixTool = {
    sessionUpdate: 'tool_call',
    toolCallId: 'call-parent-prefix',
    title: 'read_file',
    status: 'completed',
    rawInput: { Path: '/parent-only' },
    rawOutput: 'parent',
  }
  const events = await replay(
    nativeRows,
    [
      parentPrefixTool,
      userChunk('run suite'),
      thoughtChunk('boilerplate thought'),
      bgTool,
      thoughtChunk('boilerplate thought'),
      text('Waiting for background task.'),
      nextTool,
    ],
    'replay',
    undefined,
    [{ stepIndex: 13, callId: 'call-bg' }],
  )
  assert.deepEqual(labels(events), [
    'tool_call',
    'user_message',
    'compaction',
    'reasoning',
    'tool_call',
    'compaction',
    'reasoning',
    'Waiting for background task.',
    'tool_call',
    'tool_call',
    'compaction',
  ])
  assert.equal(
    timeline(events).filter(({ item }) => item.type === 'notification').length,
    0,
  )
})

test('alignReplayTools handles windowed ACP slices (M > N) and ambiguous window fallbacks', () => {
  const row = (step_index: number, path: string) => ({
    step_index,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ name: 'default_api:read_file', args: { Path: path } }],
  })
  const snapshots = new Map<string, { rawInput?: unknown }>([
    ['c1', { rawInput: { Path: '/b' } }],
    ['c2', { rawInput: { Path: '/c' } }],
  ])
  // Unique M > N window: Brain has [/a, /b, /c, /d], ACP replays [/b, /c]
  const uniqueWindow = alignReplayTools(
    [row(0, '/a'), row(2, '/b'), row(4, '/c'), row(6, '/d')],
    new Map(),
    [],
    ['c1', 'c2'],
    snapshots,
  )
  assert.deepEqual(uniqueWindow.alignedIds, [undefined, 'c1', 'c2', undefined])
  assert.deepEqual([...uniqueWindow.verifiedAcpToolIds], ['c1', 'c2'])

  // Ambiguous M > N window: Brain has [/b, /c, /b, /c], ACP replays [/b, /c]
  const ambiguousWindow = alignReplayTools(
    [row(0, '/b'), row(2, '/c'), row(4, '/b'), row(6, '/c')],
    new Map(),
    [],
    ['c1', 'c2'],
    snapshots,
  )
  assert.deepEqual(ambiguousWindow.alignedIds, ['c1', 'c2'])
  assert.equal(ambiguousWindow.verifiedAcpToolIds.size, 0)

  // Ambiguous M < N forked suffix: ACP has 2 identical [/b] windows, Brain suffix at step 10 falls back to N - M
  const suffixSnapshots = new Map<string, { rawInput?: unknown }>([
    ['p1', { rawInput: { Path: '/b' } }],
    ['p2', { rawInput: { Path: '/b' } }],
  ])
  const ambiguousSuffix = alignReplayTools(
    [row(10, '/b')],
    new Map(),
    [],
    ['p1', 'p2'],
    suffixSnapshots,
  )
  assert.deepEqual(ambiguousSuffix.alignedIds, ['p2'])
  assert.deepEqual([...ambiguousSuffix.verifiedAcpToolIds], ['p2'])
})

test('mixed foreground and background calls anchor at their GENERIC steps', async () => {
  for (const backgroundFirst of [false, true]) {
    for (const backgroundCount of [1, 2]) {
      const foreground = { name: 'read_file', args: { Path: '/example' } }
      const backgrounds = Array.from({ length: backgroundCount }, (_, i) => ({
        name: 'run_command',
        args: { CommandLine: `task-${i}`, WaitMsBeforeAsync: 500 },
      }))
      const calls = backgroundFirst
        ? [...backgrounds, foreground]
        : [foreground, ...backgrounds]
      const updates = calls.map((tool, i) => ({
        sessionUpdate: 'tool_call',
        toolCallId: `call-${i}`,
        title: tool.name,
        status: tool.name === 'read_file' ? 'completed' : 'failed',
        rawInput: tool.args,
        rawOutput: tool.name === 'read_file' ? 'ok' : 'Tool failed',
      }))
      // Include checkpoints before the batch, between response and GENERIC,
      // and between sibling GENERIC rows to expose premature anchors.
      const rows = [
        response(0, 'BEFORE'),
        checkpoint(1),
        {
          step_index: 2,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: calls,
        },
        checkpoint(3),
        ...calls.flatMap((tool, i) => [
          {
            step_index: 4 + i * 2,
            type: 'GENERIC',
            status:
              tool.name === 'read_file'
                ? 'DONE'
                : i % 2
                  ? 'WAITING'
                  : 'RUNNING',
            content:
              tool.name === 'read_file'
                ? 'ok'
                : `Tool is running as a background task with task id: task-${i}`,
          },
          checkpoint(5 + i * 2),
        ]),
        response(4 + calls.length * 2, 'AFTER'),
      ]
      const events = await replay(rows, [
        text('BEFORE'),
        ...updates,
        text('AFTER'),
      ])
      assert.deepEqual(labels(events), [
        'BEFORE',
        'compaction',
        'compaction',
        ...calls.flatMap(() => ['tool_call', 'compaction']),
        'AFTER',
      ])
      const tools = timeline(events).flatMap(({ item }) =>
        item.type === 'tool_call' ? [item] : [],
      )
      assert.deepEqual(
        tools.map((item) => item.callId),
        updates.map((tool) => tool.toolCallId),
      )
      assert.deepEqual(
        tools.map((item) => item.status),
        updates.map((tool) => tool.status),
      )
    }
  }
})

test('no checkpoint preserves ACP terminal result', async () => {
  const events = await replay(
    rows.filter((r) => r.type !== 'CHECKPOINT'),
    [
      text('BEFORE'),
      { ...read, status: 'failed', rawOutput: 'ACP failure' },
      text('AFTER'),
    ],
  )
  const tools = timeline(events).flatMap((e) =>
    e.item.type === 'tool_call' ? [e.item] : [],
  )
  assert.equal(tools.at(-1)?.status, 'failed')
})

test('checkpoint shift preserves per-tool metadata identity', async () => {
  const nativeRows = [
    {
      step_index: 0,
      type: 'PLANNER_RESPONSE',
      tool_calls: [
        { name: 'read_file', args: { Path: '/a' } },
        { name: 'read_file', args: { Path: '/b' } },
      ],
    },
    checkpoint(1),
    { step_index: 2, type: 'GENERIC', status: 'DONE', content: 'A' },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'B' },
  ]
  const updates = ['a', 'b'].map((id) => ({
    ...read,
    toolCallId: id,
    rawInput: { Path: `/${id}` },
    rawOutput: undefined,
  }))
  const events = await replay(nativeRows, updates, 'replay', undefined, [
    { stepIndex: 2, callId: 'a' },
    { stepIndex: 3, callId: 'b' },
  ])
  const tools = timeline(events).flatMap((e) =>
    e.item.type === 'tool_call' ? [e.item] : [],
  )
  const lastA = tools.filter((t) => t.callId === 'a').at(-1)
  assert.ok(lastA?.detail.type === 'read' && lastA.detail.content === 'A')
  const lastB = tools.filter((t) => t.callId === 'b').at(-1)
  assert.ok(lastB?.detail.type === 'read' && lastB.detail.content === 'B')

  // Live deferred-anonymous path across split poll batches with an intervening CHECKPOINT
  const projector = new BrainTranscriptProjector('live')
  projector.resolveToolIdentities(true)
  projector.observeRows(nativeRows.slice(0, 1))
  assert.deepEqual(projector.project(nativeRows[0]), [])
  projector.syncMetadata(
    {
      messages: [],
      readIds: new Set(),
      unconsumedHighPriority: [],
      consumedNonUserCount: 0,
      stepCallIds: new Map([
        [2, 'a'],
        [3, 'b'],
      ]),
    },
    [],
  )
  projector.observeRows(nativeRows.slice(1))
  const resolved = projector.resolveToolIdentities(true)
  assert.deepEqual(
    resolved.map((i) => (i.type === 'tool_call' ? i.callId : i.type)),
    ['a', 'b'],
  )
})

test('unverified positional tool cannot recover a different ACP call result', async () => {
  const nativeRows = [
    {
      step_index: 0,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ name: 'read_file', args: { Path: '/brain-a' } }],
    },
    { step_index: 1, type: 'GENERIC', status: 'DONE', content: 'A content' },
    checkpoint(2),
    {
      step_index: 3,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ id: 'b', name: 'read_file', args: { Path: '/b' } }],
    },
  ]
  const events = await replay(nativeRows, [
    {
      ...read,
      toolCallId: 'b',
      rawInput: { Path: '/acp' },
      rawOutput: undefined,
    },
  ])
  const tools = timeline(events).flatMap((e) =>
    e.item.type === 'tool_call' ? [e.item] : [],
  )
  assert.ok(
    !tools.some(
      (t) =>
        t.callId === 'b' &&
        t.detail.type === 'read' &&
        t.detail.content === 'A content',
    ),
  )
})

test('background completion recovers missing results without becoming a second history anchor', async () => {
  for (const withCheckpoint of [false, true]) {
    const nativeRows = [
      {
        step_index: 0,
        type: 'PLANNER_RESPONSE',
        tool_calls: [
          { id: 'bg', name: 'run_command', args: { CommandLine: 'npm test' } },
        ],
      },
      {
        step_index: 1,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: task-bg',
      },
      ...(withCheckpoint ? [checkpoint(2)] : []),
      response(3, 'WAITING'),
      {
        step_index: 4,
        type: 'SYSTEM_MESSAGE',
        status: 'DONE',
        content:
          '[Message] sender=task-bg content=Task id "task-bg" finished with result:\npass\n</SYSTEM_MESSAGE>',
      },
    ]
    const events = await replay(nativeRows, [
      {
        sessionUpdate: 'tool_call',
        toolCallId: 'bg',
        title: 'run_command',
        status: 'completed',
        rawInput: { CommandLine: 'npm test --acp' },
        rawOutput: null,
      },
      text('WAITING'),
    ])
    assert.deepEqual(labels(events), [
      'tool_call',
      ...(withCheckpoint ? ['compaction'] : []),
      'tool_call',
      'WAITING',
    ])
    const recovered = timeline(events).filter(
      ({ item }) => item.type === 'tool_call',
    )[1].item
    assert.ok(
      recovered.type === 'tool_call' && recovered.detail.type === 'shell',
    )
    assert.equal(recovered.detail.command, 'npm test --acp')
    assert.equal(recovered.detail.output, 'pass')
  }
})

test('Orchestrator contrib planexecute handoffs align across CHECKPOINT history replay and preserve plan cards', async () => {
  const planMd = '# Plan\n- Inspect\n- Patch\n'
  const nativeRows = [
    {
      step_index: 0,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'Planning',
      tool_calls: [
        {
          id: 'exit-1',
          name: 'contrib:exit_plan_mode',
          args: { plan: planMd },
        },
      ],
    },
    {
      step_index: 1,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Plan mode is off.',
    },
    {
      step_index: 2,
      type: 'SYSTEM_MESSAGE',
      status: 'DONE',
      content:
        '<SYSTEM_MESSAGE>\nPlan mode is off: this turn runs on the execution model.\n</SYSTEM_MESSAGE>',
    },
    checkpoint(3),
    {
      step_index: 4,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'Executing',
      tool_calls: [
        {
          id: 'enter-1',
          name: 'contrib:enter_plan_mode',
          args: { reason: 'Ready for review' },
        },
      ],
    },
    {
      step_index: 5,
      type: 'GENERIC',
      status: 'DONE',
      content: 'Plan mode is on.',
    },
    {
      step_index: 6,
      type: 'SYSTEM_MESSAGE',
      status: 'DONE',
      content:
        '<SYSTEM_MESSAGE>\n[plan review] Plan mode is on: this turn runs on the planning model.\n</SYSTEM_MESSAGE>',
    },
    response(7, 'Reviewed'),
  ]
  const events = await replay(nativeRows, [
    text('Planning'),
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'exit-1',
      title: 'Run contrib:exit_plan_mode',
      kind: 'other',
      status: 'completed',
      rawInput: { plan: planMd },
      rawOutput: null,
    },
    text('Executing'),
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'enter-1',
      title: 'Run contrib:enter_plan_mode',
      kind: 'other',
      status: 'completed',
      rawInput: { reason: 'Ready for review' },
      rawOutput: 'Plan mode is on.',
    },
    text('Reviewed'),
  ])
  assert.deepEqual(labels(events), [
    'Planning',
    'tool_call',
    'tool_call',
    'compaction',
    'Executing',
    'tool_call',
    'Reviewed',
  ])
  const tools = timeline(events).filter(({ item }) => item.type === 'tool_call')
  // exit-1 has rawOutput: null in ACP, so Brain result recovery emits a second update before compaction;
  // both snapshots retain { type: 'plan', text: planMd }.
  const exitItems = tools.filter(
    ({ item }) => item.type === 'tool_call' && item.callId === 'exit-1',
  )
  assert.equal(exitItems.length, 2)
  for (const { item } of exitItems) {
    assert.ok(item.type === 'tool_call')
    assert.deepEqual(item.detail, { type: 'plan', text: planMd })
  }
  const enterItem = tools.findLast(
    ({ item }) => item.type === 'tool_call' && item.callId === 'enter-1',
  )?.item
  assert.ok(enterItem?.type === 'tool_call')
  assert.equal(enterItem.detail.type, 'unknown')
})
