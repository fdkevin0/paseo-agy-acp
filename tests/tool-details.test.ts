import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { test } from 'node:test'
import type { SessionNotification } from '@agentclientprotocol/sdk'
import { BrainTranscriptProjector } from '../server/brain.ts'
import { stripToolSummary, Timeline, toolDetail } from '../server/mapping.ts'

type AcpUpdate = SessionNotification['update']
type ToolCallDetail = ReturnType<typeof toolDetail>

// Protocol fixture, not a captured ACP payload. Brain's name/args and GENERIC
// content shapes were observed locally; the values and call identity are synthetic.
const fixture = JSON.parse(
  await readFile(
    new URL('./fixtures/mcp-tool-presentation.json', import.meta.url),
    'utf8',
  ),
)

const unknownDetail = <I, O>(input: I, output: O) => ({
  type: 'unknown' as const,
  input,
  output,
})

function expectUnknownIdentity(name: string) {
  assert.deepEqual(
    toolDetail({
      title: name,
      rawInput: fixture.input,
      rawOutput: fixture.output,
    }),
    unknownDetail(fixture.input, fixture.output),
  )
  // A raw identity is authoritative over a generic title/kind hint.
  assert.deepEqual(
    toolDetail({
      name,
      title: 'File example',
      kind: 'edit',
      rawInput: fixture.input,
      rawOutput: fixture.output,
    }),
    unknownDetail(fixture.input, fixture.output),
  )
}

function expectUnknownPassThrough(value: unknown) {
  assert.deepEqual(
    toolDetail({ title: fixture.name, rawInput: value, rawOutput: value }),
    unknownDetail(value, value),
  )
}

function expectRawOutputWins(
  content: NonNullable<Parameters<typeof toolDetail>[0]['content']>,
  rawOutput: unknown,
) {
  assert.deepEqual(
    toolDetail({
      title: fixture.name,
      rawInput: fixture.input,
      content,
      rawOutput,
    }),
    unknownDetail(fixture.input, rawOutput),
  )
}

function expectLiveAndHistoryMatch(input: unknown, output: unknown) {
  const timeline = new Timeline('session-example')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    status: 'in_progress',
    rawInput: input,
  })
  const live = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'completed',
    rawOutput: output,
  })
  const projector = new BrainTranscriptProjector('session-example')
  projector.bindToolCallIds(['call-example'])
  const [start] = projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    status: 'DONE',
    tool_calls: [{ name: fixture.name, args: input }],
  })
  const [history] = projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'DONE',
    content: output,
  })
  const expectedDetail = unknownDetail(input, output)
  assert.ok(start?.type === 'tool_call')
  assert.deepEqual(start.detail, unknownDetail(input, null))
  assert.ok(live?.type === 'tool_call' && history?.type === 'tool_call')
  assert.deepEqual(live.detail, expectedDetail)
  assert.deepEqual(history.detail, expectedDetail)
  assert.deepEqual(history, live)
  assert.equal(history.id, start.id)
}

function expectTerminalParity(
  status: 'failed' | 'cancelled',
  brainStatus: 'ERROR' | 'CANCELED',
  expectedStatus: 'failed' | 'canceled',
  expectedLiveError: string | null,
  expectedHistoryError: { message: string } | null,
) {
  const timeline = new Timeline('session-example')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    rawInput: fixture.input,
  })
  const live = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: status as 'failed',
    rawOutput: 'tool failed',
  })
  const projector = new BrainTranscriptProjector('session-example')
  projector.bindToolCallIds(['call-example'])
  projector.project({
    step_index: 1,
    type: 'PLANNER_RESPONSE',
    tool_calls: [{ name: fixture.name, args: fixture.input }],
  })
  const [history] = projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: brainStatus,
    content: 'tool failed',
  })
  assert.ok(live?.type === 'tool_call' && history?.type === 'tool_call')
  assert.equal(live.status, expectedStatus)
  assert.equal(history.status, expectedStatus)
  assert.deepEqual(live.detail, unknownDetail(fixture.input, 'tool failed'))
  assert.deepEqual(history.detail, unknownDetail(fixture.input, 'tool failed'))
  assert.equal(live.error, expectedLiveError)
  assert.deepEqual(history.error, expectedHistoryError)
}

function expectCompletionContentMerge(
  oldStatus: 'in_progress' | 'completed',
  oldOutput: unknown,
  incoming: { rawOutput?: unknown },
  content: NonNullable<Parameters<typeof toolDetail>[0]['content']>,
  expected: unknown,
) {
  const timeline = new Timeline('session-example')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    status: oldStatus,
    rawInput: fixture.input,
    rawOutput: oldOutput,
  })
  const completed = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'completed',
    content,
    ...incoming,
  })
  assert.ok(completed?.type === 'tool_call')
  assert.deepEqual(completed.detail, unknownDetail(fixture.input, expected))
}

function expectNonNativeKeyRetained(title: string) {
  const rawInput = { path: '/example', offset: 7 }
  const rawOutput = 'retained result'
  assert.deepEqual(toolDetail({ title, kind: 'read', rawInput, rawOutput }), {
    type: 'read',
    filePath: '/example',
    content: rawOutput,
    offset: 7,
    limit: undefined,
  })
  assert.deepEqual(
    toolDetail({ name: title, title, kind: 'read', rawInput, rawOutput }),
    unknownDetail(rawInput, rawOutput),
  )
  const timeline = new Timeline('prototype-key')
  const item = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'stable-call',
    name: title,
    title,
    rawInput,
    rawOutput,
  })
  assert.ok(item?.type === 'tool_call')
  assert.equal(item.name, title)
  assert.equal(item.callId, 'stable-call')
  assert.deepEqual(item.detail, unknownDetail(rawInput, rawOutput))
}

function expectNullAndOmissionMerge(
  startUpdate: AcpUpdate,
  contentUpdate: AcpUpdate,
  terminalUpdate: AcpUpdate,
  expectedStatus: 'completed' | 'failed',
  expectedDetail: ToolCallDetail,
) {
  const timeline = new Timeline('raw-v1')
  const started = timeline.update(startUpdate)
  assert.ok(started?.type === 'tool_call')
  assert.equal(started.callId, 'stable')
  const populated = timeline.update(contentUpdate)
  assert.ok(populated?.type === 'tool_call')
  assert.equal(populated.callId, 'stable')
  const settled = timeline.update(terminalUpdate)
  assert.ok(settled?.type === 'tool_call')
  assert.equal(settled.callId, 'stable')
  const trailingNull: AcpUpdate = {
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawInput: null,
  }
  const retained = timeline.update(trailingNull)
  assert.ok(retained?.type === 'tool_call')
  assert.equal(retained.status, expectedStatus)
  assert.deepEqual(retained.detail, expectedDetail)
}

function expectRawInputRetained(timeline: Timeline, value: unknown) {
  const replaced = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawInput: value,
  })
  assert.ok(
    replaced?.type === 'tool_call' && replaced.detail.type === 'unknown',
  )
  assert.deepEqual(replaced.detail.input, value)
  const afterOmit = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
  })
  assert.ok(afterOmit?.type === 'tool_call')
  assert.deepEqual(afterOmit.detail, replaced.detail)
  const afterNull = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawInput: null,
  })
  assert.ok(afterNull?.type === 'tool_call')
  assert.deepEqual(afterNull.detail, replaced.detail)
}

function expectRawOutputRetained(timeline: Timeline, value: unknown) {
  const replaced = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawOutput: value,
  })
  assert.ok(
    replaced?.type === 'tool_call' && replaced.detail.type === 'unknown',
  )
  assert.deepEqual(replaced.detail.output, value)
  const afterOmit = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
  })
  assert.ok(afterOmit?.type === 'tool_call')
  assert.deepEqual(afterOmit.detail, replaced.detail)
  const afterNull = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawOutput: null,
  })
  assert.ok(afterNull?.type === 'tool_call')
  assert.deepEqual(afterNull.detail, replaced.detail)
}

function expectContentReplacement(
  timeline: Timeline,
  content: NonNullable<
    Extract<AcpUpdate, { sessionUpdate: 'tool_call_update' }>['content']
  >,
) {
  const update: AcpUpdate = {
    sessionUpdate: 'tool_call_update',
    toolCallId: 'stable',
    rawInput: null,
    rawOutput: null,
    content,
  }
  const item = timeline.update(update)
  assert.ok(item?.type === 'tool_call')
  assert.deepEqual(item.detail, unknownDetail(fixture.input, content))
}

test('unknown tools do not acquire semantics from parameter names or name fragments', () => {
  expectUnknownIdentity(fixture.name)
  expectUnknownIdentity('mcp_paseo_get_agent_activity')
  expectUnknownIdentity('mcp__server__read_file')
  expectUnknownIdentity('Vendor.invoke_subagent')
  expectUnknownIdentity('Vendor.send_message')
  expectUnknownIdentity('custom_replace')
  expectUnknownIdentity('CustomTool')
  expectUnknownIdentity('Run Vendor.send_message')
  expectUnknownIdentity('Run send_message with extra text')
})

test('unknown details retain JSON values and do not parse JSON-looking text', () => {
  expectUnknownPassThrough(null)
  expectUnknownPassThrough('')
  expectUnknownPassThrough(false)
  expectUnknownPassThrough(0)
  expectUnknownPassThrough([])
  expectUnknownPassThrough({})
  expectUnknownPassThrough(['a', 0, false, null])
  expectUnknownPassThrough('plain text')
  expectUnknownPassThrough('{"count":1}')
  expectUnknownPassThrough(fixture.output)
  expectUnknownPassThrough({
    isError: true,
    content: [{ type: 'text', text: 'tool failure' }],
  })
  assert.deepEqual(
    toolDetail({ title: fixture.name }),
    unknownDetail(null, null),
  )
})

test('ACP content blocks remain structured when rawOutput is absent', () => {
  const content = [
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'one' },
    },
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'two' },
    },
    {
      type: 'content' as const,
      content: { type: 'image' as const, mimeType: 'image/png', data: '' },
    },
  ]
  assert.deepEqual(
    toolDetail({ title: fixture.name, rawInput: fixture.input, content }),
    unknownDetail(fixture.input, content),
  )
  // rawOutput is authoritative, including explicit empty/null values.
  expectRawOutputWins(content, null)
  expectRawOutputWins(content, '')
  expectRawOutputWins(content, false)
  expectRawOutputWins(content, 0)
  expectRawOutputWins(content, [])
  expectRawOutputWins(content, fixture.output)
})

test('one ACP call merges start/update/completion without losing input or identity', () => {
  const timeline = new Timeline('session-example')
  const start = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    name: fixture.name,
    title: 'Lookup example',
    kind: 'other',
    status: 'in_progress',
    rawInput: fixture.input,
  })
  const update = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    title: 'Lookup finished',
    name: null,
    rawOutput: fixture.output,
  })
  const completed = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'completed',
  })
  assert.ok(start?.type === 'tool_call')
  assert.deepEqual(start.detail, unknownDetail(fixture.input, null))
  assert.ok(update?.type === 'tool_call')
  assert.equal(update.id, start.id)
  assert.equal(update.callId, start.callId)
  assert.equal(update.name, fixture.name)
  assert.deepEqual(update.detail, unknownDetail(fixture.input, fixture.output))
  assert.ok(completed?.type === 'tool_call')
  assert.equal(completed.id, start.id)
  assert.equal(completed.callId, start.callId)
  assert.equal(completed.name, fixture.name)
  assert.deepEqual(
    completed.detail,
    unknownDetail(fixture.input, fixture.output),
  )
  assert.equal(completed.status, 'completed')
  assert.equal(completed.error, null)
  assert.equal(timeline.isRunning(start.id), false)
  const late = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'in_progress',
  })
  assert.ok(late?.type === 'tool_call')
  assert.equal(late.status, 'completed')
  assert.deepEqual(late.detail, completed.detail)
})

test('ACP raw null and omitted fields retain previously reported values', () => {
  const timeline = new Timeline('session-example')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    rawInput: fixture.input,
    rawOutput: fixture.output,
  })
  const retained = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    rawInput: null,
    rawOutput: null,
    status: 'completed',
  })
  assert.ok(retained?.type === 'tool_call')
  assert.deepEqual(
    retained.detail,
    unknownDetail(fixture.input, fixture.output),
  )
})

test('live ACP and Brain history agree on final structured details and bound call identity', () => {
  expectLiveAndHistoryMatch(fixture.input, fixture.output)
  expectLiveAndHistoryMatch(fixture.input, 'text')
  expectLiveAndHistoryMatch(['array'], '{"json":"text"}')
  expectLiveAndHistoryMatch(null, null)
  expectLiveAndHistoryMatch(false, false)
  expectLiveAndHistoryMatch(0, 0)
  expectLiveAndHistoryMatch('scalar', '')
  expectLiveAndHistoryMatch(fixture.input, [])
})

test('failure and cancellation preserve input/output and agree across live/history', () => {
  expectTerminalParity('failed', 'ERROR', 'failed', 'tool failed', {
    message: 'tool failed',
  })
  expectTerminalParity('cancelled', 'CANCELED', 'canceled', null, null)
})

test('native identities and ACP semantic hints retain dedicated cards', () => {
  assert.equal(
    toolDetail({
      title: 'default_api:Run_Command',
      rawInput: { CommandLine: 'echo example' },
    }).type,
    'shell',
  )
  assert.equal(
    toolDetail({ title: 'read_file', rawInput: { Path: '/example' } }).type,
    'read',
  )
  assert.equal(
    toolDetail({
      title: 'write_file',
      rawInput: { Path: '/example', Content: 'data' },
    }).type,
    'write',
  )
  assert.equal(
    toolDetail({
      title: 'edit_file',
      rawInput: { Path: '/example', Target: 'before', Replacement: 'after' },
    }).type,
    'edit',
  )
  assert.equal(
    toolDetail({ title: 'grep_search', rawInput: { Query: 'example' } }).type,
    'search',
  )
  assert.equal(
    toolDetail({
      title: 'read_url_content',
      rawInput: { Url: 'https://example.com' },
    }).type,
    'fetch',
  )
  assert.equal(
    toolDetail({ title: 'invoke_subagent', rawInput: { Prompt: 'example' } })
      .type,
    'sub_agent',
  )
  assert.equal(
    toolDetail({
      title: 'Run invoke_subagent',
      rawInput: { Prompt: 'example' },
    }).type,
    'sub_agent',
  )
  assert.equal(
    toolDetail({ title: 'Run send_message', rawInput: { Message: 'example' } })
      .type,
    'sub_agent',
  )
  assert.equal(
    toolDetail({
      title: 'ask_question',
      rawInput: { questions: [{ question: 'Example?', options: ['A', 'B'] }] },
    }).type,
    'plain_text',
  )
  assert.equal(
    toolDetail({
      title: 'Command example',
      kind: 'execute',
      rawInput: { command: 'echo example' },
    }).type,
    'shell',
  )
})

test('completion content replaces an earlier running null placeholder without losing input', () => {
  const timeline = new Timeline('session-example')
  const start = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    status: 'in_progress',
    rawInput: fixture.input,
    rawOutput: null,
  })
  const content = [
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'ACTUAL RESULT' },
    },
  ]
  const completed = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'completed',
    content,
  })
  assert.ok(start?.type === 'tool_call' && completed?.type === 'tool_call')
  assert.equal(completed.id, start.id)
  assert.equal(completed.status, 'completed')
  assert.deepEqual(completed.detail, unknownDetail(fixture.input, content))
  // Later status-only snapshots must retain the actual content too.
  const late = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    status: 'completed',
  })
  assert.ok(late?.type === 'tool_call')
  assert.deepEqual(late.detail, completed.detail)
})

test('completion content fills absent raw output but does not replace existing raw data', () => {
  const content = [
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'presentation only' },
    },
  ]
  expectCompletionContentMerge(
    'in_progress',
    null,
    { rawOutput: null },
    content,
    content,
  )
  expectCompletionContentMerge(
    'in_progress',
    fixture.output,
    {},
    content,
    fixture.output,
  )
  expectCompletionContentMerge('completed', null, {}, content, content)
  expectCompletionContentMerge(
    'in_progress',
    null,
    { rawOutput: fixture.output },
    content,
    fixture.output,
  )
})

test('native kind lookup ignores prototype keys and retains explicit identities', () => {
  expectNonNativeKeyRetained('constructor')
  expectNonNativeKeyRetained('__proto__')
  expectNonNativeKeyRetained('toString')
  expectNonNativeKeyRetained('custom_read')
  assert.equal(
    toolDetail({
      name: 'read_file',
      kind: 'other',
      rawInput: { path: '/native' },
    }).type,
    'read',
  )
})

test('ACP v1 raw channels merge null and omission independently across live updates', () => {
  const content = [
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'result' },
    },
  ]
  const shellExpected = {
    type: 'shell' as const,
    command: 'echo kept',
    cwd: undefined,
    output: 'result',
  }
  const readExpected = {
    type: 'read' as const,
    filePath: '/kept',
    content: 'result',
    offset: undefined,
    limit: undefined,
  }
  const unknownExpected = unknownDetail(
    { path: '/kept', nested: null },
    content,
  )

  expectNullAndOmissionMerge(
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'stable',
      title: 'run_command',
      status: 'in_progress',
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: { command: 'echo kept' },
      content,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: null,
      rawOutput: null,
      status: 'completed',
    },
    'completed',
    shellExpected,
  )
  expectNullAndOmissionMerge(
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'stable',
      title: 'run_command',
      status: 'in_progress',
      rawInput: null,
      rawOutput: null,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: { command: 'echo kept' },
      content,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: null,
      rawOutput: null,
      status: 'failed',
    },
    'failed',
    shellExpected,
  )
  expectNullAndOmissionMerge(
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'stable',
      title: 'read_file',
      status: 'in_progress',
      rawInput: null,
      rawOutput: null,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: { path: '/kept', nested: null },
      content,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: null,
      rawOutput: null,
      status: 'completed',
    },
    'completed',
    readExpected,
  )
  expectNullAndOmissionMerge(
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'stable',
      title: fixture.name,
      status: 'in_progress',
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: { path: '/kept', nested: null },
      content,
    },
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'stable',
      rawInput: null,
      rawOutput: null,
      status: 'failed',
    },
    'failed',
    unknownExpected,
  )

  const inputTimeline = new Timeline('raw-input-values')
  inputTimeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'stable',
    title: fixture.name,
  })
  expectRawInputRetained(inputTimeline, false)
  expectRawInputRetained(inputTimeline, 0)
  expectRawInputRetained(inputTimeline, '')
  expectRawInputRetained(inputTimeline, [])
  expectRawInputRetained(inputTimeline, {})
  expectRawInputRetained(inputTimeline, { nested: null })
  expectRawInputRetained(inputTimeline, [null])
  expectRawInputRetained(inputTimeline, 'null')

  const outputTimeline = new Timeline('raw-output-values')
  outputTimeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'stable',
    title: fixture.name,
  })
  expectRawOutputRetained(outputTimeline, false)
  expectRawOutputRetained(outputTimeline, 0)
  expectRawOutputRetained(outputTimeline, '')
  expectRawOutputRetained(outputTimeline, [])
  expectRawOutputRetained(outputTimeline, {})
  expectRawOutputRetained(outputTimeline, { nested: null })
  expectRawOutputRetained(outputTimeline, [null])
  expectRawOutputRetained(outputTimeline, 'null')
})

test('ACP top-level null permits content replacement without altering direct Brain JSON null', () => {
  const timeline = new Timeline('content-v1')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'stable',
    title: fixture.name,
    rawInput: fixture.input,
    rawOutput: null,
  })
  expectContentReplacement(timeline, [])
  expectContentReplacement(timeline, [
    {
      type: 'content',
      content: { type: 'text', text: 'actual' },
    },
  ])
  expectContentReplacement(timeline, [])
  assert.deepEqual(
    toolDetail({
      title: fixture.name,
      rawInput: { nested: null },
      rawOutput: null,
      content: [
        { type: 'content', content: { type: 'text', text: 'presentation' } },
      ],
    }),
    unknownDetail({ nested: null }, null),
  )
})

test('unknown tool details sanitize non-JSON and cyclic values without throwing', () => {
  const sharedObj = { nil: null, flag: false, zero: 0, empty: '' }
  const sharedArr = [null, false, 0, '']
  const cyclicObj: Record<string, unknown> = { ok: 'yes', nested: { n: 1 } }
  cyclicObj.self = cyclicObj
  ;(cyclicObj.nested as Record<string, unknown>).root = cyclicObj
  const cyclicArr: unknown[] = ['head']
  cyclicArr.push(cyclicArr)

  const rawInput = {
    nan: Number.NaN,
    posInf: Number.POSITIVE_INFINITY,
    negInf: Number.NEGATIVE_INFINITY,
    omitted: undefined,
    big: 42n,
    fn: () => 'ignored',
    sym: Symbol('ignored'),
    nil: null,
    flag: false,
    zero: 0,
    empty: '',
    sharedA: sharedObj,
    sharedB: sharedObj,
    sharedList: [sharedArr, sharedArr],
    arr: [
      Number.NaN,
      Number.POSITIVE_INFINITY,
      undefined,
      7n,
      () => {},
      Symbol('el'),
      null,
      false,
      0,
      '',
    ],
    cyclicObj,
    cyclicArr,
  }
  const rawOutput: Record<string, unknown> = {
    big: 9n,
    nan: Number.NaN,
    omitted: undefined,
    shared: [sharedObj, sharedObj],
  }
  rawOutput.self = rawOutput

  const detail = toolDetail({
    title: fixture.name,
    rawInput,
    rawOutput,
  })
  const expectedInput = {
    nan: null,
    posInf: null,
    negInf: null,
    big: null,
    fn: null,
    sym: null,
    nil: null,
    flag: false,
    zero: 0,
    empty: '',
    sharedA: { nil: null, flag: false, zero: 0, empty: '' },
    sharedB: { nil: null, flag: false, zero: 0, empty: '' },
    sharedList: [
      [null, false, 0, ''],
      [null, false, 0, ''],
    ],
    arr: [null, null, null, null, null, null, null, false, 0, ''],
    cyclicObj: { ok: 'yes', nested: { n: 1, root: null }, self: null },
    cyclicArr: ['head', null],
  }
  const expectedOutput = {
    big: null,
    nan: null,
    shared: [
      { nil: null, flag: false, zero: 0, empty: '' },
      { nil: null, flag: false, zero: 0, empty: '' },
    ],
    self: null,
  }
  assert.deepEqual(detail, unknownDetail(expectedInput, expectedOutput))
  assert.ok(
    detail.type === 'unknown' &&
      detail.input &&
      typeof detail.input === 'object',
  )
  assert.equal(Object.hasOwn(detail.input, 'omitted'), false)
  assert.deepEqual(JSON.parse(JSON.stringify(detail)), detail)

  assert.deepEqual(
    toolDetail({
      title: fixture.name,
      rawInput: Number.NaN,
      rawOutput: 99n,
    }),
    unknownDetail(null, null),
  )
  assert.deepEqual(
    toolDetail({
      title: fixture.name,
      rawInput: () => {},
      rawOutput: Symbol('out'),
    }),
    unknownDetail(null, null),
  )
})

test('legacy ACP Running/Run/? titles map to dedicated cards without overriding explicit names or unknown tools', () => {
  assert.deepEqual(
    toolDetail({
      title: 'Running run_command',
      rawInput: { CommandLine: 'pwd' },
    }),
    {
      type: 'shell',
      command: 'pwd',
      cwd: undefined,
      output: undefined,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'Run read_file?',
      rawInput: { Path: '/repo/a.ts' },
    }),
    {
      type: 'read',
      filePath: '/repo/a.ts',
      content: undefined,
      offset: undefined,
      limit: undefined,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'run default_api:edit_file?',
      rawInput: { Path: '/repo/a.ts', Target: 'old', Replacement: 'new' },
    }),
    {
      type: 'edit',
      filePath: '/repo/a.ts',
      oldString: 'old',
      newString: 'new',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'Running default_api:write_file?',
      rawInput: { Path: '/repo/a.ts', Content: 'hello' },
    }),
    {
      type: 'write',
      filePath: '/repo/a.ts',
      content: 'hello',
    },
  )
  assert.equal(
    toolDetail({
      title: 'run default_api:read_file',
      rawInput: { Path: '/repo/a.ts' },
    }).type,
    'read',
  )

  // Explicit raw name takes precedence over legacy ACP title normalization.
  assert.deepEqual(
    toolDetail({
      name: 'custom_tool',
      title: 'Running run_command',
      kind: 'execute',
      rawInput: { CommandLine: 'pwd' },
    }),
    unknownDetail({ CommandLine: 'pwd' }, null),
  )
  assert.deepEqual(
    toolDetail({
      name: 'Running run_command',
      title: 'Running run_command',
      rawInput: { CommandLine: 'pwd' },
    }),
    unknownDetail({ CommandLine: 'pwd' }, null),
  )
  assert.deepEqual(
    toolDetail({
      name: 'Run read_file?',
      title: 'Run read_file?',
      rawInput: { Path: '/repo/a.ts' },
    }),
    unknownDetail({ Path: '/repo/a.ts' }, null),
  )

  // Unknown names remain unknown even with Running/Run/? wrappers.
  assert.deepEqual(
    toolDetail({
      title: 'Running custom_tool?',
      rawInput: { CommandLine: 'pwd' },
    }),
    unknownDetail({ CommandLine: 'pwd' }, null),
  )
  assert.deepEqual(
    toolDetail({
      title: 'Run mcp__srv__read_file?',
      rawInput: { Path: '/repo/a.ts' },
    }),
    unknownDetail({ Path: '/repo/a.ts' }, null),
  )
  assert.deepEqual(
    toolDetail({
      title: 'Running read_file with extra?',
      rawInput: { Path: '/repo/a.ts' },
    }),
    unknownDetail({ Path: '/repo/a.ts' }, null),
  )
})

test('stripToolSummary omits top-level string toolSummary for display without mutating snapshots', () => {
  const withSummary = { toolSummary: 'Inspect config', target: 'a.json' }
  assert.deepEqual(stripToolSummary(withSummary), { target: 'a.json' })
  assert.equal(withSummary.toolSummary, 'Inspect config')

  assert.deepEqual(stripToolSummary({ toolSummary: '' }), {})

  const nonStringSummary = { toolSummary: 42, target: 'a.json' }
  assert.equal(stripToolSummary(nonStringSummary), nonStringSummary)

  const nestedSummary = { nested: { toolSummary: 'keep' } }
  assert.equal(stripToolSummary(nestedSummary), nestedSummary)

  const inherited = Object.create({ toolSummary: 'inherited' })
  assert.equal(stripToolSummary(inherited), inherited)

  const arr = ['toolSummary']
  assert.equal(stripToolSummary(arr), arr)
  assert.equal(stripToolSummary(null), null)
  assert.equal(stripToolSummary('toolSummary'), 'toolSummary')

  const rawInput = { toolSummary: 'Lookup item', id: 7 }
  assert.deepEqual(
    toolDetail({ title: fixture.name, rawInput, rawOutput: null }),
    unknownDetail({ id: 7 }, null),
  )
  assert.equal(rawInput.toolSummary, 'Lookup item')

  const timeline = new Timeline('summary-test')
  const item = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-1',
    title: fixture.name,
    status: 'in_progress',
    rawInput,
  })
  assert.ok(item?.type === 'tool_call')
  assert.deepEqual(item.detail, unknownDetail({ id: 7 }, null))
  assert.deepEqual(timeline.toolCallSnapshot('call-1')?.rawInput, {
    toolSummary: 'Lookup item',
    id: 7,
  })
})
