import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { test } from 'node:test'
import { BrainTranscriptProjector } from '../server/brain.ts'
import { Timeline, toolDetail } from '../server/mapping.ts'

// Protocol fixture, not a captured ACP payload. Brain's name/args and GENERIC
// content shapes were observed locally; the values and call identity are synthetic.
const fixture = JSON.parse(
  await readFile(
    new URL('./fixtures/mcp-tool-presentation.json', import.meta.url),
    'utf8',
  ),
)

const unknownDetail = (input: unknown, output: unknown) => ({
  type: 'unknown',
  input,
  output,
})

test('unknown tools do not acquire semantics from parameter names or name fragments', () => {
  for (const name of [
    fixture.name,
    'mcp_paseo_get_agent_activity',
    'mcp__server__read_file',
    'Vendor.invoke_subagent',
    'Vendor.send_message',
    'custom_replace',
    'CustomTool',
    'Run Vendor.send_message',
    'Run send_message with extra text',
  ]) {
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
})

test('unknown details retain JSON values and do not parse JSON-looking text', () => {
  for (const value of [
    null,
    '',
    false,
    0,
    [],
    {},
    ['a', 0, false, null],
    'plain text',
    '{"count":1}',
    fixture.output,
    { isError: true, content: [{ type: 'text', text: 'tool failure' }] },
  ]) {
    assert.deepEqual(
      toolDetail({ title: fixture.name, rawInput: value, rawOutput: value }),
      unknownDetail(value, value),
    )
  }
  assert.deepEqual(
    toolDetail({ title: fixture.name }),
    unknownDetail(null, null),
  )
  assert.throws(() =>
    toolDetail({ title: fixture.name, rawInput: { bad: NaN } }),
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
  for (const rawOutput of [null, '', false, 0, [], fixture.output])
    assert.deepEqual(
      toolDetail({
        title: fixture.name,
        rawInput: fixture.input,
        content,
        rawOutput,
      }),
      unknownDetail(fixture.input, rawOutput),
    )
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
  for (const item of [update, completed]) {
    assert.ok(item?.type === 'tool_call')
    assert.equal(item.id, start.id)
    assert.equal(item.callId, start.callId)
    assert.equal(item.name, fixture.name)
    assert.deepEqual(item.detail, unknownDetail(fixture.input, fixture.output))
  }
  assert.ok(completed?.type === 'tool_call')
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

test('explicit raw null clears a previous value while omitted raw fields retain it', () => {
  const timeline = new Timeline('session-example')
  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call-example',
    title: fixture.name,
    rawInput: fixture.input,
    rawOutput: fixture.output,
  })
  const cleared = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'call-example',
    rawInput: null,
    rawOutput: null,
    status: 'completed',
  })
  assert.ok(cleared?.type === 'tool_call')
  assert.deepEqual(cleared.detail, unknownDetail(null, null))
})

test('live ACP and Brain history agree on final structured details and bound call identity', () => {
  for (const output of [
    fixture.output,
    'text',
    '{"json":"text"}',
    '',
    null,
    false,
    0,
    [],
  ]) {
    for (const input of [fixture.input, null, ['array'], false, 0, 'scalar']) {
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
      assert.ok(start?.type === 'tool_call')
      assert.deepEqual(start.detail, unknownDetail(input, null))
      assert.deepEqual(history, live)
      assert.equal(history.id, start.id)
    }
  }
})

test('failure and cancellation preserve input/output and agree across live/history', () => {
  for (const [status, brainStatus] of [
    ['failed', 'ERROR'],
    ['cancelled', 'CANCELED'],
  ] as const) {
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
    assert.equal(history.status, live.status)
    assert.deepEqual(history.detail, live.detail)
    assert.deepEqual(live.detail, unknownDetail(fixture.input, 'tool failed'))
    if (status === 'failed') {
      assert.equal(live.error, 'tool failed')
      assert.deepEqual(history.error, { message: 'tool failed' })
    } else {
      assert.equal(live.error, null)
      assert.equal(history.error, null)
    }
  }
})

test('native identities and ACP semantic hints retain dedicated cards', () => {
  for (const [title, rawInput, type] of [
    ['default_api:Run_Command', { CommandLine: 'echo example' }, 'shell'],
    ['read_file', { Path: '/example' }, 'read'],
    ['write_file', { Path: '/example', Content: 'data' }, 'write'],
    [
      'edit_file',
      { Path: '/example', Target: 'before', Replacement: 'after' },
      'edit',
    ],
    ['grep_search', { Query: 'example' }, 'search'],
    ['read_url_content', { Url: 'https://example.com' }, 'fetch'],
    ['invoke_subagent', { Prompt: 'example' }, 'sub_agent'],
    ['Run invoke_subagent', { Prompt: 'example' }, 'sub_agent'],
    ['Run send_message', { Message: 'example' }, 'sub_agent'],
    [
      'ask_question',
      { questions: [{ question: 'Example?', options: ['A', 'B'] }] },
      'plain_text',
    ],
  ] as const) {
    assert.equal(toolDetail({ title, rawInput }).type, type)
  }
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

test('completion content does not override explicit null or existing structured raw output', () => {
  const content = [
    {
      type: 'content' as const,
      content: { type: 'text' as const, text: 'presentation only' },
    },
  ]
  for (const [oldStatus, oldOutput, incoming, expected] of [
    ['in_progress', null, { rawOutput: null }, null],
    ['in_progress', fixture.output, {}, fixture.output],
    ['completed', null, {}, null],
    ['in_progress', null, { rawOutput: fixture.output }, fixture.output],
  ] as const) {
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
})

test('native kind lookup ignores prototype keys and retains explicit identities', () => {
  for (const title of ['constructor', '__proto__', 'toString', 'custom_read']) {
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
  assert.equal(
    toolDetail({
      name: 'read_file',
      kind: 'other',
      rawInput: { path: '/native' },
    }).type,
    'read',
  )
})
