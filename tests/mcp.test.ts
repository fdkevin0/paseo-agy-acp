import assert from 'node:assert/strict'
import { test } from 'node:test'
import { resolveMcpToolIdentity, toolDisplayName } from '../server/mcp.ts'
import { isPreapprovedMcpTool } from '../server/permissions.ts'
import { Timeline, toolDetail } from '../server/mapping.ts'
import { BrainTranscriptProjector } from '../server/brain.ts'

const policy = {
  preapproved: [{ kind: 'mcp' as const, server: 'srv', tool: 'lookup' }],
}
const identity = { server: 'srv', tool: 'lookup' }

// Synthetic boundary cases; these are not captured AGY MCP transcripts.
test('one MCP resolver drives display and exact grants without rewriting input', () => {
  for (const title of [
    'mcp__srv__lookup',
    'default_api:mcp__srv__lookup',
    'Run mcp__srv__lookup',
    'Run default_api:mcp__srv__lookup',
    'srv:lookup',
    'call_mcp_tool',
    'Run default_api:call_mcp_tool',
  ]) {
    const rawInput = {
      ServerName: 'srv',
      ToolName: 'lookup',
      Arguments: { q: 'x' },
    }
    const call = { title, rawInput }
    assert.deepEqual(resolveMcpToolIdentity(call), identity)
    assert.equal(toolDisplayName(call), 'srv.lookup')
    assert.equal(isPreapprovedMcpTool(call, policy), true)
    assert.deepEqual(toolDetail(call), {
      type: 'unknown',
      input: rawInput,
      output: null,
    })
  }
})

test('explicit identity wins over descriptive titles and cannot widen grants', () => {
  assert.equal(
    isPreapprovedMcpTool(
      { name: 'mcp__srv__lookup', title: 'Look up a record' },
      policy,
    ),
    true,
  )
  for (const call of [
    { name: 'native_tool', title: 'Run mcp__srv__lookup' },
    { name: '', title: 'mcp__srv__lookup' },
    { name: 'mcp__srv__other', title: 'mcp__srv__lookup' },
    { title: 'Run mcp__other__lookup' },
    { title: 'Run mcp__srv__lookup extra' },
    { title: 'srv.lookup' },
    { title: 'mcp__srv__lookup__extra' },
    { title: 'contrib:switch_mode' },
    {
      title: 'untrusted:call_mcp_tool',
      rawInput: { ServerName: 'srv', ToolName: 'lookup' },
    },
    { title: 'call_mcp_tool', rawInput: { ServerName: 'srv', ToolName: 1 } },
    { title: 'call_mcp_tool', rawInput: null },
    {
      title: 'call_mcp_tool',
      rawInput: { ServerName: 'srv', ToolName: 'Lookup' },
    },
  ])
    assert.equal(
      isPreapprovedMcpTool(call, policy),
      false,
      JSON.stringify(call),
    )
  assert.equal(
    toolDisplayName({ title: 'contrib:switch_mode' }),
    'contrib:switch_mode',
  )
})

test('ACP updates and Brain replay use the same MCP label and retain input/output/status', () => {
  for (const name of [
    'default_api:mcp__srv__lookup',
    'default_api:mcp_srv_lookup',
    'default_api:call_mcp_tool',
  ]) {
    const input = {
      ServerName: 'srv',
      ToolName: 'lookup',
      Arguments: { q: 'x' },
    }
    const output = {
      isError: true,
      content: [{ type: 'text', text: 'reported error' }],
    }
    const timeline = new Timeline('session', ['srv'])
    const start = timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'call',
      name,
      title: 'Lookup record',
      rawInput: input,
      status: 'in_progress',
    })
    assert.ok(start?.type === 'tool_call')
    assert.equal(start.name, 'srv.lookup')
    const live = timeline.update({
      sessionUpdate: 'tool_call_update',
      toolCallId: 'call',
      title: 'Done',
      rawOutput: output,
      status: 'completed',
    })
    const projector = new BrainTranscriptProjector('session', undefined, true, [
      'srv',
    ])
    projector.bindToolCallIds(['call'])
    projector.project({
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      tool_calls: [{ name, args: input }],
    })
    const [replayed] = projector.project({
      step_index: 2,
      type: 'GENERIC',
      status: 'DONE',
      content: output,
    })
    assert.deepEqual(replayed, live)
    assert.ok(live?.type === 'tool_call')
    assert.equal(live.status, 'completed')
    assert.deepEqual(live.detail, { type: 'unknown', input, output })
  }
})

// Format established from AGY 1.5.8 GetMcpToolNameWithPrefix; values are synthetic.
test('native underscore names require a unique configured server, including collision with legacy encoding', () => {
  const call = { title: 'Run default_api:mcp_srv_one_lookup_item' }
  const grant = {
    preapproved: [
      { kind: 'mcp' as const, server: 'srv_one', tool: 'lookup_item' },
    ],
  }
  assert.deepEqual(resolveMcpToolIdentity(call, ['srv_one']), {
    server: 'srv_one',
    tool: 'lookup_item',
  })
  assert.equal(toolDisplayName(call, ['srv_one']), 'srv_one.lookup_item')
  assert.equal(isPreapprovedMcpTool(call, grant, ['srv_one']), true)
  for (const servers of [[], ['other'], ['srv', 'srv_one']]) {
    assert.equal(resolveMcpToolIdentity(call, servers), undefined)
    assert.equal(isPreapprovedMcpTool(call, grant, servers), false)
    assert.equal(toolDisplayName(call, servers), call.title)
  }
  assert.equal(
    resolveMcpToolIdentity({ name: 'mcp__srv__lookup' }, ['_srv']),
    undefined,
  )
  assert.deepEqual(
    resolveMcpToolIdentity({ name: 'mcp__srv_lookup' }, ['_srv']),
    { server: '_srv', tool: 'lookup' },
  )
  assert.equal(
    isPreapprovedMcpTool({ name: 'mcp_srv_one_lookup_Item' }, grant, [
      'srv_one',
    ]),
    false,
  )
})
