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

function expectResolvedMcpCall(title: string) {
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

function expectLiveAndReplayMcpParity(name: string) {
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

function expectRejectedUnderscoreServers(
  call: { title: string },
  grant: { preapproved: Array<{ kind: 'mcp'; server: string; tool: string }> },
  servers: readonly string[],
) {
  assert.equal(resolveMcpToolIdentity(call, servers), undefined)
  assert.equal(isPreapprovedMcpTool(call, grant, servers), false)
  assert.equal(toolDisplayName(call, servers), call.title)
}

// Synthetic boundary cases; these are not captured AGY MCP transcripts.
test('one MCP resolver drives display and exact grants without rewriting input', () => {
  expectResolvedMcpCall('mcp__srv__lookup')
  expectResolvedMcpCall('default_api:mcp__srv__lookup')
  expectResolvedMcpCall('Run mcp__srv__lookup')
  expectResolvedMcpCall('Run default_api:mcp__srv__lookup')
  expectResolvedMcpCall('srv:lookup')
  expectResolvedMcpCall('call_mcp_tool')
  expectResolvedMcpCall('Run default_api:call_mcp_tool')
})

test('explicit identity wins over descriptive titles and cannot widen grants', () => {
  assert.equal(
    isPreapprovedMcpTool(
      { name: 'mcp__srv__lookup', title: 'Look up a record' },
      policy,
    ),
    true,
  )
  assert.equal(
    isPreapprovedMcpTool(
      { name: 'native_tool', title: 'Run mcp__srv__lookup' },
      policy,
    ),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool({ name: '', title: 'mcp__srv__lookup' }, policy),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      { name: 'mcp__srv__other', title: 'mcp__srv__lookup' },
      policy,
    ),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool({ title: 'Run mcp__other__lookup' }, policy),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool({ title: 'Run mcp__srv__lookup extra' }, policy),
    false,
  )
  assert.equal(isPreapprovedMcpTool({ title: 'srv.lookup' }, policy), false)
  assert.equal(
    isPreapprovedMcpTool({ title: 'mcp__srv__lookup__extra' }, policy),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool({ title: 'contrib:switch_mode' }, policy),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      {
        title: 'untrusted:call_mcp_tool',
        rawInput: { ServerName: 'srv', ToolName: 'lookup' },
      },
      policy,
    ),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      { title: 'call_mcp_tool', rawInput: { ServerName: 'srv', ToolName: 1 } },
      policy,
    ),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool({ title: 'call_mcp_tool', rawInput: null }, policy),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      {
        title: 'call_mcp_tool',
        rawInput: { ServerName: 'srv', ToolName: 'Lookup' },
      },
      policy,
    ),
    false,
  )
  assert.equal(
    toolDisplayName({ title: 'contrib:switch_mode' }),
    'contrib:switch_mode',
  )
})

test('ACP updates and Brain replay use the same MCP label and retain input/output/status', () => {
  expectLiveAndReplayMcpParity('default_api:mcp__srv__lookup')
  expectLiveAndReplayMcpParity('default_api:mcp_srv_lookup')
  expectLiveAndReplayMcpParity('default_api:call_mcp_tool')
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
  expectRejectedUnderscoreServers(call, grant, [])
  expectRejectedUnderscoreServers(call, grant, ['other'])
  expectRejectedUnderscoreServers(call, grant, ['srv', 'srv_one'])
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

test('built-in paseo server fallback applies only when explicit servers produce no underscore match', () => {
  const paseoGrant = {
    preapproved: [
      { kind: 'mcp' as const, server: 'paseo', tool: 'get_agent_activity' },
    ],
  }
  const customGrant = {
    preapproved: [
      { kind: 'mcp' as const, server: 'paseo_custom', tool: 'run' },
    ],
  }
  const paseoCall = { title: 'Run default_api:mcp_paseo_get_agent_activity' }
  assert.deepEqual(resolveMcpToolIdentity(paseoCall), {
    server: 'paseo',
    tool: 'get_agent_activity',
  })
  assert.deepEqual(resolveMcpToolIdentity(paseoCall, ['other']), {
    server: 'paseo',
    tool: 'get_agent_activity',
  })
  assert.equal(toolDisplayName(paseoCall), 'paseo.get_agent_activity')
  assert.equal(isPreapprovedMcpTool(paseoCall, paseoGrant), true)

  assert.equal(resolveMcpToolIdentity({ name: 'mcp_paseo_' }), undefined)

  // Explicit paseo_custom wins without colliding with built-in paseo fallback.
  const customCall = { name: 'mcp_paseo_custom_run' }
  assert.deepEqual(resolveMcpToolIdentity(customCall, ['paseo_custom']), {
    server: 'paseo_custom',
    tool: 'run',
  })
  assert.equal(
    toolDisplayName(customCall, ['paseo_custom']),
    'paseo_custom.run',
  )
  assert.equal(
    isPreapprovedMcpTool(customCall, customGrant, ['paseo_custom']),
    true,
  )
  assert.deepEqual(
    resolveMcpToolIdentity({ name: 'mcp_paseo_status' }, ['paseo_custom']),
    { server: 'paseo', tool: 'status' },
  )

  // When both paseo and paseo_custom are explicitly configured, fail closed as ambiguous.
  assert.equal(
    resolveMcpToolIdentity(customCall, ['paseo', 'paseo_custom']),
    undefined,
  )
  assert.equal(
    isPreapprovedMcpTool(customCall, customGrant, ['paseo', 'paseo_custom']),
    false,
  )

  // Legacy colon spelling still collides with built-in fallback and fails closed.
  assert.equal(resolveMcpToolIdentity({ name: 'mcp_paseo_foo:bar' }), undefined)
})
