import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  isPreapprovedMcpTool,
  permissionKind,
  pickPermissionOption,
} from '../server/permissions.ts'

const options = [
  { optionId: 'always', kind: 'allow_always' },
  { optionId: 'yes', kind: 'allow_once' },
  { optionId: 'never', kind: 'reject_always' },
  { optionId: 'no', kind: 'reject_once' },
]

test('pickPermissionOption prefers one-shot options and honors selected actions', () => {
  assert.equal(pickPermissionOption(options, 'allow')?.optionId, 'yes')
  assert.equal(pickPermissionOption(options, 'deny')?.optionId, 'no')
  assert.equal(
    pickPermissionOption(options, 'allow', 'always')?.optionId,
    'always',
  )
  // A selected action must match the allow/deny intent.
  assert.equal(pickPermissionOption(options, 'deny', 'yes'), undefined)
  assert.equal(
    pickPermissionOption([{ optionId: 'a', kind: 'allow_always' }], 'allow')
      ?.optionId,
    'a',
  )
})

test('permissionKind classifies questions, mode switches, plans and tools', () => {
  const binary = [{ kind: 'allow_once' }, { kind: 'reject_once' }]
  const chooser = [{ kind: 'allow_once' }, { kind: 'allow_once' }]
  const kind = (
    title: string,
    detailType: 'plan' | 'plain_text',
    toolKind?: string,
    options = binary,
  ) =>
    permissionKind(
      { options, toolCall: { title, kind: toolKind } },
      detailType === 'plan'
        ? { type: 'plan', text: 'p' }
        : { type: 'plain_text', label: title },
    )
  assert.equal(
    kind('run_command', 'plain_text', 'execute', chooser),
    'question',
  )
  assert.equal(kind('ask_user', 'plain_text'), 'question')
  assert.equal(kind('exit_plan_mode', 'plain_text'), 'mode')
  assert.equal(kind('x', 'plain_text', 'switch_mode'), 'mode')
  assert.equal(kind('write_to_file', 'plan', 'edit'), 'plan')
  assert.equal(kind('run_command', 'plain_text', 'execute'), 'tool')
})

test('isPreapprovedMcpTool matches exact direct and lazy MCP grants only', () => {
  const policy = {
    preapproved: [{ kind: 'mcp' as const, server: 'rea', tool: 'open' }],
  }
  assert.equal(isPreapprovedMcpTool('mcp__rea__open', {}, policy), true)
  assert.equal(isPreapprovedMcpTool('rea:open', {}, policy), true)
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'open' },
      policy,
    ),
    true,
  )
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'close' },
      policy,
    ),
    false,
  )
  assert.equal(isPreapprovedMcpTool('mcp__rea__close', {}, policy), false)
  assert.equal(isPreapprovedMcpTool('mcp_rea_open', {}, policy), false)
  assert.equal(isPreapprovedMcpTool('mcp__rea__open', {}, undefined), false)
})
