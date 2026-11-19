import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { isNativeToolName } from '../server/common.ts'
import {
  BrainTranscriptProjector,
  activeChildIdsForTurn,
} from '../server/brain-projector.ts'
import { readInboxState, readSubagents } from '../server/brain-io.ts'
import { SubagentRegistry, isSubagentMessage } from '../server/subagents.ts'
import { toolDetail } from '../server/mapping.ts'

const parent = '11111111-1111-4111-8111-111111111111'
const child = '22222222-2222-4222-8222-222222222222'

// These are protocol fixtures, not captured private sessions.
const call = (step_index: number, name: string, args: object) => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  tool_calls: [{ name, args }],
})
const result = (step_index: number, content = 'done') => ({
  step_index,
  type: 'GENERIC',
  status: 'DONE',
  content,
})

function expectBackgroundManageTaskGuard(
  foreignName: string,
  nativeKillName: string,
) {
  const projector = new BrainTranscriptProjector('example')
  projector.project(
    call(1, 'run_command', {
      CommandLine: 'example',
      IsDaemon: false,
      MaxIterations: 3,
    }),
  )
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: 'Tool is running as a background task with task id: task-example',
  })
  assert.equal(projector.hasBackground(), true)
  projector.project(
    call(3, foreignName, { Action: 'kill', TaskId: 'task-example' }),
  )
  projector.project(result(4, 'external ticket updated'))
  assert.equal(projector.hasBackground(), true, foreignName)
  projector.project(
    call(5, nativeKillName, { Action: 'kill', TaskId: 'task-example' }),
  )
  projector.project(result(6))
  assert.equal(projector.hasBackground(), false, nativeKillName)
}

function expectDaemonManageTaskGuard(
  foreignName: string,
  nativeKillName: string,
) {
  const projector = new BrainTranscriptProjector('example')
  projector.project(
    call(1, 'run_command', {
      CommandLine: 'example',
      IsDaemon: true,
      MaxIterations: 3,
    }),
  )
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'RUNNING',
    content: 'Tool is running as a background task with task id: task-example',
  })
  assert.equal(projector.hasDaemons(), true)
  projector.project(
    call(3, foreignName, { Action: 'kill', TaskId: 'task-example' }),
  )
  projector.project(result(4, 'external ticket updated'))
  assert.equal(projector.hasDaemons(), true, foreignName)
  projector.project(
    call(5, nativeKillName, { Action: 'kill', TaskId: 'task-example' }),
  )
  projector.project(result(6))
  assert.equal(projector.hasDaemons(), false, nativeKillName)
}

function expectSubagentActivation(
  name: string,
  expectedBefore: readonly string[],
  expectedIsMessage: boolean,
  expectedAfter: readonly string[],
) {
  const subs = [
    { conversationId: child, state: 'SUBAGENT_STATE_ALIVE', spawnStepIndex: 1 },
  ]
  const projector = new BrainTranscriptProjector('example')
  projector.project(call(10, name, { Recipient: child }))
  assert.deepEqual([...projector.touchedSubagentIds()], expectedBefore)
  assert.deepEqual(activeChildIdsForTurn(subs, projector, 9), expectedBefore)
  assert.equal(isSubagentMessage(name), expectedIsMessage)
  projector.project(result(11, JSON.stringify({ conversationId: child })))
  assert.deepEqual(activeChildIdsForTurn(subs, projector, 9), expectedAfter)
}

async function expectBrainSpawnIdentity(
  name: string,
  expectedConsumedNonUserCount: number,
  expectedSpawnStepIndex: number,
  expectedSpawnCallId: string | undefined,
) {
  const root = await mkdtemp(join(tmpdir(), 'agy-native-identity-'))
  try {
    const subDir = join(root, parent, '.system_generated/subagents')
    const msgDir = join(root, child, '.system_generated/messages')
    await mkdir(subDir, { recursive: true })
    await mkdir(msgDir, { recursive: true })
    await writeFile(
      join(subDir, `${child}.json`),
      JSON.stringify({
        conversationId: child,
        state: 'SUBAGENT_STATE_ALIVE',
        spawnStepIndex: 1,
      }),
    )
    await writeFile(
      join(msgDir, 'init.json'),
      JSON.stringify({
        id: 'init',
        sender: parent,
        recipient: child,
        priority: 'MESSAGE_PRIORITY_HIGH',
        content: 'work',
        sourceMetadata: {
          tool: {
            conversationId: parent,
            stepIndex: 23,
            toolCall: { id: 'spawn-example', name, argumentsJson: '{}' },
          },
        },
      }),
    )
    await writeFile(join(msgDir, 'read.json'), JSON.stringify({ init: true }))
    const inbox = await readInboxState(root, child)
    assert.equal(inbox.consumedNonUserCount, expectedConsumedNonUserCount)
    const [sub] = await readSubagents(root, parent)
    assert.equal(sub.spawnStepIndex, expectedSpawnStepIndex)
    assert.equal(sub.spawnCallId, expectedSpawnCallId)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
}

function projectScheduleWakeup(name: string) {
  const projector = new BrainTranscriptProjector('example')
  projector.project(call(1, name, { TimerCondition: 'any' }))
  projector.project({
    step_index: 2,
    type: 'GENERIC',
    status: 'WAITING',
    content: 'Tool is running as a background task with task id: timer-example',
  })
  const items = projector.project({
    step_index: 3,
    type: 'SYSTEM_MESSAGE',
    content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-11T00:00:00Z sender=${child} priority=MESSAGE_PRIORITY_HIGH content=report\n</SYSTEM_MESSAGE>`,
  })
  return { projector, items }
}

function expectNativeScheduleCanceled(name: string) {
  const { projector, items } = projectScheduleWakeup(name)
  assert.equal(projector.hasBackground(), false)
  assert.ok(
    items.some(
      (item) => item.type === 'tool_call' && item.status === 'canceled',
    ),
  )
}

test('native identity boundary accepts documented wrapper forms and rejects foreign collisions', () => {
  // Documented bare, default_api-prefixed, and Run-wrapped forms
  assert.equal(isNativeToolName('invoke_subagent', 'invoke_subagent'), true)
  assert.equal(
    isNativeToolName('default_api:send_message', 'send_message'),
    true,
  )
  assert.equal(
    isNativeToolName('Run manage_subagents', 'manage_subagents'),
    true,
  )
  assert.equal(
    isNativeToolName('Run default_api:manage_task', 'manage_task'),
    true,
  )
  assert.equal(isNativeToolName('schedule', 'schedule'), true)

  // Case and whitespace normalization on the Run wrapper prefix
  assert.equal(isNativeToolName('run invoke_subagent', 'invoke_subagent'), true)
  assert.equal(
    isNativeToolName('RUN default_api:send_message', 'send_message'),
    true,
  )
  assert.equal(
    isNativeToolName('Run  manage_subagents', 'manage_subagents'),
    true,
  )
  assert.equal(isNativeToolName('Run\tmanage_task', 'manage_task'), true)

  // Rejection boundaries: case-sensitive tool name, foreign prefixes/suffixes, trailing text, mismatched native tool
  assert.equal(
    isNativeToolName('Run INVOKE_SUBAGENT', 'invoke_subagent'),
    false,
  )
  assert.equal(isNativeToolName('default_api:SCHEDULE', 'schedule'), false)
  assert.equal(
    isNativeToolName('mcp__example__invoke_subagent', 'invoke_subagent'),
    false,
  )
  assert.equal(
    isNativeToolName('Run mcp__example__manage_task', 'manage_task'),
    false,
  )
  assert.equal(
    isNativeToolName('mcp_example_send_message', 'send_message'),
    false,
  )
  assert.equal(
    isNativeToolName('Vendor.manage_subagents', 'manage_subagents'),
    false,
  )
  assert.equal(
    isNativeToolName('default_api:Vendor.schedule', 'schedule'),
    false,
  )
  assert.equal(isNativeToolName('schedule_archive', 'schedule'), false)
  assert.equal(
    isNativeToolName('Run invoke_subagent extra', 'invoke_subagent'),
    false,
  )
  assert.equal(isNativeToolName('send_message', 'invoke_subagent'), false)
})

test('foreign manage_task results preserve background and daemon tracking; native kill still clears it', () => {
  expectBackgroundManageTaskGuard('mcp__example__manage_task', 'manage_task')
  expectBackgroundManageTaskGuard(
    'Vendor.manage_task',
    'default_api:manage_task',
  )
  expectBackgroundManageTaskGuard(
    'Run manage_task extra',
    'Run default_api:manage_task',
  )
  expectDaemonManageTaskGuard('mcp__example__manage_task', 'manage_task')
  expectDaemonManageTaskGuard('mcp_example_manage_task', 'Run manage_task')
  expectDaemonManageTaskGuard(
    'default_api:Vendor.manage_task',
    'Run default_api:manage_task',
  )
})

test('foreign message/spawn calls do not activate an old child before or after completion', () => {
  expectSubagentActivation('mcp__example__send_message', [], false, [])
  expectSubagentActivation('Vendor.send_message', [], false, [])
  expectSubagentActivation('Run send_message extra', [], false, [])
  expectSubagentActivation('mcp__example__invoke_subagent', [], false, [])
  expectSubagentActivation('Vendor.invoke_subagent', [], false, [])

  expectSubagentActivation('send_message', [child], true, [child])
  expectSubagentActivation('default_api:send_message', [child], true, [child])
  expectSubagentActivation('Run send_message', [child], true, [child])
  expectSubagentActivation('Run default_api:send_message', [child], true, [
    child,
  ])

  expectSubagentActivation('invoke_subagent', [], false, [child])
  expectSubagentActivation('default_api:invoke_subagent', [], false, [child])
  expectSubagentActivation('Run invoke_subagent', [], false, [child])
  expectSubagentActivation('Run default_api:invoke_subagent', [], false, [
    child,
  ])
})

test('foreign spawn cards cannot acquire ownership and foreign management does not request a kill sync', async () => {
  const events: unknown[] = []
  let syncReads = 0
  const registry = new SubagentRegistry({
    sessionId: 'example',
    cwd: '/example',
    brainRoot: '/example',
    capabilities: ['session.subsession'],
    emit: (event) => events.push(event),
    getNativeId: () => parent,
    isParentTurnActive: () => false,
    isLoading: () => false,
    isUnavailable: () => false,
    readSubagents: async () => {
      syncReads++
      return []
    },
  })

  // Foreign spawn tools fail canLinkChild and map to unknown details.
  assert.equal(
    registry.canLinkChild(child, parent, 'mcp__example__invoke_subagent'),
    false,
  )
  assert.equal(
    registry.canLinkChild(child, parent, 'Vendor.invoke_subagent'),
    false,
  )
  assert.equal(
    toolDetail({
      title: 'mcp__example__invoke_subagent',
      rawOutput: { conversationId: child },
    }).type,
    'unknown',
  )
  assert.equal(
    toolDetail({
      title: 'Vendor.invoke_subagent',
      rawOutput: { conversationId: child },
    }).type,
    'unknown',
  )

  // Passing a crafted sub_agent card with a foreign tool name exercises linkSubsession's own guard.
  const craftedForeignSpawn = {
    id: 'example:tool:crafted',
    type: 'tool_call' as const,
    callId: 'crafted',
    name: 'foreign:invoke_subagent',
    status: 'completed' as const,
    error: null,
    detail: {
      type: 'sub_agent' as const,
      childSessionId: child,
      log: '',
    },
  }
  registry.linkSubsession('example', parent, craftedForeignSpawn)
  assert.equal(craftedForeignSpawn.detail.childSessionId, undefined)
  assert.equal(registry.hasChild(child), false)
  assert.deepEqual(events, [])

  assert.equal(registry.canLinkChild(child, parent, 'invoke_subagent'), true)
  assert.equal(
    registry.canLinkChild(child, parent, 'default_api:invoke_subagent'),
    true,
  )
  assert.equal(
    registry.canLinkChild(child, parent, 'Run invoke_subagent'),
    true,
  )
  assert.equal(
    registry.canLinkChild(child, parent, 'Run default_api:invoke_subagent'),
    true,
  )
  assert.equal(registry.canLinkChild(child, parent, 'send_message'), false)

  registry.rememberSubagents([
    { conversationId: child, state: 'SUBAGENT_STATE_ALIVE', spawnStepIndex: 1 },
  ])
  assert.equal(registry.canLinkChild(child, parent, 'send_message'), true)
  assert.equal(
    registry.canLinkChild(child, parent, 'mcp__example__send_message'),
    false,
  )
  assert.equal(
    registry.canLinkChild(child, parent, 'Vendor.send_message'),
    false,
  )

  const manageCall = (name: string) => ({
    id: 'example:tool:c',
    type: 'tool_call' as const,
    callId: 'c',
    name,
    status: 'completed' as const,
    error: null,
    detail: { type: 'plain_text' as const, text: 'done' },
  })
  registry.linkSubsession(
    'example',
    parent,
    manageCall('mcp__example__manage_subagents'),
  )
  registry.linkSubsession(
    'example',
    parent,
    manageCall('Vendor.manage_subagents'),
  )
  registry.linkSubsession(
    'example',
    parent,
    manageCall('Run manage_subagents extra'),
  )
  await Promise.resolve()
  assert.equal(syncReads, 0)

  registry.linkSubsession('example', parent, manageCall('manage_subagents'))
  registry.linkSubsession(
    'example',
    parent,
    manageCall('default_api:manage_subagents'),
  )
  registry.linkSubsession('example', parent, manageCall('Run manage_subagents'))
  registry.linkSubsession(
    'example',
    parent,
    manageCall('Run default_api:manage_subagents'),
  )
  await Promise.resolve()
  assert.equal(syncReads, 4)
})

test('Brain metadata and inbox agree on namespaced native spawn identity', async () => {
  await expectBrainSpawnIdentity('invoke_subagent', 0, 23, 'spawn-example')
  await expectBrainSpawnIdentity(
    'default_api:invoke_subagent',
    0,
    23,
    'spawn-example',
  )
  await expectBrainSpawnIdentity('Run invoke_subagent', 0, 23, 'spawn-example')
  await expectBrainSpawnIdentity(
    'Run default_api:invoke_subagent',
    0,
    23,
    'spawn-example',
  )
  await expectBrainSpawnIdentity(
    'mcp__example__invoke_subagent',
    1,
    1,
    undefined,
  )
})

test('namespaced schedule retains cancellation behavior without treating a foreign schedule as native', () => {
  expectNativeScheduleCanceled('schedule')
  expectNativeScheduleCanceled('default_api:schedule')
  expectNativeScheduleCanceled('Run schedule')
  expectNativeScheduleCanceled('Run default_api:schedule')

  const foreign = projectScheduleWakeup('Vendor.schedule')
  assert.equal(foreign.projector.hasBackground(), true)
  assert.deepEqual(
    foreign.items.filter((item) => item.type === 'tool_call'),
    [],
  )
})
