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
const nativeNames = [
  'invoke_subagent',
  'send_message',
  'manage_subagents',
  'manage_task',
  'schedule',
] as const
// These are protocol fixtures, not captured private sessions.
const nativeForms = (name: string) => [
  name,
  `default_api:${name}`,
  `Run ${name}`,
  `Run default_api:${name}`,
]
const collisions = (name: string) => [
  `mcp__example__${name}`,
  `mcp_example_${name}`,
  `Vendor.${name}`,
  `${name}_archive`,
  `Run ${name} extra`,
  `default_api:Vendor.${name}`,
]
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

test('native lifecycle titles share case and whitespace normalization without accepting foreign identities', () => {
  // Parser-contract cases, not evidence that agy emits these wrapper variants.
  for (const expected of nativeNames) {
    for (const prefix of ['run ', 'RUN ', 'Run  ', 'Run\t']) {
      for (const name of [expected, `default_api:${expected}`])
        assert.equal(isNativeToolName(`${prefix}${name}`, expected), true)
      for (const name of collisions(expected))
        assert.equal(isNativeToolName(`${prefix}${name}`, expected), false)
      assert.equal(
        isNativeToolName(`${prefix}${expected.toUpperCase()}`, expected),
        false,
      )
    }
  }
})

test('one native identity boundary accepts documented forms and rejects foreign collisions', () => {
  for (const expected of nativeNames) {
    for (const name of nativeForms(expected))
      assert.equal(isNativeToolName(name, expected), true)
    for (const name of [
      ...collisions(expected),
      ...nativeNames.filter((n) => n !== expected),
    ])
      assert.equal(isNativeToolName(name, expected), false)
  }
})

test('foreign manage_task results preserve background and daemon tracking; native kill still clears it', () => {
  for (const name of collisions('manage_task')) {
    for (const nativeKill of nativeForms('manage_task')) {
      for (const IsDaemon of [false, true]) {
        const projector = new BrainTranscriptProjector('example')
        projector.project(
          call(1, 'run_command', {
            CommandLine: 'example',
            IsDaemon,
            MaxIterations: 3,
          }),
        )
        projector.project({
          step_index: 2,
          type: 'GENERIC',
          status: 'RUNNING',
          content:
            'Tool is running as a background task with task id: task-example',
        })
        const tracked = () =>
          IsDaemon ? projector.hasDaemons() : projector.hasBackground()
        assert.equal(tracked(), true)
        projector.project(
          call(3, name, { Action: 'kill', TaskId: 'task-example' }),
        )
        projector.project(result(4, 'external ticket updated'))
        assert.equal(tracked(), true, name)
        projector.project(
          call(5, nativeKill, { Action: 'kill', TaskId: 'task-example' }),
        )
        projector.project(result(6))
        assert.equal(tracked(), false, nativeKill)
      }
    }
  }
})

test('foreign message/spawn calls do not activate an old child before or after completion', () => {
  const subs = [
    { conversationId: child, state: 'SUBAGENT_STATE_ALIVE', spawnStepIndex: 1 },
  ]
  for (const name of [
    ...collisions('send_message'),
    ...collisions('invoke_subagent'),
    ...nativeForms('send_message'),
    ...nativeForms('invoke_subagent'),
  ]) {
    const projector = new BrainTranscriptProjector('example')
    projector.project(call(10, name, { Recipient: child }))
    const expected = nativeForms('send_message').includes(name) ? [child] : []
    assert.deepEqual([...projector.touchedSubagentIds()], expected)
    assert.deepEqual(activeChildIdsForTurn(subs, projector, 9), expected)
    assert.equal(isSubagentMessage(name), expected.length > 0)
    projector.project(result(11, JSON.stringify({ conversationId: child })))
    const completedExpected =
      nativeForms('send_message').includes(name) ||
      nativeForms('invoke_subagent').includes(name)
        ? [child]
        : []
    assert.deepEqual(
      activeChildIdsForTurn(subs, projector, 9),
      completedExpected,
    )
  }
})

test('foreign spawn cards cannot acquire ownership and foreign management does not request a kill sync', () => {
  const events: unknown[] = []
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
  })
  for (const name of collisions('invoke_subagent')) {
    assert.equal(registry.canLinkChild(child, parent, name), false)
    // Exercise the baseline mapper -> registry boundary, even when an older
    // mapper classifies the foreign tool as a sub_agent card.
    registry.linkSubsession('example', parent, {
      id: 'example:tool:c',
      type: 'tool_call',
      callId: 'c',
      name,
      status: 'completed',
      error: null,
      detail: toolDetail({ title: name, rawOutput: { conversationId: child } }),
    })
  }
  assert.deepEqual(events, [])
  for (const name of nativeForms('invoke_subagent'))
    assert.equal(registry.canLinkChild(child, parent, name), true)
  assert.equal(registry.canLinkChild(child, parent, 'send_message'), false)
  registry.rememberSubagents([
    { conversationId: child, state: 'SUBAGENT_STATE_ALIVE', spawnStepIndex: 1 },
  ])
  assert.equal(registry.canLinkChild(child, parent, 'send_message'), true)
  for (const name of [
    ...collisions('invoke_subagent'),
    ...collisions('send_message'),
  ])
    assert.equal(registry.canLinkChild(child, parent, name), false)
  let syncs = 0
  registry.syncKilledChildren = async () => {
    syncs++
  }
  for (const name of collisions('manage_subagents'))
    registry.linkSubsession('example', parent, {
      id: 'example:tool:c',
      type: 'tool_call',
      callId: 'c',
      name,
      status: 'completed',
      error: null,
      detail: { type: 'plain_text', text: 'done' },
    })
  assert.equal(syncs, 0)
  for (const name of nativeForms('manage_subagents'))
    registry.linkSubsession('example', parent, {
      id: 'example:tool:c',
      type: 'tool_call',
      callId: 'c',
      name,
      status: 'completed',
      error: null,
      detail: { type: 'plain_text', text: 'done' },
    })
  assert.equal(syncs, nativeForms('manage_subagents').length)
})

test('Brain metadata and inbox agree on namespaced native spawn identity', async () => {
  for (const name of [
    ...nativeForms('invoke_subagent'),
    'mcp__example__invoke_subagent',
  ]) {
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
      const native = nativeForms('invoke_subagent').includes(name)
      const inbox = await readInboxState(root, child)
      assert.equal(inbox.consumedNonUserCount, native ? 0 : 1)
      const [sub] = await readSubagents(root, parent)
      assert.equal(sub.spawnStepIndex, native ? 23 : 1)
      assert.equal(sub.spawnCallId, native ? 'spawn-example' : undefined)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  }
})

test('namespaced schedule retains cancellation behavior without treating a foreign schedule as native', () => {
  for (const name of [...nativeForms('schedule'), 'Vendor.schedule']) {
    const projector = new BrainTranscriptProjector('example')
    projector.project(call(1, name, { TimerCondition: 'any' }))
    projector.project({
      step_index: 2,
      type: 'GENERIC',
      status: 'WAITING',
      content:
        'Tool is running as a background task with task id: timer-example',
    })
    const items = projector.project({
      step_index: 3,
      type: 'SYSTEM_MESSAGE',
      content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-11T00:00:00Z sender=${child} priority=MESSAGE_PRIORITY_HIGH content=report\n</SYSTEM_MESSAGE>`,
    })
    if (name === 'Vendor.schedule') {
      assert.equal(projector.hasBackground(), true)
      assert.deepEqual(
        items.filter((item) => item.type === 'tool_call'),
        [],
      )
    } else {
      assert.equal(projector.hasBackground(), false)
      assert.ok(
        items.some(
          (item) => item.type === 'tool_call' && item.status === 'canceled',
        ),
      )
    }
  }
})
