import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
  type ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents, isolatedEnv } from './helpers.ts'

const CHILD_SESSION_ID = 's:child:22222222-2222-4222-8222-222222222222'

type ToolCallItem = Extract<ProviderTimelineItem, { type: 'tool_call' }>
type SubAgentToolCallItem = ToolCallItem & {
  detail: Extract<ToolCallItem['detail'], { type: 'sub_agent' }>
}

function toolCalls(events: readonly ProviderEvent[]): ToolCallItem[] {
  return events
    .filter(
      (event): event is Extract<ProviderEvent, { type: 'timeline.item' }> =>
        event.type === 'timeline.item',
    )
    .map((event) => event.item)
    .filter((item): item is ToolCallItem => item.type === 'tool_call')
}

function subAgentToolCalls(
  events: readonly ProviderEvent[],
): SubAgentToolCallItem[] {
  return toolCalls(events).filter(
    (item): item is SubAgentToolCallItem => item.detail.type === 'sub_agent',
  )
}

async function run(
  scenario: string,
  verify: (events: ProviderEvent[]) => void,
  afterClose?: (events: ProviderEvent[]) => void,
  options: {
    history?: 'replay' | 'skip'
    capabilities?: readonly string[]
  } = {},
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-compat-'))
  const env = isolatedEnv(home, {
    AGY_COMPAT_SCENARIO: scenario,
  })
  const provider = await createProvider(new AgyUsageSource({ env })).connect({
    versions: [1],
    capabilities: options.capabilities ?? PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./compat-acp.ts', import.meta.url))],
      env,
    },
  })
  const {
    events,
    protocolErrors,
    wait: until,
  } = collectProviderEvents(provider, (event) => {
    if (
      event.type === 'timeline.item' &&
      event.item.type === 'tool_call' &&
      event.item.callId === 'call_345678901' &&
      event.item.status === 'completed'
    )
      writeFileSync(join(home, 'message-observed'), '')
  })
  try {
    await provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: options.history ?? 'replay',
      ...(scenario.startsWith('history') || options.history === 'skip'
        ? {
            persistence: {
              version: 1,
              data: { sessionId: '11111111-1111-4111-8111-111111111111' },
            },
          }
        : {}),
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await until(
      (event) => event.type === 'session.ready' && event.sessionId === 's',
    )
    if (scenario.startsWith('history')) {
      assert.deepEqual(protocolErrors, [])
      verify(events)
      return
    }
    await provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'prompt',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'test' }] },
      },
    })
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.sessionId === 's' &&
        event.state === 'completed',
    )
    assert.deepEqual(protocolErrors, [])
    verify(events)
  } finally {
    await provider.close()
    afterClose?.(events)
    await rm(home, { recursive: true, force: true })
  }
}

async function assertMissingChildIdFilled(scenario: 'child' | 'history') {
  await run(scenario, (events) => {
    const children = events.filter(
      (event) =>
        event.type === 'session.opened' && event.parentSessionId === 's',
    )
    assert.equal(
      children.length,
      1,
      'the initial invocation must register its native child',
    )
    const calls = toolCalls(events)
    assert.deepEqual(
      [...new Set(calls.map((call) => call.callId))],
      ['call_234567890'],
    )
    assert.ok(
      calls.some(
        (call) =>
          call.detail.type === 'sub_agent' &&
          call.detail.childSessionId === CHILD_SESSION_ID,
      ),
    )
  })
}

async function assertManageSubagentsCancelsChildren(
  scenario: 'kill' | 'kill-all',
) {
  await run(
    scenario,
    (events) => {
      assert.equal(
        events.filter(
          (event) =>
            event.type === 'session.opened' &&
            event.sessionId === CHILD_SESSION_ID,
        ).length,
        1,
        'a killed child is never reopened by later settlement',
      )
      const messageEvents = events.filter(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'call_345678901',
      )
      assert.equal(
        messageEvents.length,
        2,
        'only the running and completed live message events',
      )
      assert.deepEqual(
        subAgentToolCalls(events)
          .filter((item) => item.callId === 'call_345678901')
          .map((item) => item.detail.childSessionId),
        [CHILD_SESSION_ID, CHILD_SESSION_ID],
      )
      const firstMessage = events.indexOf(messageEvents[0])
      const openedIndex = events.findIndex(
        (e) => e.type === 'session.opened' && e.sessionId === CHILD_SESSION_ID,
      )
      const manageIndex = events.findIndex(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'call_456789012',
      )
      assert.ok(openedIndex < firstMessage && firstMessage < manageIndex)
      const canceled = events.findIndex(
        (event) =>
          event.type === 'session.turn' &&
          event.sessionId === CHILD_SESSION_ID &&
          event.state === 'canceled',
      )
      const parentDone = events.findIndex(
        (event) =>
          event.type === 'session.turn' &&
          event.sessionId === 's' &&
          event.state === 'completed',
      )
      assert.ok(
        canceled >= 0,
        'a successful native cancellation must stop the child watcher',
      )
      assert.ok(
        canceled < parentDone,
        'the child is canceled before the parent turn completes',
      )
    },
    (events) =>
      assert.ok(
        events.some(
          (event) =>
            event.type === 'session.closed' &&
            event.sessionId === CHILD_SESSION_ID,
        ),
        'closing the parent closes the killed child session',
      ),
  )
}

async function assertMessageOwnership(
  scenario: string,
  expectedChildSessionId: string | undefined,
  expectedOpenedCount: number,
) {
  await run(scenario, (events) => {
    const messages = subAgentToolCalls(events).filter(
      (item) => item.callId === 'call_345678901',
    )
    assert.deepEqual(
      messages.map((item) => item.detail.childSessionId),
      [expectedChildSessionId, expectedChildSessionId],
    )
    assert.equal(
      events.filter((e) => e.type === 'session.opened').length,
      expectedOpenedCount,
    )
  })
}

test('native Brain calls without IDs complete the existing ACP tool card', async () => {
  await run('background', (events) => {
    const tools = toolCalls(events)
    assert.deepEqual(
      [...new Set(tools.map((tool) => tool.callId))],
      ['call_123456789'],
    )
    const final = tools.at(-1)!
    assert.equal(final.status, 'completed')
    assert.ok(final.detail.type === 'shell' && final.detail.output === 'output')
  })
})

test('identical parallel native calls retain their ACP identities when results arrive in reverse order', async () => {
  await run('parallel', (events) => {
    const final = new Map(toolCalls(events).map((tool) => [tool.callId, tool]))
    assert.deepEqual([...final.keys()], ['call_123456789', 'call_987654321'])
    const first = final.get('call_123456789')!
    assert.equal(first.status, 'completed')
    assert.ok(first.detail.type === 'shell' && first.detail.output === 'first')
    const second = final.get('call_987654321')!
    assert.equal(second.status, 'completed')
    assert.ok(
      second.detail.type === 'shell' && second.detail.output === 'second',
    )
  })
})

test('Brain fills the missing child ID for child ACP invoke with no output', () =>
  assertMissingChildIdFilled('child'))

test('Brain fills the missing child ID for history ACP invoke with no output', () =>
  assertMissingChildIdFilled('history'))

test('replaying a historical kill preserves the child timeline before cancellation', async () => {
  await run('history-kill', (events) => {
    const childEvents = events.filter(
      (event) => 'sessionId' in event && event.sessionId === CHILD_SESSION_ID,
    )
    const replyIndex = childEvents.findIndex(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'Saved child reply',
    )
    const canceledIndex = childEvents.findIndex(
      (event) => event.type === 'session.turn' && event.state === 'canceled',
    )
    assert.ok(replyIndex >= 0, 'the child history must be read')
    assert.ok(canceledIndex > replyIndex, 'cancel only after replaying history')
    const finalTool = childEvents.findLast(
      (event) =>
        event.type === 'timeline.item' && event.item.type === 'tool_call',
    )
    assert.ok(
      finalTool?.type === 'timeline.item' &&
        finalTool.item.type === 'tool_call' &&
        finalTool.item.status === 'canceled',
    )
  })
})

test('native Run manage_subagents kill cancels registered children', () =>
  assertManageSubagentsCancelsChildren('kill'))

test('native Run manage_subagents kill-all cancels registered children', () =>
  assertManageSubagentsCancelsChildren('kill-all'))

test('message ownership is resolved before publishing: owned-message', () =>
  assertMessageOwnership('owned-message', CHILD_SESSION_ID, 2))

test('message ownership is resolved before publishing: owned-message-late', () =>
  assertMessageOwnership('owned-message-late', CHILD_SESSION_ID, 2))

test('message ownership is resolved before publishing: owned-message-killed', () =>
  assertMessageOwnership('owned-message-killed', undefined, 1))

test('message ownership is resolved before publishing: root-message', () =>
  assertMessageOwnership('root-message', undefined, 1))

test('message ownership is resolved before publishing: unknown-message', () =>
  assertMessageOwnership('unknown-message', undefined, 1))

test('message ownership is resolved before publishing: history-message', () =>
  assertMessageOwnership('history-message', CHILD_SESSION_ID, 2))

test('skip restoration resolves the first live message to a historical child', async () => {
  await run(
    'owned-message',
    (events) => {
      const messages = subAgentToolCalls(events).filter(
        (item) => item.callId === 'call_345678901',
      )
      assert.deepEqual(
        messages.map((item) => item.detail.childSessionId),
        [CHILD_SESSION_ID, CHILD_SESSION_ID],
      )
    },
    undefined,
    { history: 'skip' },
  )
})

test('without subsession capability message targets never leak native IDs', async () => {
  await run(
    'owned-message',
    (events) => {
      assert.equal(events.filter((e) => e.type === 'session.opened').length, 1)
      assert.deepEqual(
        subAgentToolCalls(events).map((item) => ({
          callId: item.callId,
          status: item.status,
          childSessionId: item.detail.childSessionId,
        })),
        [
          {
            callId: 'call_345678901',
            status: 'running',
            childSessionId: undefined,
          },
          {
            callId: 'call_345678901',
            status: 'completed',
            childSessionId: undefined,
          },
        ],
      )
    },
    undefined,
    {
      capabilities: PROVIDER_CAPABILITIES.filter(
        (c) => c !== 'session.subsession',
      ),
    },
  )
})

test('a child spawned and killed in one live poll retains its history before cancellation', async () => {
  await run('spawn-kill', (events) => {
    assert.equal(
      events.filter(
        (e) => e.type === 'session.opened' && e.sessionId === CHILD_SESSION_ID,
      ).length,
      1,
    )
    const saved = events.findIndex(
      (e) =>
        e.type === 'timeline.item' &&
        e.sessionId === CHILD_SESSION_ID &&
        e.item.type === 'assistant_message' &&
        e.item.text === 'Saved before kill',
    )
    const canceled = events.findIndex(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === CHILD_SESSION_ID &&
        e.state === 'canceled',
    )
    const completed = events.findIndex(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 's' &&
        e.state === 'completed',
    )
    assert.ok(saved >= 0 && saved < canceled && canceled < completed)
  })
})
