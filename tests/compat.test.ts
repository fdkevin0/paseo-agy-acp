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
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents } from './helpers.ts'

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
  const provider = await createProvider(new AgyUsageSource()).connect({
    versions: [1],
    capabilities: options.capabilities ?? PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./compat-acp.ts', import.meta.url))],
      env: {
        HOME: home,
        PATH: process.env.PATH ?? '',
        AGY_COMPAT_SCENARIO: scenario,
      },
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

test('native Brain calls without IDs complete the existing ACP tool card', async () => {
  await run('background', (events) => {
    const tools = events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'tool_call'
        ? [event.item]
        : [],
    )
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
    const tools = events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'tool_call'
        ? [event.item]
        : [],
    )
    const final = new Map(tools.map((tool) => [tool.callId, tool]))
    assert.deepEqual([...final.keys()], ['call_123456789', 'call_987654321'])
    for (const [id, expected] of [
      ['call_123456789', 'first'],
      ['call_987654321', 'second'],
    ]) {
      const tool = final.get(id)!
      assert.equal(tool.status, 'completed')
      assert.ok(tool.detail.type === 'shell' && tool.detail.output === expected)
    }
  })
})

for (const scenario of ['child', 'history'])
  test(`Brain fills the missing child ID for ${scenario} ACP invoke with no output`, async () => {
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
      const calls = events.flatMap((event) =>
        event.type === 'timeline.item' && event.item.type === 'tool_call'
          ? [event.item]
          : [],
      )
      assert.deepEqual(
        [...new Set(calls.map((call) => call.callId))],
        ['call_234567890'],
      )
      assert.ok(
        calls.some(
          (call) =>
            call.detail.type === 'sub_agent' &&
            call.detail.childSessionId ===
              's:child:22222222-2222-4222-8222-222222222222',
        ),
      )
    })
  })

test('replaying a historical kill preserves the child timeline before cancellation', async () => {
  await run('history-kill', (events) => {
    const childEvents = events.filter(
      (event) =>
        'sessionId' in event &&
        event.sessionId === 's:child:22222222-2222-4222-8222-222222222222',
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

for (const scenario of ['kill', 'kill-all'])
  test(`native Run manage_subagents ${scenario} cancels registered children`, async () => {
    const childId = 's:child:22222222-2222-4222-8222-222222222222'
    await run(
      scenario,
      (events) => {
        assert.equal(
          events.filter(
            (event) =>
              event.type === 'session.opened' && event.sessionId === childId,
          ).length,
          1,
          'a killed child is never reopened by later settlement',
        )
        const messages = events.filter(
          (e) =>
            e.type === 'timeline.item' &&
            e.item.type === 'tool_call' &&
            e.item.callId === 'call_345678901',
        )
        assert.equal(
          messages.length,
          2,
          'only the running and completed live message events',
        )
        for (const e of messages) {
          assert.ok(
            e.type === 'timeline.item' &&
              e.item.type === 'tool_call' &&
              e.item.detail.type === 'sub_agent',
          )
          assert.equal(e.item.detail.childSessionId, childId)
        }
        const firstMessage = events.indexOf(messages[0])
        const openedIndex = events.findIndex(
          (e) => e.type === 'session.opened' && e.sessionId === childId,
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
            event.sessionId === childId &&
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
              event.type === 'session.closed' && event.sessionId === childId,
          ),
          'closing the parent closes the killed child session',
        ),
    )
  })

for (const scenario of [
  'owned-message',
  'owned-message-late',
  'owned-message-killed',
  'root-message',
  'unknown-message',
  'history-message',
]) {
  test(`message ownership is resolved before publishing: ${scenario}`, async () => {
    await run(scenario, (events) => {
      const expected =
        (scenario.startsWith('owned-message') &&
          !scenario.endsWith('killed')) ||
        scenario === 'history-message'
      const messages = events.filter(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'call_345678901',
      )
      assert.equal(messages.length, 2)
      for (const e of messages) {
        assert.ok(
          e.type === 'timeline.item' &&
            e.item.type === 'tool_call' &&
            e.item.detail.type === 'sub_agent',
        )
        assert.equal(
          e.item.detail.childSessionId,
          expected ? 's:child:22222222-2222-4222-8222-222222222222' : undefined,
        )
      }
      assert.equal(
        events.filter((e) => e.type === 'session.opened').length,
        expected ? 2 : 1,
      )
    })
  })
}
test('skip restoration resolves the first live message to a historical child', async () => {
  await run(
    'owned-message',
    (events) => {
      const messages = events.filter(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'call_345678901',
      )
      assert.equal(messages.length, 2)
      for (const e of messages) {
        assert.ok(
          e.type === 'timeline.item' &&
            e.item.type === 'tool_call' &&
            e.item.detail.type === 'sub_agent',
        )
        assert.equal(
          e.item.detail.childSessionId,
          's:child:22222222-2222-4222-8222-222222222222',
        )
      }
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
      for (const e of events) {
        if (
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.detail.type === 'sub_agent'
        )
          assert.equal(e.item.detail.childSessionId, undefined)
      }
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
    const childId = 's:child:22222222-2222-4222-8222-222222222222'
    assert.equal(
      events.filter(
        (e) => e.type === 'session.opened' && e.sessionId === childId,
      ).length,
      1,
    )
    const saved = events.findIndex(
      (e) =>
        e.type === 'timeline.item' &&
        e.sessionId === childId &&
        e.item.type === 'assistant_message' &&
        e.item.text === 'Saved before kill',
    )
    const canceled = events.findIndex(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === childId &&
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
