import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  ProviderEventSchema,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'
import { AgyUsageSource } from '../server/usage.ts'

async function run(
  scenario: string,
  verify: (events: ProviderEvent[]) => void,
  afterClose?: (events: ProviderEvent[]) => void,
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-compat-'))
  const provider = await createProvider(new AgyUsageSource()).connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
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
  const events: ProviderEvent[] = []
  provider.onEvent((event) => {
    ProviderEventSchema.parse(event)
    events.push(event)
  })
  async function until(predicate: (event: ProviderEvent) => boolean) {
    const deadline = Date.now() + 5000
    while (!events.some(predicate)) {
      assert.ok(
        Date.now() < deadline,
        `Missing event: ${JSON.stringify(events)}`,
      )
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  try {
    await provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'replay',
      ...(scenario.startsWith('history')
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
