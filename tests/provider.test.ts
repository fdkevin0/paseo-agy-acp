import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, mkdir, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  ProviderEventSchema,
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'

async function harness(env: Record<string, string> = {}) {
  const provider = await createProvider().connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { PATH: process.env.PATH ?? '', ...env },
    },
  })
  const events: ProviderEvent[] = []
  provider.onEvent((e) => {
    ProviderEventSchema.parse(e)
    events.push(e)
  })
  async function wait(predicate: (e: ProviderEvent) => boolean) {
    const deadline = Date.now() + 5000
    while (Date.now() < deadline) {
      const event = events.find(predicate)
      if (event) return event
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    assert.fail(`Timed out: ${JSON.stringify(events)}`)
  }
  const config = {
    cwd: process.cwd(),
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  return { provider, events, wait, config }
}

test('open, permission, tool merge, duplicate prompt, cancel and runtime crash', async () => {
  const h = await harness()
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready')
    const prompt = {
      type: 'session.prompt' as const,
      sessionId: 's',
      prompt: {
        clientMessageId: 'p',
        delivery: 'auto' as const,
        input: {
          type: 'message' as const,
          content: [{ type: 'text' as const, text: 'hello' }],
        },
      },
    }
    await h.provider.send(prompt)
    const permission = await h.wait((e) => e.type === 'session.permission')
    assert.equal(permission.type, 'session.permission')
    if (permission.type !== 'session.permission') return
    assert.equal(permission.request.title, 'run_command')
    assert.deepEqual(permission.request.input, { CommandLine: 'echo test' })
    assert.equal(permission.request.detail?.type, 'shell')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: permission.request.id,
      response: { behavior: 'deny', selectedActionId: 'yes' },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.equal(
      h.events.filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'user_message',
      ).length,
      1,
    )
    const usage = h.events.filter((e) => e.type === 'session.usage').at(-1)
    assert.ok(usage?.type === 'session.usage')
    assert.deepEqual(usage.usage, {
      contextWindowUsedTokens: 10,
      contextWindowMaxTokens: 1000,
      totalCostUsd: 0.01,
      inputTokens: 12,
      outputTokens: 3,
      cachedInputTokens: 4,
    })
    const terminalIndex = h.events.findIndex(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    )
    assert.ok(
      h.events.findLastIndex((e) => e.type === 'session.usage') < terminalIndex,
    )
    const tool = h.events.find(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.status === 'completed' &&
        e.item.callId === 'shell',
    )
    assert.ok(
      tool && tool.type === 'timeline.item' && tool.item.type === 'tool_call',
    )
    assert.deepEqual(tool.item.detail, {
      type: 'shell',
      command: 'echo test',
      cwd: undefined,
      output: 'test',
    })
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'cancelled',
      ),
    )
    await h.provider.send(prompt)
    assert.equal(
      h.events.filter((e) => e.type === 'session.prompt_result').length,
      1,
    )
    h.events.length = 0
    await h.provider.send({
      ...prompt,
      prompt: {
        ...prompt.prompt,
        clientMessageId: 'wait',
        input: { type: 'message', content: [{ type: 'text', text: 'wait' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'started')
    await h.provider.send({
      type: 'session.interrupt',
      requestId: 'stop',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'canceled')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'unfinished' &&
          e.item.status === 'canceled',
      ),
    )
    h.events.length = 0
    await h.provider.send({
      ...prompt,
      prompt: {
        ...prompt.prompt,
        clientMessageId: 'crash',
        input: { type: 'message', content: [{ type: 'text', text: 'crash' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'failed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'unfinished' &&
          e.item.status === 'failed' &&
          e.item.error,
      ),
    )
  } finally {
    await h.provider.close()
  }
})

test('list/import and replay before ready; skip does not replay', async () => {
  const h = await harness()
  try {
    await h.provider.send({
      type: 'sessions',
      requestId: 'list',
      query: 'test',
    })
    const list = await h.wait((e) => e.type === 'sessions')
    assert.ok(list.type === 'sessions' && list.sessions.length === 1)
    for (const history of ['replay', 'skip'] as const) {
      h.events.length = 0
      await h.provider.send({
        type: 'session.open',
        requestId: history,
        sessionId: history,
        config: h.config,
        history,
        persistence: { version: 1, data: { sessionId: 'native-test' } },
      })
      await h.wait((e) => e.type === 'session.ready' && e.sessionId === history)
      assert.equal(
        h.events.filter((e) => e.type === 'timeline.item').length,
        history === 'replay' ? 3 : 0,
      )
      assert.equal(h.events.at(-1)?.type, 'session.ready')
      assert.equal(
        h.events.filter((e) => e.type === 'session.opened' && e.parentSessionId)
          .length,
        history === 'replay' ? 1 : 0,
      )
    }
  } finally {
    await h.provider.close()
  }
})

test('catalog requests share a probe, close it, and skip unchanged opening config', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-catalog-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await Promise.all(
      ['a', 'b'].map((requestId) =>
        h.provider.send({ type: 'catalog', requestId }),
      ),
    )
    const catalog = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'a',
    )
    assert.ok(catalog.type === 'catalog')
    assert.deepEqual(
      catalog.catalog.models[0].thinkingOptions?.map((o) => o.id),
      ['low'],
    )
    await h.wait((e) => e.type === 'catalog' && e.requestId === 'b')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
    await h.provider.send({ type: 'catalog', requestId: 'c' })
    await h.wait((e) => e.type === 'catalog' && e.requestId === 'c')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: { ...h.config, model: 'test' },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\nnew\n')
    assert.ok(
      h.events.some(
        (e) => e.type === 'session.config' && e.config.model === 'test',
      ),
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('send rejects malformed input, unsupported capabilities and closed connections', async () => {
  const provider = await createProvider().connect({
    versions: [1],
    capabilities: [],
  })
  try {
    await assert.rejects(provider.send({ type: 'sessions', requestId: '' }))
    await assert.rejects(
      provider.send({ type: 'sessions', requestId: 'list' }),
      /session.list/,
    )
  } finally {
    await provider.close()
  }
  await assert.rejects(
    provider.send({ type: 'catalog', requestId: 'closed' }),
    /connection is closed/,
  )
})

test('clearPendingPermissions cancels approval before rejecting a busy prompt', async () => {
  const h = await harness()
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    const prompt = {
      clientMessageId: 'first',
      delivery: 'auto' as const,
      input: {
        type: 'message' as const,
        content: [{ type: 'text' as const, text: 'hello' }],
      },
    }
    await h.provider.send({ type: 'session.prompt', sessionId: 's', prompt })
    await h.wait((e) => e.type === 'session.permission')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        ...prompt,
        clientMessageId: 'second',
        clearPendingPermissions: true,
      },
    })
    await h.wait((e) => e.type === 'session.permission_resolved')
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'session.prompt_result' &&
          e.clientMessageId === 'second' &&
          e.result.type === 'failed',
      ),
    )
  } finally {
    await h.provider.close()
  }
})

test('catalog discovery survives an ACP close error', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-close-error-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_CLOSE_ERROR: '1',
    AGY_TEST_LOG: log,
  })
  try {
    await h.provider.send({ type: 'catalog', requestId: 'catalog' })
    const catalog = await h.wait((e) => e.type === 'catalog')
    assert.ok(catalog.type === 'catalog' && catalog.catalog.models.length > 0)
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
    assert.ok(!h.events.some((e) => e.type === 'request.failed'))
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('malformed wire permission is canceled instead of blocking or throwing', async () => {
  const h = await harness({ AGY_TEST_BAD_PERMISSION: '1' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'bad-permission',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(!h.events.some((e) => e.type === 'session.permission'))
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'cancelled',
      ),
    )
  } finally {
    await h.provider.close()
  }
})

test('child Brain watching replaces running cards with FIFO results', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-child-'))
  const id = '22222222-2222-4222-8222-222222222222'
  const dir = join(
    home,
    '.gemini/antigravity-cli/brain',
    id,
    '.system_generated/logs',
  )
  await mkdir(dir, { recursive: true })
  await writeFile(
    join(dir, 'transcript.jsonl'),
    [
      {
        type: 'PLANNER_RESPONSE',
        source: 'MODEL',
        step_index: 1,
        tool_calls: [
          {
            id: 'call',
            name: 'run_command',
            args: { CommandLine: '"echo child"' },
          },
        ],
      },
      {
        type: 'GENERIC',
        source: 'MODEL',
        step_index: 2,
        status: 'DONE',
        content: 'child output',
      },
      {
        type: 'CHECKPOINT',
        source: 'SYSTEM',
        step_index: 3,
        content: 'metadata',
      },
    ]
      .map((r) => JSON.stringify(r) + '\n')
      .join(''),
  )
  const h = await harness({ HOME: home })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 'parent',
      config: h.config,
      history: 'replay',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    const childId = `parent:child:${id}`
    await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.sessionId === childId &&
        e.item.type === 'tool_call' &&
        e.item.status === 'completed',
    )
    const child = h.events.filter(
      (e) => e.type === 'timeline.item' && e.sessionId === childId,
    )
    assert.equal(child.length, 2)
    assert.ok(
      child[0].type === 'timeline.item' &&
        child[0].item.type === 'tool_call' &&
        child[0].item.status === 'running',
    )
    assert.ok(
      child[1].type === 'timeline.item' &&
        child[1].item.type === 'tool_call' &&
        child[1].item.status === 'completed',
    )
    assert.equal(child[0].item.id, child[1].item.id)
    assert.ok(
      child[1].item.detail.type === 'shell' &&
        child[1].item.detail.output === 'child output',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
