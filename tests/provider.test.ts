import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, mkdir, writeFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  ProviderEventSchema,
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { AgyUsageSource } from '../server/usage.ts'
import { record } from '../server/mapping.ts'
import { dataDirectory } from '../server/preflight.ts'
import { createProvider } from '../server/provider.ts'

async function harness(env: Record<string, string> = {}) {
  const usage = new AgyUsageSource()
  const provider = await createProvider(usage).connect({
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
  return { provider, events, wait, config, usage }
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
    await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'unfinished' &&
        e.item.status === 'running',
    )
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

test('delayed commands precede readiness on both new and restored sessions; tier is observed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-commands-'))
  const h = await harness({ HOME: home })
  try {
    for (const restored of [false, true]) {
      h.events.length = 0
      const id = String(restored)
      await h.provider.send({
        type: 'session.open',
        requestId: id,
        sessionId: id,
        config: h.config,
        history: 'skip',
        ...(restored
          ? { persistence: { version: 1, data: { sessionId: 'native-test' } } }
          : {}),
      })
      await h.wait((e) => e.type === 'session.ready')
      const commands = h.events.find((e) => e.type === 'session.commands')
      assert.ok(commands?.type === 'session.commands')
      assert.equal(commands.commands[0].name, 'compact')
      assert.equal(
        h.events.indexOf(commands) <
          h.events.findIndex((e) => e.type === 'session.ready'),
        true,
      )
    }
    assert.equal(
      h.usage.tiers.get(dataDirectory(undefined, { HOME: home })),
      'Google AI Ultra',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('disposable sessions reuse runtimes and wait for fresh commands before readiness', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-warm-'))
  const dir = dataDirectory(undefined, { HOME: home })
  const h = await harness({ HOME: home, AGY_TEST_WARM_COMMANDS: '1' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-1',
      sessionId: 's1',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's1')
    const first = h.events.find((e) => e.type === 'session.opened')
    assert.ok(first?.type === 'session.opened')
    const id = String(record(first.persistence?.data).sessionId)
    const db = join(dir, 'conversations', `${id}.db`)
    const brain = join(dir, 'brain', id)
    await mkdir(join(dir, 'conversations'), { recursive: true })
    await mkdir(brain, { recursive: true })
    await writeFile(db, 'fixture')
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-1',
      sessionId: 's1',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's1')
    await assert.rejects(stat(db), { code: 'ENOENT' })
    await assert.rejects(stat(brain), { code: 'ENOENT' })

    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-2',
      sessionId: 's2',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's2')
    const commands = h.events.filter((e) => e.type === 'session.commands')
    assert.equal(commands.length, 1)
    assert.deepEqual(
      commands[0].commands.map((c) => c.name),
      ['compact', 'plan'],
    )
    assert.ok(
      h.events.indexOf(commands[0]) <
        h.events.findIndex((e) => e.type === 'session.ready'),
    )
    const second = h.events.find((e) => e.type === 'session.opened')
    assert.ok(second?.type === 'session.opened')
    assert.notEqual(record(second.persistence?.data).sessionId, id)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('readiness has a bounded wait when the runtime advertises no commands', async () => {
  const h = await harness({ AGY_TEST_NO_COMMANDS: '1' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    assert.ok(!h.events.some((e) => e.type === 'session.commands'))
  } finally {
    await h.provider.close()
  }
})

test('close removes only new unprompted session artifacts and retains imported sessions', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-disposable-'))
  const log = join(home, 'calls')
  const id = '11111111-1111-4111-8111-111111111111'
  const dir = dataDirectory(undefined, { HOME: home })
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_CLOSE_ERROR: '1',
  })
  try {
    for (const restored of [false, true]) {
      h.events.length = 0
      await h.provider.send({
        type: 'session.open',
        requestId: 'open',
        sessionId: 's',
        config: h.config,
        history: 'skip',
        ...(restored
          ? { persistence: { version: 1, data: { sessionId: id } } }
          : {}),
      })
      await h.wait((e) => e.type === 'session.ready')
      const opened = h.events.find((e) => e.type === 'session.opened')
      assert.ok(opened?.type === 'session.opened')
      const nativeId = String(record(opened.persistence?.data).sessionId)
      const files = ['.db', '.db-wal', '.db-shm'].map((ext) =>
        join(dir, 'conversations', nativeId + ext),
      )
      const brain = join(dir, 'brain', nativeId)
      await mkdir(join(dir, 'conversations'), { recursive: true })
      await mkdir(brain, { recursive: true })
      for (const file of files) await writeFile(file, 'fixture')
      const shared = join(dir, 'conversations', 'summary.db')
      await writeFile(shared, 'keep')

      await h.provider.send({
        type: 'session.close',
        requestId: 'close',
        sessionId: 's',
      })
      await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
      for (const path of [...files, brain]) {
        if (restored) await stat(path)
        else await assert.rejects(stat(path), { code: 'ENOENT' })
      }
      assert.equal(await readFile(shared, 'utf8'), 'keep')
    }
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('context fallback counts tool steps and prompted sessions remain resumable', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_NO_CONTEXT: '1',
    AGY_TEST_BAD_PERMISSION: '1',
  })
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
        clientMessageId: 'p',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    const usage = h.events.findLast((e) => e.type === 'session.usage')
    assert.ok(usage?.type === 'session.usage')
    assert.equal(usage.usage.contextWindowUsedTokens, 11)
    assert.equal(usage.usage.contextWindowMaxTokens, 256_000)
    await h.provider.send({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
    assert.equal(await readFile(log, 'utf8'), 'new\n')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('failed probes cannot replace remembered usage launches with missing workspaces', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-failed-probe-'))
  const h = await harness({ HOME: home })
  try {
    await h.provider.send({
      type: 'catalog',
      requestId: 'missing',
      cwd: join(home, 'missing'),
    })
    await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'missing',
    )
    assert.deepEqual(
      await h.usage.discover({
        kind: 'session',
        provider: 'agy',
        env: { HOME: home },
      }),
      [],
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('stale buffered/live notifications and permissions cannot cross native sessions or close', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-stale-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_STALE_EVENTS: '1',
  })
  try {
    for (const id of ['one', 'two']) {
      await h.provider.send({
        type: 'session.open',
        requestId: id,
        sessionId: id,
        config: h.config,
        history: 'replay',
      })
      await h.wait((e) => e.type === 'session.ready' && e.sessionId === id)
      await h.provider.send({
        type: 'session.close',
        requestId: `close-${id}`,
        sessionId: id,
      })
      await h.wait((e) => e.type === 'session.closed' && e.sessionId === id)
    }
    assert.ok(
      !h.events.some(
        (e) =>
          e.type === 'session.permission' ||
          e.type === 'timeline.item' ||
          e.type === 'session.usage',
      ),
    )
    const commands = h.events.filter((e) => e.type === 'session.commands')
    assert.equal(commands.length, 2)
    assert.ok(
      commands.every(
        (e) => e.commands.length === 1 && e.commands[0].name === 'compact',
      ),
    )
    assert.ok(
      h.events
        .filter((e) => e.type === 'session.config')
        .every((e) => e.config.model === 'test'),
    )
    assert.equal(
      (await readFile(log, 'utf8'))
        .split('\n')
        .filter((s) => s === 'stale-permission:cancelled').length,
      6,
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('session listing returns its healthy process for the next open', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-list-warm-'))
  const log = join(home, 'pids')
  const h = await harness({ HOME: home, AGY_TEST_PID_LOG: log })
  try {
    await h.provider.send({
      type: 'sessions',
      requestId: 'list',
      cwd: h.config.cwd,
    })
    await h.wait((e) => e.type === 'sessions')
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    assert.equal((await readFile(log, 'utf8')).trim().split('\n').length, 1)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('cancel is sticky when it arrives before the mock installs its prompt resolver', async () => {
  const h = await harness({ AGY_TEST_CANCEL_DELAY: '1' })
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
        clientMessageId: 'wait',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'wait' }] },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'unfinished',
    )
    await h.provider.send({
      type: 'session.interrupt',
      requestId: 'cancel',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'canceled')
  } finally {
    await h.provider.close()
  }
})

test('forwards systemPrompt via _meta.antigravity.systemPrompt.append on new and loaded sessions', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sysprompt-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'new-sys',
      sessionId: 's1',
      config: { ...h.config, systemPrompt: 'Follow Paseo peer protocol.' },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's1')
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-1',
      sessionId: 's1',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's1')
    await h.provider.send({
      type: 'session.open',
      requestId: 'load-sys',
      sessionId: 's2',
      config: { ...h.config, systemPrompt: 'Follow Paseo peer protocol.' },
      history: 'skip',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's2')
    assert.equal(
      await readFile(log, 'utf8'),
      'new:Follow Paseo peer protocol.\nclose\nload:Follow Paseo peer protocol.\n',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
