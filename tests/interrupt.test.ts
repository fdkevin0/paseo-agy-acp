import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import {
  PROVIDER_CAPABILITIES,
  ProviderEventSchema,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'

/** Connect a provider to interrupt-acp.ts and record its events. */
async function connect(home: string, timeoutMs = 2000) {
  const provider = await createProvider().connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
  })
  const events: ProviderEvent[] = []
  provider.onEvent((event) => {
    ProviderEventSchema.parse(event)
    events.push(event)
  })
  async function until(predicate: (event: ProviderEvent) => boolean) {
    const deadline = Date.now() + timeoutMs
    while (!events.some(predicate)) {
      assert.ok(Date.now() < deadline, JSON.stringify(events))
      await delay(5)
    }
  }
  /** Runtime start and exit records written by interrupt-acp.ts. */
  const log = async () =>
    (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
  return { provider, events, until, log }
}

for (const mode of ['plan', 'full-access']) {
  test(`interrupt stops native background owner and restores ${mode} session without history replay`, async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-'))
    const { provider, events, until } = await connect(home, 5000)
    const prompt = (text: string) =>
      provider.send({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: text,
          delivery: 'auto',
          input: { type: 'message', content: [{ type: 'text', text }] },
        },
      })
    try {
      await provider.send({
        type: 'session.open',
        requestId: 'open',
        sessionId: 's',
        history: 'replay',
        config: {
          cwd: home,
          env: {},
          mcpServers: {
            test: { type: 'stdio', command: 'test-mcp', args: ['preserve'] },
          },
          settings: {},
          persist: true,
          model: 'large',
          thinkingOption: 'high',
          mode,
          systemPrompt: 'preserve system prompt',
        },
      })
      await until((event) => event.type === 'session.ready')
      await prompt('background')
      await until(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === 'WAITING',
      )
      await provider.send({
        type: 'session.interrupt',
        requestId: 'interrupt',
        sessionId: 's',
      })
      await until(
        (event) =>
          event.type === 'request.completed' && event.requestId === 'interrupt',
      )
      await until(
        (event) => event.type === 'session.turn' && event.state === 'canceled',
      )
      await prompt('next')
      await until(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      )
      await delay(750)
      const log = (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      assert.equal(
        await stat(join(home, 'old-result')).then(
          () => true,
          () => false,
        ),
        false,
      )
      assert.equal(
        log.some((row) => row.event === 'leaked-background'),
        false,
      )
      const starts = log.filter((row) => row.event === 'start')
      assert.equal(
        starts.length,
        2,
        'interrupt must replace the runtime that owns background tasks',
      )
      assert.ok(
        log.some((row) => row.event === 'exit' && row.pid === starts[0].pid),
      )
      assert.throws(() => process.kill(starts[0].pid, 0), { code: 'ESRCH' })
      const created = log.find((row) => row.event === 'new')
      const loaded = log.find((row) => row.event === 'load')
      assert.equal(loaded.nativeId, created.nativeId)
      const { sessionId: _sessionId, ...loadParams } = loaded.params
      assert.deepEqual(
        loadParams,
        created.params,
        'reload must preserve MCP and system prompt configuration',
      )
      const prompts = log.filter((row) => row.event === 'prompt')
      assert.equal(prompts.length, 2)
      assert.deepEqual(prompts[1].config, prompts[0].config)
      assert.deepEqual(prompts[1].config, {
        model: 'large',
        thought_level: 'high',
        mode: mode === 'full-access' ? 'accept-edits' : 'plan',
      })
      assert.equal(prompts[1].fullAccess, mode === 'full-access')
      if (mode === 'full-access')
        assert.deepEqual(
          log.find((row) => row.event === 'permission').outcome,
          { outcome: 'selected', optionId: 'allow' },
        )
      const text = events.flatMap((event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message'
          ? [event.item.text]
          : [],
      )
      assert.deepEqual(text, ['WAITING', 'NEW_REPLY'])
      assert.equal(
        events.some(
          (event) =>
            event.type === 'session.runtime_failed' ||
            event.type === 'request.failed' ||
            (event.type === 'session.turn' && event.state === 'failed'),
        ),
        false,
      )
      assert.ok(
        events.some(
          (event) =>
            event.type === 'timeline.item' &&
            event.item.type === 'tool_call' &&
            event.item.callId === 'call-background' &&
            event.item.status === 'canceled',
        ),
      )
    } finally {
      await provider.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

test('closing during interruption cannot start a replacement runtime after session.closed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-close-'))
  const { provider, events, until, log } = await connect(home)
  try {
    await provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await until((event) => event.type === 'session.ready')
    await provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'background',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'background' }],
        },
      },
    })
    await until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'WAITING',
    )
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await provider.send({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    await until((event) => event.type === 'session.closed')
    const closedLog = await log()
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt',
    )
    const finalLog = await log()
    assert.equal(finalLog.filter((row) => row.event === 'start').length, 1)
    assert.deepEqual(
      finalLog,
      closedLog,
      'a closed session must not launch another runtime',
    )
    for (const row of finalLog.filter((row) => row.event === 'start')) {
      assert.ok(
        finalLog.some((exit) => exit.event === 'exit' && exit.pid === row.pid),
      )
      assert.throws(() => process.kill(row.pid, 0), { code: 'ESRCH' })
    }
  } finally {
    let timer: NodeJS.Timeout | undefined
    try {
      await Promise.race([
        provider.close(),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(
                new Error('provider.close hung after concurrent interruption'),
              ),
            1500,
          )
        }),
      ])
    } finally {
      clearTimeout(timer)
      for (const row of (await log()).filter((row) => row.event === 'start')) {
        try {
          process.kill(row.pid, 'SIGTERM')
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
        }
      }
      await rm(home, { recursive: true, force: true })
    }
  }
})

test('interrupt without active background work uses native session/cancel fast path without relaunching', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-fast-'))
  const { provider, events, until } = await connect(home)
  try {
    await provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await until((event) => event.type === 'session.ready')
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt',
    )
    const log = (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
    assert.equal(log.filter((row) => row.event === 'start').length, 1)
    assert.ok(log.some((row) => row.event === 'cancel'))
    assert.equal(
      log.some((row) => row.event === 'load'),
      false,
    )
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
