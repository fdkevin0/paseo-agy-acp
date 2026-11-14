import assert from 'node:assert/strict'
import { test } from 'node:test'
import { existsSync } from 'node:fs'
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import {
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { createProvider } from '../server/provider.ts'
import { collectProviderEvents, waitUntil } from './helpers.ts'

/** Connect a provider to interrupt-acp.ts and record its events. */
async function connect(
  home: string,
  timeoutMs = 2000,
  extraEnv: Record<string, string> = {},
) {
  const provider = await createProvider().connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '', ...extraEnv },
    },
  })
  const { events, wait } = collectProviderEvents(provider)
  const until = (predicate: (event: ProviderEvent) => boolean) =>
    wait(predicate, timeoutMs)
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
  const { provider, until, log } = await connect(home)
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
  const { provider, until } = await connect(home)
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

test('interrupt returns while reload is gated, serializes queued configure and prompt, fences stale artifacts, and uses in-place cancel afterwards', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-gate-'))
  const gateDir = join(home, 'gate')
  await mkdir(gateDir, { recursive: true })
  const { provider, events, until, log } = await connect(home, 5000, {
    AGY_RELOAD_GATE: gateDir,
  })
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
      history: 'skip',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
        model: 'small',
        thinkingOption: 'low',
        mode: 'plan',
      },
    })
    await until((event) => event.type === 'session.ready')
    await prompt('stale-everything')
    await until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'WAITING_STALE',
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
    await waitUntil(
      () => existsSync(join(gateDir, 'entered')),
      3000,
      () => 'reload gate did not enter',
    )
    assert.equal(await readFile(join(gateDir, 'entered'), 'utf8'), 'load')

    await provider.send({
      type: 'session.configure',
      requestId: 'cfg',
      sessionId: 's',
      changes: { mode: 'full-access', model: 'large', thinkingOption: 'high' },
    })
    await prompt('next')
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'started' &&
        events.filter((e) => e.type === 'session.turn' && e.state === 'started')
          .length === 2,
    )
    assert.equal(
      events.some(
        (event) =>
          event.type === 'request.completed' && event.requestId === 'cfg',
      ),
      false,
    )

    await writeFile(join(gateDir, 'release'), '1')
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'cfg',
    )
    await until(
      (event) => event.type === 'session.turn' && event.state === 'completed',
    )

    const entries = await log()
    const prompts = entries.filter((row) => row.event === 'prompt')
    assert.equal(prompts.length, 2)
    assert.equal(prompts[1].text, 'next')
    assert.equal(prompts[1].fullAccess, true)
    assert.deepEqual(prompts[1].config, {
      model: 'large',
      thought_level: 'high',
      mode: 'accept-edits',
    })

    await provider.send({
      type: 'session.can_stop_runtime',
      requestId: 'can-stop',
      sessionId: 's',
    })
    assert.deepEqual(
      await until(
        (event) =>
          event.type === 'session.can_stop_runtime' &&
          event.requestId === 'can-stop',
      ),
      {
        type: 'session.can_stop_runtime',
        requestId: 'can-stop',
        sessionId: 's',
        canStop: true,
      },
    )

    const preHangStarts = entries.filter((row) => row.event === 'start').length
    await prompt('hang')
    await until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'HANGING',
    )
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-hang',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' &&
        event.requestId === 'interrupt-hang',
    )
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'canceled' &&
        events.filter(
          (e) => e.type === 'session.turn' && e.state === 'canceled',
        ).length === 2,
    )
    const afterHangEntries = await log()
    assert.equal(
      afterHangEntries.filter((row) => row.event === 'start').length,
      preHangStarts,
    )
    assert.equal(
      afterHangEntries.some((row) => row.event === 'cancel'),
      true,
    )
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('interrupt prefers session/resume, cancels queued prompt before reload finishes, and reopen fences stale Brain state', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-resume-'))
  const gateDir = join(home, 'gate')
  await mkdir(gateDir, { recursive: true })
  const first = await connect(home, 5000, {
    AGY_RESUME_CAP: '1',
    AGY_RELOAD_GATE: gateDir,
  })
  try {
    await first.provider.send({
      type: 'session.open',
      requestId: 'open-1',
      sessionId: 's1',
      history: 'skip',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await first.until((event) => event.type === 'session.ready')
    await first.provider.send({
      type: 'session.prompt',
      sessionId: 's1',
      prompt: {
        clientMessageId: 'stale-everything',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'stale-everything' }],
        },
      },
    })
    await first.until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'WAITING_STALE',
    )
    await first.provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-1',
      sessionId: 's1',
    })
    await first.until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-1',
    )
    await first.until(
      (event) => event.type === 'session.turn' && event.state === 'canceled',
    )
    await waitUntil(
      () => existsSync(join(gateDir, 'entered')),
      3000,
      () => 'resume gate did not enter',
    )
    assert.equal(await readFile(join(gateDir, 'entered'), 'utf8'), 'resume')

    await first.provider.send({
      type: 'session.prompt',
      sessionId: 's1',
      prompt: {
        clientMessageId: 'aborted-queued',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'aborted-queued' }],
        },
      },
    })
    await first.until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'started' &&
        first.events.filter(
          (e) => e.type === 'session.turn' && e.state === 'started',
        ).length === 2,
    )
    await first.provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-2',
      sessionId: 's1',
    })
    await first.until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-2',
    )
    await first.until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'canceled' &&
        first.events.filter(
          (e) => e.type === 'session.turn' && e.state === 'canceled',
        ).length === 2,
    )

    await writeFile(join(gateDir, 'release'), '1')
    await first.provider.send({
      type: 'session.prompt',
      sessionId: 's1',
      prompt: {
        clientMessageId: 'after-resume',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'after-resume' }],
        },
      },
    })
    await first.until(
      (event) => event.type === 'session.turn' && event.state === 'completed',
    )

    const entries = await first.log()
    assert.equal(
      entries.some((row) => row.event === 'resume'),
      true,
    )
    assert.equal(
      entries.some((row) => row.event === 'load'),
      false,
    )
    assert.equal(
      entries.some(
        (row) => row.event === 'prompt' && row.text === 'aborted-queued',
      ),
      false,
    )
    assert.equal(
      entries.some(
        (row) => row.event === 'prompt' && row.text === 'after-resume',
      ),
      true,
    )

    await first.provider.close()
    const second = await connect(home, 5000, { AGY_RESUME_CAP: '1' })
    try {
      await second.provider.send({
        type: 'session.open',
        requestId: 'open-2',
        sessionId: 's2',
        history: 'skip',
        persistence: {
          version: 1,
          data: { sessionId: '33333333-3333-4333-8333-333333333333' },
        },
        config: {
          cwd: home,
          env: {},
          mcpServers: {},
          settings: {},
          persist: true,
        },
      })
      await second.until((event) => event.type === 'session.ready')
      await second.provider.send({
        type: 'session.can_stop_runtime',
        requestId: 'can-stop-reopen',
        sessionId: 's2',
      })
      assert.deepEqual(
        await second.until(
          (event) =>
            event.type === 'session.can_stop_runtime' &&
            event.requestId === 'can-stop-reopen',
        ),
        {
          type: 'session.can_stop_runtime',
          requestId: 'can-stop-reopen',
          sessionId: 's2',
          canStop: true,
        },
      )
      await second.provider.send({
        type: 'session.prompt',
        sessionId: 's2',
        prompt: {
          clientMessageId: 'reopened-next',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'reopened-next' }],
          },
        },
      })
      await second.until(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      )
    } finally {
      await second.provider.close()
    }
  } finally {
    await first.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
