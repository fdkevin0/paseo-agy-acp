import assert from 'node:assert/strict'
import { test } from 'node:test'
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
import { collectProviderEvents, writeBrainTranscript } from './helpers.ts'

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
  const untilLog = async (event: string) => {
    const deadline = Date.now() + timeoutMs
    while (!(await log()).some((row) => row.event === event)) {
      assert.ok(Date.now() < deadline, `Timed out waiting for ${event}`)
      await delay(10)
    }
  }
  return { provider, events, until, log, untilLog }
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

test('interrupt completes before slow loadSession, avoids relaunch loops without restart notice, and keeps can_stop_runtime accurate', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-slow-load-'))
  const { provider, events, until, log } = await connect(home, 4000, {
    AGY_LOAD_DELAY_MS: '2200',
  })
  const prompt = (clientMessageId: string, text: string) =>
    provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
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
      },
    })
    await until((event) => event.type === 'session.ready')
    await prompt('p-bg', 'background')
    await until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'WAITING',
    )

    const interruptStartedAt = Date.now()
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-1',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-1',
    )
    await until(
      (event) => event.type === 'session.turn' && event.state === 'canceled',
    )
    assert.ok(
      Date.now() - interruptStartedAt < 1500,
      'interrupt must finish Phase 1 shutdown well under Paseo 2s budget despite 2.2s loadSession',
    )

    // Admit follow-up prompt immediately while background loadSession is still running.
    await prompt('p-next', 'next')
    const started = await until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'started' &&
        events.some(
          (p) =>
            p.type === 'session.prompt_result' &&
            p.clientMessageId === 'p-next' &&
            p.result.type === 'turn' &&
            p.result.turnId === event.turnId,
        ),
    )
    assert.ok(started.type === 'session.turn')
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.turnId === started.turnId &&
        event.state === 'completed',
    )
    assert.ok(
      events.some(
        (event) =>
          event.type === 'timeline.item' &&
          event.item.type === 'assistant_message' &&
          event.item.text === 'NEW_REPLY',
      ),
    )

    // Transcript still contains pre-relaunch GENERIC(RUNNING) without a restart notice; idle runtime must be stoppable.
    await provider.send({
      type: 'session.can_stop_runtime',
      requestId: 'can-stop',
      sessionId: 's',
    })
    const canStopEvent = await until(
      (event) =>
        event.type === 'session.can_stop_runtime' &&
        event.requestId === 'can-stop',
    )
    assert.ok(canStopEvent.type === 'session.can_stop_runtime')
    assert.equal(
      canStopEvent.canStop,
      true,
      'pre-relaunch RUNNING row without restart notice must not block can_stop_runtime',
    )

    // A second interrupt must take the fast cancel path rather than entering a relaunch loop.
    const secondInterruptAt = Date.now()
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-2',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-2',
    )
    assert.ok(Date.now() - secondInterruptAt < 1000)

    const entries = await log()
    assert.equal(entries.filter((row) => row.event === 'start').length, 2)
    assert.equal(entries.filter((row) => row.event === 'load').length, 1)
    assert.equal(entries.filter((row) => row.event === 'cancel').length, 2)
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('interrupting a queued prompt while background reload is in progress cancels it without killing replacement runtime', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-queued-'))
  const { provider, events, until, log } = await connect(home, 3000, {
    AGY_LOAD_DELAY_MS: '800',
  })
  const prompt = (clientMessageId: string, text: string) =>
    provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
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
      },
    })
    await until((event) => event.type === 'session.ready')
    await prompt('p-bg', 'background')
    await until(
      (event) =>
        event.type === 'timeline.item' &&
        event.item.type === 'assistant_message' &&
        event.item.text === 'WAITING',
    )

    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-1',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-1',
    )
    await until(
      (event) => event.type === 'session.turn' && event.state === 'canceled',
    )

    await prompt('p-queued', 'should-not-run')
    const queuedTurn = await until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'started' &&
        events.some(
          (p) =>
            p.type === 'session.prompt_result' &&
            p.clientMessageId === 'p-queued' &&
            p.result.type === 'turn' &&
            p.result.turnId === event.turnId,
        ),
    )
    assert.ok(queuedTurn.type === 'session.turn')

    const cancelQueuedAt = Date.now()
    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-2',
      sessionId: 's',
    })
    await until(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'interrupt-2',
    )
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.turnId === queuedTurn.turnId &&
        event.state === 'canceled',
    )
    assert.ok(Date.now() - cancelQueuedAt < 500)

    await prompt('p-after', 'next')
    const afterTurn = await until(
      (event) =>
        event.type === 'session.turn' &&
        event.state === 'started' &&
        events.some(
          (p) =>
            p.type === 'session.prompt_result' &&
            p.clientMessageId === 'p-after' &&
            p.result.type === 'turn' &&
            p.result.turnId === event.turnId,
        ),
    )
    assert.ok(afterTurn.type === 'session.turn')
    await until(
      (event) =>
        event.type === 'session.turn' &&
        event.turnId === afterTurn.turnId &&
        event.state === 'completed',
    )

    const entries = await log()
    assert.equal(entries.filter((row) => row.event === 'start').length, 2)
    assert.deepEqual(
      entries.filter((row) => row.event === 'prompt').map((row) => row.text),
      ['background', 'next'],
    )
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('reopened persisted session fences pre-restart RUNNING work and orphaned unread inbox messages', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-reopen-'))
  const { provider, until, log } = await connect(home, 3000)
  try {
    await provider.send({
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
    const opened = await until(
      (event) => event.type === 'session.opened' && event.sessionId === 's1',
    )
    assert.ok(
      opened.type === 'session.opened' &&
        opened.persistence?.data &&
        typeof opened.persistence.data === 'object' &&
        !Array.isArray(opened.persistence.data),
    )
    await until(
      (event) => event.type === 'session.ready' && event.sessionId === 's1',
    )
    await provider.send({
      type: 'session.prompt',
      sessionId: 's1',
      prompt: {
        clientMessageId: 'p-bg',
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

    const nativeId = String(opened.persistence.data.sessionId)
    const messagesDir = join(
      home,
      '.gemini/antigravity-cli/brain',
      nativeId,
      '.system_generated/messages',
    )
    await mkdir(messagesDir, { recursive: true })
    await writeFile(
      join(messagesDir, 'dead-unread.json'),
      JSON.stringify({
        id: 'dead-unread',
        sender: '22222222-2222-4222-8222-222222222222',
        priority: 'MESSAGE_PRIORITY_HIGH',
        content: 'orphaned from previous runtime',
      }),
    )

    await provider.send({
      type: 'session.close',
      requestId: 'close-1',
      sessionId: 's1',
    })
    await until(
      (event) => event.type === 'session.closed' && event.sessionId === 's1',
    )

    await provider.send({
      type: 'session.open',
      requestId: 'open-2',
      sessionId: 's2',
      history: 'skip',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
      persistence: opened.persistence,
    })
    await until(
      (event) => event.type === 'session.ready' && event.sessionId === 's2',
    )

    await provider.send({
      type: 'session.can_stop_runtime',
      requestId: 'can-stop-reopen',
      sessionId: 's2',
    })
    const canStopEvent = await until(
      (event) =>
        event.type === 'session.can_stop_runtime' &&
        event.requestId === 'can-stop-reopen',
    )
    assert.ok(canStopEvent.type === 'session.can_stop_runtime')
    assert.equal(canStopEvent.canStop, true)

    await provider.send({
      type: 'session.interrupt',
      requestId: 'interrupt-reopen',
      sessionId: 's2',
    })
    await until(
      (event) =>
        event.type === 'request.completed' &&
        event.requestId === 'interrupt-reopen',
    )

    const entries = await log()
    assert.equal(entries.filter((row) => row.event === 'start').length, 2)
    assert.equal(entries.filter((row) => row.event === 'load').length, 1)
    assert.equal(entries.filter((row) => row.event === 'cancel').length, 1)
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

for (const operation of ['prompt', 'configure']) {
  test(`${operation} arriving during idle-runtime retirement waits for its replacement`, async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-retiring-ready-'))
    const { provider, until, log, untilLog } = await connect(home, 3000, {
      AGY_EXIT_DELAY_MS: '300',
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
        },
      })
      await until((event) => event.type === 'session.ready')
      // An idle runtime with unread background work still needs retirement.
      const messages = join(
        home,
        '.gemini/antigravity-cli/brain',
        '33333333-3333-4333-8333-333333333333',
        '.system_generated/messages',
      )
      await mkdir(messages, { recursive: true })
      await writeFile(
        join(messages, 'unread.json'),
        JSON.stringify({ id: 'unread', sender: 'task-old' }),
      )
      await provider.send({
        type: 'session.interrupt',
        requestId: 'interrupt',
        sessionId: 's',
      })
      await untilLog('stdin-ended')
      if (operation === 'configure') {
        await provider.send({
          type: 'session.configure',
          requestId: 'configure',
          sessionId: 's',
          changes: { model: 'large' },
        })
        await until(
          (event) =>
            event.type === 'request.completed' &&
            event.requestId === 'configure',
        )
      }
      await provider.send({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'next',
          delivery: 'auto',
          input: { type: 'message', content: [{ type: 'text', text: 'next' }] },
        },
      })
      await until(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      )
      const entries = await log()
      const loaded = entries.find((row) => row.event === 'loaded')
      const prompted = entries.find((row) => row.event === 'prompt')
      assert.ok(loaded)
      assert.equal(prompted.pid, loaded.pid)
      assert.equal(
        prompted.config.model,
        operation === 'configure' ? 'large' : 'small',
      )
    } finally {
      await provider.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

test('a prompt waits for an in-flight configuration transaction', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-config-prompt-'))
  const { provider, until, log, untilLog } = await connect(home, 3000, {
    AGY_CONFIG_DELAY_MS: '200',
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
      },
    })
    await until((event) => event.type === 'session.ready')
    await provider.send({
      type: 'session.configure',
      requestId: 'configure',
      sessionId: 's',
      changes: { model: 'large', thinkingOption: 'high' },
    })
    await untilLog('configure')
    await provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'next',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'next' }] },
      },
    })
    await until(
      (event) => event.type === 'session.turn' && event.state === 'completed',
    )
    const prompted = (await log()).find((row) => row.event === 'prompt')
    assert.equal(prompted.config.model, 'large')
    assert.equal(prompted.config.thought_level, 'high')
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('restoration fences history before load, not wakeups created by the new runtime', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-load-wakeup-'))
  const { provider, until } = await connect(home, 3000, {
    AGY_LOAD_WAKEUP: '1',
  })
  const nativeId = '33333333-3333-4333-8333-333333333333'
  try {
    await writeBrainTranscript(
      join(home, '.gemini/antigravity-cli/brain'),
      nativeId,
      [
        {
          step_index: 3,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          content: 'old turn',
        },
      ],
    )
    await provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      persistence: { version: 1, data: { sessionId: nativeId } },
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
      type: 'session.can_stop_runtime',
      requestId: 'can-stop',
      sessionId: 's',
    })
    const result = await until(
      (event) => event.type === 'session.can_stop_runtime',
    )
    assert.ok(result.type === 'session.can_stop_runtime')
    assert.equal(
      result.canStop,
      false,
      'a newly woken model must keep its runtime alive',
    )
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

for (const operation of ['prompt', 'configure']) {
  test(`a repeated interrupt cancels a ${operation} queued during retirement`, async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-retiring-cancel-'))
    const { provider, until, untilLog, log } = await connect(home, 3000, {
      AGY_EXIT_DELAY_MS: '300',
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
        },
      })
      await until((event) => event.type === 'session.ready')
      const messages = join(
        home,
        '.gemini/antigravity-cli/brain',
        '33333333-3333-4333-8333-333333333333',
        '.system_generated/messages',
      )
      await mkdir(messages, { recursive: true })
      await writeFile(
        join(messages, 'old.json'),
        JSON.stringify({ id: 'old', sender: 'task-old' }),
      )
      await provider.send({
        type: 'session.interrupt',
        requestId: 'interrupt-1',
        sessionId: 's',
      })
      await untilLog('stdin-ended')
      if (operation === 'configure') {
        await provider.send({
          type: 'session.configure',
          requestId: 'queued-config',
          sessionId: 's',
          changes: { model: 'large' },
        })
      } else {
        await provider.send({
          type: 'session.prompt',
          sessionId: 's',
          prompt: {
            clientMessageId: 'queued',
            delivery: 'auto',
            input: {
              type: 'message',
              content: [{ type: 'text', text: 'must-not-run' }],
            },
          },
        })
        await until(
          (event) => event.type === 'session.turn' && event.state === 'started',
        )
      }
      await provider.send({
        type: 'session.interrupt',
        requestId: 'interrupt-2',
        sessionId: 's',
      })
      await until((event) =>
        operation === 'configure'
          ? event.type === 'request.failed' &&
            event.requestId === 'queued-config'
          : event.type === 'session.turn' && event.state === 'canceled',
      )
      await until(
        (event) =>
          event.type === 'request.completed' &&
          event.requestId === 'interrupt-2',
      )
      await provider.send({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'after',
          delivery: 'auto',
          input: { type: 'message', content: [{ type: 'text', text: 'next' }] },
        },
      })
      await until(
        (event) => event.type === 'session.turn' && event.state === 'completed',
      )
      const entries = await log()
      assert.deepEqual(
        entries.filter((row) => row.event === 'prompt').map((row) => row.text),
        ['next'],
      )
      assert.equal(
        entries.some((row) => row.event === 'configure'),
        false,
      )
      assert.equal(entries.filter((row) => row.event === 'start').length, 2)
    } finally {
      await provider.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

test('initial session configuration does not admit prompts or external configuration before ready', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-opening-config-'))
  const { provider, until, untilLog, log } = await connect(home, 3000, {
    AGY_CONFIG_DELAY_MS: '200',
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
        model: 'large',
      },
    })
    await until((event) => event.type === 'session.opened')
    await untilLog('configure')
    await provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'too-early',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'must-not-run' }],
        },
      },
    })
    const result = await until(
      (event) => event.type === 'session.prompt_result',
    )
    assert.ok(result.type === 'session.prompt_result')
    assert.equal(result.result.type, 'failed')
    await provider.send({
      type: 'session.configure',
      requestId: 'too-early-config',
      sessionId: 's',
      changes: { model: 'small' },
    })
    await until(
      (event) =>
        event.type === 'request.failed' &&
        event.requestId === 'too-early-config',
    )
    await until((event) => event.type === 'session.ready')
    assert.equal(
      (await log()).some((row) => row.event === 'prompt'),
      false,
    )
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
