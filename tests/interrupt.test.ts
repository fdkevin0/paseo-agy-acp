import assert from 'node:assert/strict'
import { createServer, type Socket } from 'node:net'
import { test, type TestContext } from 'node:test'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderConfigChanges,
  type ProviderEvent,
  type ProviderInput,
} from '@getpaseo/plugin/server/provider'
import { readSubagents } from '../server/brain.ts'
import { TranscriptReader } from '../server/brain-io.ts'
import { createProvider } from '../server/provider.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  collectProviderEvents,
  createEventCollector,
  isolatedEnv,
} from './helpers.ts'

async function createReloadGate(dir: string) {
  const socketPath = join(dir, 'reload.sock')
  let resolveEntered!: (kind: 'load' | 'resume') => void
  const entered = new Promise<'load' | 'resume'>((resolve) => {
    resolveEntered = resolve
  })
  let released = false
  let conn: Socket | undefined
  const sockets = new Set<Socket>()
  const server = createServer((socket) => {
    conn = socket
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
    let buf = ''
    socket.on('data', (chunk) => {
      buf += chunk.toString('utf8')
      const nl = buf.indexOf('\n')
      if (nl < 0) return
      resolveEntered(buf.slice(0, nl).trim() as 'load' | 'resume')
      if (released) socket.end('go\n')
    })
  })
  await new Promise<void>((resolve) => server.listen(socketPath, resolve))
  return {
    socketPath,
    entered,
    release() {
      released = true
      if (conn && !conn.destroyed) conn.end('go\n')
    },
    async close() {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    },
  }
}

/** Connect a provider to interrupt-acp.ts and record its events. */
async function connect(
  home: string,
  timeoutMs = 5000,
  extraEnv: Record<string, string> = {},
) {
  const env = isolatedEnv(home, extraEnv)
  const provider = await createProvider(new AgyUsageSource({ env })).connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
      env,
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

function assistantMessages(events: readonly ProviderEvent[]): string[] {
  return events
    .filter(
      (event): event is Extract<ProviderEvent, { type: 'timeline.item' }> =>
        event.type === 'timeline.item',
    )
    .map((event) => event.item)
    .filter(
      (
        item,
      ): item is Extract<
        Extract<ProviderEvent, { type: 'timeline.item' }>['item'],
        { type: 'assistant_message' }
      > => item.type === 'assistant_message',
    )
    .map((item) => item.text)
}

async function terminateLoggedProcesses(
  log: () => Promise<Array<{ event?: string; pid?: number }>>,
) {
  for (const row of (await log()).filter((row) => row.event === 'start')) {
    if (typeof row.pid !== 'number') continue
    try {
      process.kill(row.pid, 'SIGTERM')
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
    }
  }
}

async function assertInterruptStopsBackground(opts: {
  mode: 'plan' | 'full-access'
  expectedNativeMode: 'plan' | 'accept-edits'
  expectedFullAccess: boolean
}) {
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
        mode: opts.mode,
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
      mode: opts.expectedNativeMode,
    })
    assert.equal(prompts[1].fullAccess, opts.expectedFullAccess)
    assert.deepEqual(assistantMessages(events), ['WAITING', 'NEW_REPLY'])
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
    return log
  } finally {
    await provider.close()
    await rm(home, { recursive: true, force: true })
  }
}

test('interrupt stops native background owner and restores plan session without history replay', async () => {
  await assertInterruptStopsBackground({
    mode: 'plan',
    expectedNativeMode: 'plan',
    expectedFullAccess: false,
  })
})

test('interrupt stops native background owner and restores full-access session without history replay', async () => {
  const log = await assertInterruptStopsBackground({
    mode: 'full-access',
    expectedNativeMode: 'accept-edits',
    expectedFullAccess: true,
  })
  assert.deepEqual(log.find((row) => row.event === 'permission').outcome, {
    outcome: 'selected',
    optionId: 'allow',
  })
})

test(
  'closing during interruption cannot start a replacement runtime after session.closed',
  { timeout: 10_000 },
  async () => {
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
      const starts = finalLog.filter((row) => row.event === 'start')
      assert.equal(starts.length, 1)
      assert.deepEqual(
        finalLog,
        closedLog,
        'a closed session must not launch another runtime',
      )
      assertPidExited(finalLog, starts[0].pid)
    } finally {
      try {
        await provider.close()
      } finally {
        await terminateLoggedProcesses(log)
        await rm(home, { recursive: true, force: true })
      }
    }
  },
)

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
    await until(
      (event) =>
        event.type === 'session.commands' &&
        event.commands.some((command) => command.name === 'cancel-ack'),
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

function openReq(
  sessionId: string,
  cwd: string,
  opts?: {
    requestId?: string
    history?: 'skip' | 'replay'
    nativeId?: string
    mode?: 'default' | 'accept-edits' | 'plan' | 'full-access'
    model?: string
    thinkingOption?: string
  },
): Extract<ProviderInput, { type: 'session.open' }> {
  return {
    type: 'session.open',
    requestId: opts?.requestId ?? `open-${sessionId}`,
    sessionId,
    history: opts?.history ?? 'skip',
    ...(opts?.nativeId
      ? {
          persistence: {
            version: 1 as const,
            data: { sessionId: opts.nativeId },
          },
        }
      : {}),
    config: {
      cwd,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
      ...(opts?.model ? { model: opts.model } : {}),
      ...(opts?.thinkingOption ? { thinkingOption: opts.thinkingOption } : {}),
      ...(opts?.mode ? { mode: opts.mode } : {}),
    },
  }
}

function promptReq(
  sessionId: string,
  text: string,
  clientMessageId = text,
): Extract<ProviderInput, { type: 'session.prompt' }> {
  return {
    type: 'session.prompt',
    sessionId,
    prompt: {
      clientMessageId,
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text }] },
    },
  }
}

const interruptReq = (sessionId: string, requestId: string) => ({
  type: 'session.interrupt' as const,
  requestId,
  sessionId,
})

const configureReq = (
  sessionId: string,
  requestId: string,
  changes: ProviderConfigChanges,
) => ({
  type: 'session.configure' as const,
  requestId,
  sessionId,
  changes,
})

const closeReq = (sessionId: string, requestId: string) => ({
  type: 'session.close' as const,
  requestId,
  sessionId,
})

const assistantText = (text: string) => (event: ProviderEvent) =>
  event.type === 'timeline.item' &&
  event.item.type === 'assistant_message' &&
  event.item.text === text

const turnEvent =
  (
    sessionId: string,
    state: 'started' | 'completed' | 'canceled' | 'failed',
    nth = 1,
    events?: readonly ProviderEvent[],
  ) =>
  (event: ProviderEvent) =>
    event.type === 'session.turn' &&
    event.sessionId === sessionId &&
    event.state === state &&
    (!events ||
      events.filter(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === sessionId &&
          e.state === state,
      ).length === nth)

async function assertCanStop(
  conn: {
    provider: { send: (input: ProviderInput) => Promise<void> }
    until: (
      predicate: (event: ProviderEvent) => boolean,
    ) => Promise<ProviderEvent>
  },
  sessionId: string,
  requestId: string,
  canStop: boolean,
) {
  await conn.provider.send({
    type: 'session.can_stop_runtime',
    requestId,
    sessionId,
  })
  assert.deepEqual(
    await conn.until(
      (event) =>
        event.type === 'session.can_stop_runtime' &&
        event.requestId === requestId,
    ),
    { type: 'session.can_stop_runtime', requestId, sessionId, canStop },
  )
}

function assertPidExited(
  entries: ReadonlyArray<{ event?: string; pid?: number }>,
  pid: number,
) {
  assert.ok(entries.some((row) => row.event === 'exit' && row.pid === pid))
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
}

function assertNoFailures(events: readonly ProviderEvent[]) {
  assert.equal(
    events.some(
      (event) =>
        event.type === 'session.runtime_failed' ||
        event.type === 'request.failed' ||
        (event.type === 'session.turn' && event.state === 'failed'),
    ),
    false,
  )
}

async function tempHome(prefix: string) {
  return mkdtemp(join(tmpdir(), prefix))
}

test('interrupt returns while reload is gated, serializes queued configure and prompt, fences stale artifacts, and uses in-place cancel afterwards', async (t) => {
  const home = await tempHome('agy-interrupt-gate-')
  const gate = await createReloadGate(home)
  const conn = await connect(home, 5000, { AGY_RELOAD_GATE: gate.socketPath })
  t.after(async () => {
    gate.release()
    await gate.close()
    await conn.provider.close()
    await rm(home, { recursive: true, force: true })
  })
  await conn.provider.send(
    openReq('s', home, {
      requestId: 'open',
      model: 'small',
      thinkingOption: 'low',
      mode: 'plan',
    }),
  )
  await conn.until((event) => event.type === 'session.ready')
  await conn.provider.send(promptReq('s', 'stale-everything'))
  await conn.until(assistantText('WAITING_STALE'))
  await conn.provider.send(interruptReq('s', 'interrupt'))
  await conn.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'interrupt',
  )
  await conn.until(turnEvent('s', 'canceled'))
  assert.equal(await gate.entered, 'load')

  await conn.provider.send(
    configureReq('s', 'cfg', {
      mode: 'full-access',
      model: 'large',
      thinkingOption: 'high',
    }),
  )
  await conn.provider.send(promptReq('s', 'next'))
  await conn.until(turnEvent('s', 'started', 2, conn.events))
  assert.equal(
    conn.events.some(
      (event) =>
        event.type === 'request.completed' && event.requestId === 'cfg',
    ),
    false,
  )

  gate.release()
  await conn.until(
    (event) => event.type === 'request.completed' && event.requestId === 'cfg',
  )
  await conn.until(turnEvent('s', 'completed'))

  const entries = await conn.log()
  const prompts = entries.filter((row) => row.event === 'prompt')
  assert.equal(prompts.length, 2)
  assert.equal(prompts[1].text, 'next')
  assert.equal(prompts[1].fullAccess, true)
  assert.deepEqual(prompts[1].config, {
    model: 'large',
    thought_level: 'high',
    mode: 'accept-edits',
  })

  await assertCanStop(conn, 's', 'can-stop', true)

  const preHangStarts = entries.filter((row) => row.event === 'start').length
  await conn.provider.send(promptReq('s', 'hang'))
  await conn.until(assistantText('HANGING'))
  await conn.provider.send(interruptReq('s', 'interrupt-hang'))
  await conn.until(
    (event) =>
      event.type === 'request.completed' &&
      event.requestId === 'interrupt-hang',
  )
  await conn.until(turnEvent('s', 'canceled', 2, conn.events))
  const afterHangEntries = await conn.log()
  assert.equal(
    afterHangEntries.filter((row) => row.event === 'start').length,
    preHangStarts,
  )
  assert.equal(
    afterHangEntries.some((row) => row.event === 'cancel'),
    true,
  )
})

test('interrupt prefers session/resume, cancels queued prompt before reload finishes, and reopen fences stale Brain state', async (t) => {
  const home = await tempHome('agy-interrupt-resume-')
  const gate = await createReloadGate(home)
  const first = await connect(home, 5000, {
    AGY_RESUME_CAP: '1',
    AGY_RELOAD_GATE: gate.socketPath,
  })
  let second: Awaited<ReturnType<typeof connect>> | undefined
  let third: Awaited<ReturnType<typeof connect>> | undefined
  t.after(async () => {
    gate.release()
    await gate.close()
    await first.provider.close()
    await second?.provider.close()
    await third?.provider.close()
    await rm(home, { recursive: true, force: true })
  })
  await first.provider.send(openReq('s1', home, { requestId: 'open-1' }))
  await first.until((event) => event.type === 'session.ready')
  await first.provider.send(promptReq('s1', 'stale-everything'))
  await first.until(assistantText('WAITING_STALE'))
  await first.provider.send(interruptReq('s1', 'interrupt-1'))
  await first.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'interrupt-1',
  )
  await first.until(turnEvent('s1', 'canceled'))
  assert.equal(await gate.entered, 'resume')

  await first.provider.send(promptReq('s1', 'aborted-queued'))
  await first.until(turnEvent('s1', 'started', 2, first.events))
  await first.provider.send(interruptReq('s1', 'interrupt-2'))
  await first.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'interrupt-2',
  )
  await first.until(turnEvent('s1', 'canceled', 2, first.events))

  gate.release()
  await first.provider.send(promptReq('s1', 'after-resume'))
  await first.until(turnEvent('s1', 'completed'))

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
  second = await connect(home, 5000, { AGY_RESUME_CAP: '1' })
  await second.provider.send(
    openReq('s2', home, {
      requestId: 'open-2',
      nativeId: '33333333-3333-4333-8333-333333333333',
    }),
  )
  await second.until((event) => event.type === 'session.ready')
  await assertCanStop(second, 's2', 'can-stop-reopen', true)
  await second.provider.send(promptReq('s2', 'reopened-next'))
  await second.until(turnEvent('s2', 'completed'))
  await second.provider.close()

  third = await connect(home, 5000)
  await third.provider.send(
    openReq('s3', home, {
      requestId: 'open-3',
      history: 'replay',
      nativeId: '33333333-3333-4333-8333-333333333333',
    }),
  )
  await third.until(
    (event) => event.type === 'session.ready' && event.sessionId === 's3',
  )
  assert.equal(
    third.events.some(
      turnEvent('s3:child:44444444-4444-4444-8444-444444444444', 'canceled'),
    ),
    true,
  )
  await assertCanStop(third, 's3', 'can-stop-replay', true)

  const beforeHangLogs = await third.log()
  const loadCountBeforeHang = beforeHangLogs.filter(
    (row) => row.event === 'load' || row.event === 'resume',
  ).length
  const cancelCountBeforeHang = beforeHangLogs.filter(
    (row) => row.event === 'cancel',
  ).length
  await third.provider.send(promptReq('s3', 'hang'))
  await third.until(assistantText('HANGING'))
  await third.provider.send(interruptReq('s3', 'interrupt-hang'))
  await third.until(
    (event) =>
      event.type === 'request.completed' &&
      event.requestId === 'interrupt-hang',
  )
  await third.until(turnEvent('s3', 'canceled'))
  const afterHangLogs = await third.log()
  assert.equal(
    afterHangLogs.filter(
      (row) => row.event === 'load' || row.event === 'resume',
    ).length,
    loadCountBeforeHang,
  )
  assert.equal(
    afterHangLogs.filter((row) => row.event === 'cancel').length,
    cancelCountBeforeHang + 1,
  )
})

test('ignore-cancel fast-path timeout falls back to retire and reload, and non-reloadable ignore-cancel fails cleanly', async (t) => {
  const homeA = await tempHome('agy-ignore-cancel-reload-')
  const homeB = await tempHome('agy-ignore-cancel-no-reload-')
  const reloadable = await connect(homeA, 5000)
  const nonReloadable = await connect(homeB, 5000, { AGY_NO_RELOAD: '1' })
  t.after(async () => {
    await reloadable.provider.close()
    await nonReloadable.provider.close()
    await rm(homeA, { recursive: true, force: true })
    await rm(homeB, { recursive: true, force: true })
  })

  await reloadable.provider.send(
    openReq('s-reload', homeA, { requestId: 'open-reloadable' }),
  )
  await reloadable.until((event) => event.type === 'session.ready')
  await reloadable.provider.send(
    promptReq('s-reload', 'ignore-cancel', 'ignore-1'),
  )
  await reloadable.until(assistantText('IGNORE_CANCEL'))
  await reloadable.provider.send(interruptReq('s-reload', 'int-ignore-1'))
  await reloadable.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'int-ignore-1',
  )
  await reloadable.until(turnEvent('s-reload', 'canceled'))
  await reloadable.provider.send(promptReq('s-reload', 'after-ignore-cancel'))
  await reloadable.until(turnEvent('s-reload', 'completed'))
  await assertCanStop(reloadable, 's-reload', 'can-stop-reloadable', true)
  assert.equal(
    (await reloadable.log()).some((row) => row.event === 'load'),
    true,
  )

  await nonReloadable.provider.send(
    openReq('s-no-reload', homeB, { requestId: 'open-no-reload' }),
  )
  await nonReloadable.until((event) => event.type === 'session.ready')
  await nonReloadable.provider.send(
    promptReq('s-no-reload', 'ignore-cancel', 'ignore-2'),
  )
  await nonReloadable.until(assistantText('IGNORE_CANCEL'))
  await nonReloadable.provider.send(interruptReq('s-no-reload', 'int-ignore-2'))
  await nonReloadable.until(
    (event) =>
      event.type === 'request.failed' && event.requestId === 'int-ignore-2',
  )
  await nonReloadable.until(
    (event) =>
      event.type === 'session.runtime_failed' &&
      event.sessionId === 's-no-reload',
  )
  await nonReloadable.until(turnEvent('s-no-reload', 'failed'))
  await assertCanStop(nonReloadable, 's-no-reload', 'can-stop-no-reload', true)
})

test('reload failure fails queued configure and prompt with root-cause diagnostic once', async (t) => {
  const home = await tempHome('agy-reload-fail-')
  const gate = await createReloadGate(home)
  const conn = await connect(home, 5000, {
    AGY_RELOAD_GATE: gate.socketPath,
    AGY_FAIL_RELOAD: '1',
  })
  t.after(async () => {
    gate.release()
    await gate.close()
    await conn.provider.close()
    await rm(home, { recursive: true, force: true })
  })
  await conn.provider.send(openReq('s-fail', home, { requestId: 'open-fail' }))
  await conn.until((event) => event.type === 'session.ready')
  await conn.provider.send(
    promptReq('s-fail', 'stale-everything', 'stale-fail'),
  )
  await conn.until(assistantText('WAITING_STALE'))
  await conn.provider.send(interruptReq('s-fail', 'int-fail-1'))
  await conn.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'int-fail-1',
  )
  await conn.until(turnEvent('s-fail', 'canceled'))
  assert.equal(await gate.entered, 'load')

  await conn.provider.send(
    configureReq('s-fail', 'cfg-queued', { model: 'large' }),
  )
  await conn.provider.send(promptReq('s-fail', 'prompt-queued'))
  await conn.until(turnEvent('s-fail', 'started', 2, conn.events))

  gate.release()
  const runtimeFailed = await conn.until(
    (event) => event.type === 'session.runtime_failed',
  )
  const cfgFailed = await conn.until(
    (event) =>
      event.type === 'request.failed' && event.requestId === 'cfg-queued',
  )
  const turnFailed = await conn.until(turnEvent('s-fail', 'failed'))
  assert.equal(
    conn.events.filter((event) => event.type === 'session.runtime_failed')
      .length,
    1,
  )
  assert.ok(runtimeFailed.type === 'session.runtime_failed')
  assert.match(runtimeFailed.error.message, /Simulated reload failure/)
  assert.ok(cfgFailed.type === 'request.failed')
  assert.match(
    cfgFailed.error.message,
    /ACP runtime has exited; reopen the session.*Simulated reload failure/,
  )
  assert.ok(turnFailed.type === 'session.turn' && turnFailed.state === 'failed')
  assert.ok(turnFailed.error)
  assert.match(turnFailed.error.message, /Simulated reload failure/)
})

test('retireAndRecover after native plan switch does not resurrect full-access skipPermissions', async (t) => {
  const home = await tempHome('agy-native-plan-')
  const conn = await connect(home, 5000)
  t.after(async () => {
    await conn.provider.close()
    await rm(home, { recursive: true, force: true })
  })
  await conn.provider.send(
    openReq('s-plan', home, { requestId: 'open-plan', mode: 'full-access' }),
  )
  await conn.until((event) => event.type === 'session.ready')
  await conn.provider.send(promptReq('s-plan', 'enter-plan-bg'))
  await conn.until(assistantText('ENTERED_PLAN_BG'))
  await conn.provider.send(interruptReq('s-plan', 'int-plan'))
  await conn.until(
    (event) =>
      event.type === 'request.completed' && event.requestId === 'int-plan',
  )
  await conn.until(turnEvent('s-plan', 'canceled'))
  await conn.provider.send(promptReq('s-plan', 'after-plan-switch'))
  await conn.until(turnEvent('s-plan', 'completed'))

  const entries = await conn.log()
  const starts = entries.filter((row) => row.event === 'start')
  assert.equal(starts.length, 2)
  assert.equal(starts[0].fullAccess, true)
  assert.equal(starts[1].fullAccess, false)
  const afterPrompt = entries.find(
    (row) => row.event === 'prompt' && row.text === 'after-plan-switch',
  )
  assert.ok(afterPrompt)
  assert.equal(afterPrompt.fullAccess, false)
  assert.equal(afterPrompt.config.mode, 'plan')
})

function createBooleanGate<TArgs extends unknown[], TReturn>(
  fallback: (...args: TArgs) => Promise<TReturn>,
  gatedResult: () => Promise<TReturn>,
) {
  let armed = false
  let resolveEntered!: () => void
  const entered = new Promise<void>((resolve) => {
    resolveEntered = resolve
  })
  let releaseGate!: () => void
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve
  })
  return {
    arm() {
      armed = true
    },
    entered,
    release() {
      releaseGate()
    },
    fn: async (...args: TArgs): Promise<TReturn> => {
      if (!armed) return fallback(...args)
      armed = false
      resolveEntered()
      await gate
      return gatedResult()
    },
  }
}

class GatedSettleReader extends TranscriptReader {
  private readonly onRead: (
    rows: Awaited<ReturnType<TranscriptReader['read']>>['rows'],
  ) => Promise<void>

  constructor(
    onRead: (
      rows: Awaited<ReturnType<TranscriptReader['read']>>['rows'],
    ) => Promise<void>,
  ) {
    super()
    this.onRead = onRead
  }

  override async read(root: string, nativeId: string) {
    const result = await super.read(root, nativeId)
    await this.onRead(result.rows)
    return result
  }
}

function createSettleReadGate() {
  let armed = false
  let resolveEntered!: () => void
  const entered = new Promise<void>((resolve) => {
    resolveEntered = resolve
  })
  let releaseGate!: () => void
  const gate = new Promise<void>((resolve) => {
    releaseGate = resolve
  })
  return {
    arm() {
      armed = true
    },
    entered,
    release() {
      releaseGate()
    },
    createTranscriptReader(): TranscriptReader {
      return new GatedSettleReader(async (rows) => {
        if (armed && rows.some((row) => row.content === 'WAITING_SETTLE')) {
          armed = false
          resolveEntered()
          await gate
        }
      })
    },
  }
}

async function createSessionManagerHarness(
  t: TestContext,
  prefix: string,
  opts?: {
    extraEnv?: Record<string, string>
    managerOptions?: Partial<ConstructorParameters<typeof SessionManager>[0]>
  },
) {
  const home = await mkdtemp(join(tmpdir(), prefix))
  const env = isolatedEnv(home, opts?.extraEnv)
  const collector = createEventCollector()
  const manager = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
      env,
    },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    ...opts?.managerOptions,
  })
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  })
  const log = async () =>
    (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line))
  return { home, collector, manager, log }
}

test(
  'concurrent idle interrupt and full-access configure serialize cleanly across fast-path cancel and background-probe relaunch',
  { timeout: 15_000 },
  async (t) => {
    const fast = await createSessionManagerHarness(t, 'agy-race-fast-')
    await fast.manager.handle(
      openReq('s-fast', fast.home, { requestId: 'open-fast', mode: 'plan' }),
    )
    await Promise.all([
      fast.manager.handle(interruptReq('s-fast', 'int-fast-race')),
      fast.manager.handle(
        configureReq('s-fast', 'cfg-fast-race', { mode: 'full-access' }),
      ),
    ])
    await fast.manager.handle(promptReq('s-fast', 'after-fast-race'))

    const fastLog = await fast.log()
    const fastStarts = fastLog.filter((row) => row.event === 'start')
    assert.equal(fastStarts.length, 2)
    assert.equal(fastStarts[0].fullAccess, false)
    assert.equal(fastStarts[1].fullAccess, true)
    assertPidExited(fastLog, fastStarts[0].pid)
    const afterFastPrompt = fastLog.find(
      (row) => row.event === 'prompt' && row.text === 'after-fast-race',
    )
    assert.ok(afterFastPrompt)
    assert.equal(afterFastPrompt.fullAccess, true)
    assertNoFailures(fast.collector.events)

    const probeGate = createBooleanGate(
      async () => false,
      async () => true,
    )
    t.after(() => probeGate.release())
    const probe = await createSessionManagerHarness(t, 'agy-race-probe-', {
      managerOptions: { hasActiveBackgroundWork: probeGate.fn },
    })
    await probe.manager.handle(
      openReq('s-probe', probe.home, { requestId: 'open-probe', mode: 'plan' }),
    )
    probeGate.arm()
    const interrupting = probe.manager.handle(
      interruptReq('s-probe', 'int-probe-race'),
    )
    await probeGate.entered
    const configuring = probe.manager.handle(
      configureReq('s-probe', 'cfg-probe-race', { mode: 'full-access' }),
    )
    const prompting = probe.manager.handle(
      promptReq('s-probe', 'after-probe-race'),
    )
    probeGate.release()
    await Promise.all([interrupting, configuring, prompting])

    const probeLog = await probe.log()
    const probeStarts = probeLog.filter((row) => row.event === 'start')
    assert.equal(probeStarts.length, 3)
    assert.equal(probeStarts[0].fullAccess, false)
    assert.equal(probeStarts[1].fullAccess, false)
    assert.equal(probeStarts[2].fullAccess, true)
    assertPidExited(probeLog, probeStarts[0].pid)
    assertPidExited(probeLog, probeStarts[1].pid)
    const afterProbePrompt = probeLog.find(
      (row) => row.event === 'prompt' && row.text === 'after-probe-race',
    )
    assert.ok(afterProbePrompt)
    assert.equal(afterProbePrompt.fullAccess, true)
    assertNoFailures(probe.collector.events)
  },
)

test(
  'closing session while replaceRuntime awaits updateRuntimeEpoch does not spawn a replacement runtime',
  { timeout: 5000 },
  async (t) => {
    const epochGate = createBooleanGate(readSubagents, async () => [])
    t.after(() => epochGate.release())
    const { home, collector, manager, log } = await createSessionManagerHarness(
      t,
      'agy-close-epoch-',
      {
        managerOptions: {
          hasActiveBackgroundWork: async () => true,
          readSubagents: epochGate.fn,
        },
      },
    )
    await manager.handle(
      openReq('s-epoch-close', home, { requestId: 'open-epoch-close' }),
    )

    epochGate.arm()
    const interrupting = manager.handle(
      interruptReq('s-epoch-close', 'int-epoch-close'),
    )
    await epochGate.entered
    await manager.handle(closeReq('s-epoch-close', 'close-epoch'))
    epochGate.release()
    await interrupting

    assert.equal(
      collector.events.some(
        (event) =>
          event.type === 'session.closed' &&
          event.sessionId === 's-epoch-close',
      ),
      true,
    )
    const entries = await log()
    const starts = entries.filter((row) => row.event === 'start')
    assert.equal(starts.length, 1)
    assertPidExited(entries, starts[0].pid)
  },
)

test(
  'failed interrupt relaunch during post-RPC settleBrainTurn emits failed turn instead of canceled',
  { timeout: 10_000 },
  async (t) => {
    const settleGate = createSettleReadGate()
    t.after(() => settleGate.release())
    const { home, collector, manager } = await createSessionManagerHarness(
      t,
      'agy-settle-fail-',
      {
        extraEnv: { AGY_NO_RELOAD: '1' },
        managerOptions: {
          observationPollIntervalMs: 60_000,
          createTranscriptReader: settleGate.createTranscriptReader,
        },
      },
    )
    await manager.handle(
      openReq('s-settle-fail', home, { requestId: 'open-settle-fail' }),
    )

    settleGate.arm()
    const prompting = manager.handle(promptReq('s-settle-fail', 'settle-bg'))
    await settleGate.entered
    await assert.rejects(
      manager.handle(interruptReq('s-settle-fail', 'int-settle-fail')),
      /cannot reload after interruption/,
    )
    settleGate.release()
    await prompting

    assert.equal(
      collector.events.some(
        (event) =>
          event.type === 'session.runtime_failed' &&
          event.sessionId === 's-settle-fail',
      ),
      true,
    )
    const turnEnd = collector.events.find(
      (event) =>
        event.type === 'session.turn' &&
        event.sessionId === 's-settle-fail' &&
        event.state !== 'started',
    )
    assert.ok(
      turnEnd && turnEnd.type === 'session.turn' && turnEnd.state === 'failed',
    )
    assert.ok(turnEnd.error)
    assert.match(turnEnd.error.message, /cannot reload after interruption/)
    assert.equal(
      collector.events.some(turnEvent('s-settle-fail', 'canceled')),
      false,
    )
  },
)
