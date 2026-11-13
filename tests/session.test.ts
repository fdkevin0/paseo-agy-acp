import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import {
  mkdtemp,
  readFile,
  rm,
  mkdir,
  writeFile,
  symlink,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'
import { readSubagents } from '../server/brain.ts'
import { createProvider } from '../server/provider.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents, writeBrainTranscript } from './helpers.ts'

function manager(extraArgs: string[] = []) {
  return new SessionManager({
    launch: {
      command: process.execPath,
      args: [
        fileURLToPath(new URL('./mock-acp.ts', import.meta.url)),
        ...extraArgs,
      ],
      env: { PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: [],
    emit() {},
  })
}

test(
  'protocol disconnection kills an active child and shutdown waits for it',
  { timeout: 5000 },
  async () => {
    const m = manager()
    try {
      const session = await m.start('broken', process.cwd())
      let exited = false
      void session.transport.exited.then(() => {
        exited = true
      })
      const prompt = session.transport.rpc.prompt({
        sessionId: 'disconnect-test',
        prompt: [{ type: 'text', text: 'disconnect' }],
      })
      await assert.rejects(prompt)
      assert.equal(session.transport.closed, true)
      assert.equal(session.state.failed, true)
      await m.close()
      assert.equal(exited, true)
    } finally {
      await m.close()
    }
  },
)

test(
  'stall failure interrupts the native session and rejects between-turn permissions, while ERROR_MESSAGE failure does not interrupt',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-session-stall-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'permission', 'session.configure'],
      emit(event) {
        events.push(event)
      },
      stallTimeoutMs: 30,
    })
    const waitTurn = async (state: 'completed' | 'failed' | 'canceled') => {
      const deadline = Date.now() + 2000
      while (
        !events.some((e) => e.type === 'session.turn' && e.state === state)
      ) {
        assert.ok(Date.now() < deadline, JSON.stringify(events))
        await delay(5)
      }
    }
    const readRuntimeLog = async () =>
      (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
    try {
      await m.handle({
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
          mode: 'full-access',
        },
      })
      // 1. Native ERROR_MESSAGE fails the turn without calling interrupt()
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'err-1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'error-step' }],
          },
        },
      })
      await waitTurn('failed')
      const afterErrorLog = await readRuntimeLog()
      assert.equal(
        afterErrorLog.some((r) => r.event === 'cancel'),
        false,
        'non-stall ERROR_MESSAGE must not interrupt the session',
      )

      // 2. Stall failure interrupts the native session, and late permission after turn end is canceled even in full-access mode
      events.length = 0
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'stall-1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'stall' }],
          },
        },
      })
      await waitTurn('failed')
      await m.handle({
        type: 'session.configure',
        requestId: 'cfg-after-stall',
        sessionId: 's',
        changes: { thinkingOption: 'high' },
      })
      const afterStallLog = await readRuntimeLog()
      assert.equal(
        afterStallLog.some((r) => r.event === 'cancel'),
        true,
        'stalled turn must interrupt the native session',
      )
      assert.deepEqual(
        afterStallLog.find((r) => r.event === 'late-permission')?.outcome,
        { outcome: 'cancelled' },
        'permission arriving when !state.turn must be canceled even in full-access mode',
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

for (const blocked of ['closing', 'failed', 'capability'] as const) {
  test(`subagent items clear native IDs when linking is blocked by ${blocked}`, async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-blocked-link-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: blocked === 'capability' ? [] : ['session.subsession'],
      emit: (e) => events.push(structuredClone(e)),
    })
    const session = await m.start('s', home)
    try {
      session.state.nativeId = '11111111-1111-4111-8111-111111111111'
      session.state.persisted = true
      if (blocked !== 'capability') session.state[blocked] = true
      session.emitItem({
        type: 'tool_call',
        id: 'tool',
        callId: 'tool',
        name: 'invoke_subagent',
        status: 'completed',
        error: null,
        detail: {
          type: 'sub_agent',
          log: '',
          childSessionId: '22222222-2222-4222-8222-222222222222',
        },
      })
      assert.equal(events.filter((e) => e.type === 'session.opened').length, 0)
      const item = events.find((e) => e.type === 'timeline.item')
      assert.ok(
        item?.type === 'timeline.item' &&
          item.item.type === 'tool_call' &&
          item.item.detail.type === 'sub_agent',
      )
      assert.equal(item.item.detail.childSessionId, undefined)
    } finally {
      await session.close()
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

for (const failure of ['closing', 'failed'] as const) {
  test(`terminalizing during ownership refresh never exposes native IDs: ${failure}`, async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-refresh-boundary-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['session.subsession'],
      emit: (e) => events.push(structuredClone(e)),
    })
    const session = await m.start('s', home)
    try {
      session.state.nativeId = '11111111-1111-4111-8111-111111111111'
      session.state.persisted = true
      session.state.loading = false
      await session.flush()
      const update = session.timeline.update.bind(session.timeline)
      let interrupted = false
      t.mock.method(
        session.timeline,
        'update',
        (...args: Parameters<typeof update>) => {
          const item = update(...args)
          if (item?.type === 'tool_call' && item.name === 'send_message')
            queueMicrotask(() => {
              interrupted = true
              session.state[failure] = true
              session.terminalize(failure === 'failed' ? 'failed' : 'canceled')
            })
          return item
        },
      )
      await session.transport.rpc.prompt({
        sessionId: session.state.nativeId,
        prompt: [{ type: 'text', text: 'message-refresh-boundary' }],
      })
      await session.drain()
      assert.equal(interrupted, true)
      const tools = events.filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'tool_call',
      )
      assert.equal(tools.length, 1)
      for (const event of tools) {
        assert.ok(
          event.type === 'timeline.item' &&
            event.item.type === 'tool_call' &&
            event.item.detail.type === 'sub_agent',
        )
        assert.equal(event.item.detail.childSessionId, undefined)
        assert.equal(
          event.item.status,
          failure === 'failed' ? 'failed' : 'canceled',
        )
      }
    } finally {
      await session.close()
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

for (const killedAtSpawn of [false, true]) {
  test(`syncKilledChildren waits for grandchild history and cancellation (killed at spawn: ${killedAtSpawn})`, async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-grandchild-cancel-'))
    const events: ProviderEvent[] = []
    const root = '11111111-1111-4111-8111-111111111111'
    const child = '22222222-2222-4222-8222-222222222222'
    const grand = '33333333-3333-4333-8333-333333333333'
    const logs = join(
      home,
      '.gemini/antigravity-cli/brain',
      child,
      '.system_generated/logs',
    )
    await mkdir(logs, { recursive: true })
    await writeFile(
      join(logs, 'transcript_full.jsonl'),
      [
        {
          step_index: 0,
          type: 'PLANNER_RESPONSE',
          tool_calls: [
            {
              id: 'spawn-grand',
              name: 'invoke_subagent',
              args: { Prompt: 'work' },
            },
          ],
        },
        {
          step_index: 1,
          type: 'GENERIC',
          status: 'DONE',
          content: JSON.stringify({ conversationId: grand }),
        },
      ]
        .map((row) => JSON.stringify(row) + '\n')
        .join(''),
    )
    if (killedAtSpawn) {
      const descriptors = join(
        home,
        '.gemini/antigravity-cli/brain',
        child,
        '.system_generated/subagents',
      )
      await mkdir(descriptors, { recursive: true })
      await writeFile(
        join(descriptors, `${grand}.json`),
        JSON.stringify({
          conversationId: grand,
          state: 'SUBAGENT_STATE_KILLED',
        }),
      )
    }
    const grandLogs = join(
      home,
      '.gemini/antigravity-cli/brain',
      grand,
      '.system_generated/logs',
    )
    await mkdir(grandLogs, { recursive: true })
    await writeFile(
      join(grandLogs, 'transcript_full.jsonl'),
      [
        {
          step_index: 0,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          content: 'Grandchild before kill',
        },
        {
          step_index: 1,
          type: 'PLANNER_RESPONSE',
          tool_calls: [
            {
              id: 'pending',
              name: 'run_command',
              args: { CommandLine: 'work' },
            },
          ],
        },
      ]
        .map((row) => JSON.stringify(row) + '\n')
        .join(''),
    )
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['session.subsession'],
      emit: (e) => events.push(structuredClone(e)),
    })
    const session = await m.start('s', home)
    try {
      session.state.nativeId = root
      session.state.persisted = true
      session.state.loading = false
      session.emitItem({
        type: 'tool_call',
        id: 'spawn-child',
        callId: 'spawn-child',
        name: 'invoke_subagent',
        status: 'completed',
        error: null,
        detail: { type: 'sub_agent', log: '', childSessionId: child },
      })
      const deadline = Date.now() + 2000
      while (!session.hasChild(grand)) {
        assert.ok(Date.now() < deadline, 'grandchild not opened')
        await delay(5)
      }
      session.rememberSubagents(
        [
          {
            conversationId: grand,
            state: 'SUBAGENT_STATE_KILLED',
            spawnStepIndex: 1,
          },
        ],
        child,
      )
      await session.syncKilledChildren([])
      const saved = events.findIndex(
        (e) =>
          e.type === 'timeline.item' &&
          e.sessionId === `s:child:${grand}` &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'Grandchild before kill',
      )
      const canceled = events.findIndex(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === `s:child:${grand}` &&
          e.state === 'canceled',
      )
      assert.ok(saved >= 0 && canceled > saved)
    } finally {
      await session.close()
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  })
}

test('ownership read failures do not poison subsequent ACP notifications', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-ownership-read-error-'))
  const root = '11111111-1111-4111-8111-111111111111'
  const child = '22222222-2222-4222-8222-222222222222'
  const brain = join(home, '.gemini/antigravity-cli/brain')
  const system = join(brain, root, '.system_generated')
  const outside = join(home, 'outside')
  await mkdir(system, { recursive: true })
  await mkdir(outside)
  await writeFile(
    join(outside, `${child}.json`),
    JSON.stringify({ conversationId: child }),
  )
  await symlink(outside, join(system, 'subagents'))
  await assert.rejects(
    readSubagents(brain, root),
    /Brain artifact escapes its directory/,
  )
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['session.subsession'],
    emit: (e) => events.push(structuredClone(e)),
  })
  const session = await m.start('s', home)
  async function message() {
    await session.transport.rpc.prompt({
      sessionId: root,
      prompt: [{ type: 'text', text: 'message-refresh-boundary' }],
    })
    await session.drain()
    const event = events.findLast(
      (e) => e.type === 'timeline.item' && e.item.type === 'tool_call',
    )
    assert.ok(
      event?.type === 'timeline.item' &&
        event.item.type === 'tool_call' &&
        event.item.detail.type === 'sub_agent',
    )
    return event.item.detail.childSessionId
  }
  try {
    session.state.nativeId = root
    session.state.persisted = true
    session.state.loading = false
    await session.flush()
    assert.equal(await message(), undefined)
    assert.equal(
      events.some((e) => e.type === 'session.opened'),
      false,
    )
    await rm(join(system, 'subagents'))
    await mkdir(join(system, 'subagents'))
    await writeFile(
      join(system, 'subagents', `${child}.json`),
      JSON.stringify({ conversationId: child }),
    )
    assert.equal(await message(), `s:child:${child}`)
    assert.equal(
      events.filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'tool_call',
      ).length,
      2,
    )
    assert.equal(events.filter((e) => e.type === 'session.opened').length, 1)
  } finally {
    await session.close()
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('history:skip coalesces restored context usage before ready and configure validates options before full-access relaunch', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-skip-usage-config-'))
  const log = join(home, 'calls.log')
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: {
        HOME: home,
        PATH: process.env.PATH ?? '',
        AGY_TEST_LOAD_USAGE: '1',
        AGY_TEST_LOG_LOAD: '1',
        AGY_TEST_LOG: log,
      },
    },
    usage: new AgyUsageSource(),
    capabilities: ['session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.open({
      type: 'session.open',
      requestId: 'open-skip',
      sessionId: 's',
      history: 'skip',
      persistence: {
        version: 1,
        data: { sessionId: '11111111-1111-4111-8111-111111111111' },
      },
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    assert.equal(events.filter((e) => e.type === 'timeline.item').length, 0)
    const usageEvents = events.filter((e) => e.type === 'session.usage')
    assert.equal(usageEvents.length, 1)
    assert.equal(usageEvents[0].usage.contextWindowUsedTokens, 900)
    assert.ok(
      events.findIndex((e) => e.type === 'session.opened') <
        events.findIndex((e) => e.type === 'session.usage') &&
        events.findIndex((e) => e.type === 'session.usage') <
          events.findIndex((e) => e.type === 'session.ready'),
    )

    // thought_level is omitted from advertised options, so { mode: 'full-access', thinkingOption: 'high' } fails pre-validation without relaunching.
    const loadsBefore = (await readFile(log, 'utf8'))
      .trim()
      .split('\n')
      .filter((l) => l === 'load').length
    await assert.rejects(
      m.handle({
        type: 'session.configure',
        requestId: 'cfg-bad',
        sessionId: 's',
        changes: { mode: 'full-access', thinkingOption: 'high' },
      }),
      /does not advertise thought_level/,
    )
    const loadsAfter = (await readFile(log, 'utf8'))
      .trim()
      .split('\n')
      .filter((l) => l === 'load').length
    assert.equal(loadsAfter, loadsBefore)
    const latestConfig = events.findLast((e) => e.type === 'session.config')
    assert.ok(latestConfig?.type === 'session.config')
    assert.notEqual(latestConfig.config.mode, 'full-access')
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('interrupt rejects after runtime_failed without reviving or spawning a replacement runtime', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-failed-'))
  const log = join(home, 'starts.log')
  const fixture = join(home, 'fixture-crash.mjs')
  const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk')
  await writeFile(
    fixture,
    `
import { AgentSideConnection, ndJsonStream } from ${JSON.stringify(sdkUrl)}
import { appendFileSync } from 'node:fs'
import { Readable, Writable } from 'node:stream'

appendFileSync(process.env.AGY_TEST_LOG, 'start\\n')
const nativeId = '11111111-1111-4111-8111-111111111111'
const stream = ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin))
new AgentSideConnection((client) => ({
  async initialize() {
    return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
  },
  async authenticate() { return {} },
  async newSession() {
    setImmediate(() => {
      void client.sessionUpdate({
        sessionId: nativeId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { sessionId: nativeId, configOptions: [] }
  },
  async loadSession() {
    appendFileSync(process.env.AGY_TEST_LOG, 'load\\n')
    return { configOptions: [] }
  },
  async cancel() {},
  async prompt() {
    process.stderr.write('fatal runtime crash\\n')
    process.exit(3)
  },
}), stream)
`,
  )
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fixture],
      env: {
        HOME: home,
        PATH: process.env.PATH ?? '',
        AGY_TEST_LOG: log,
      },
    },
    usage: new AgyUsageSource(),
    capabilities: ['session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.open({
      type: 'session.open',
      requestId: 'open-crash',
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
    // Seed an unconsumed high-priority inbox message so hasActiveBackgroundWork would be true if reached.
    const inboxDir = join(
      home,
      '.gemini',
      'antigravity-cli',
      'brain',
      '11111111-1111-4111-8111-111111111111',
      '.system_generated',
      'inbox',
    )
    await mkdir(inboxDir, { recursive: true })
    await writeFile(
      join(inboxDir, 'msg-1.json'),
      JSON.stringify({ priority: 'MESSAGE_PRIORITY_HIGH', consumed: false }),
    )

    await m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'crash-turn',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'crash' }] },
      },
    })
    const failedEvents = events.filter(
      (e) => e.type === 'session.runtime_failed',
    )
    assert.equal(failedEvents.length, 1)
    assert.match(failedEvents[0].error.message, /code 3/)
    assert.match(failedEvents[0].error.message, /fatal runtime crash/)

    await assert.rejects(
      m.handle({
        type: 'session.interrupt',
        requestId: 'int-after-crash',
        sessionId: 's',
      }),
      /ACP runtime has exited; reopen the session/,
    )
    const lines = (await readFile(log, 'utf8')).trim().split('\n')
    assert.deepEqual(lines, ['start'])
    assert.equal(
      events.filter((e) => e.type === 'session.runtime_failed').length,
      1,
    )
    await assert.rejects(
      m.handle({
        type: 'session.configure',
        requestId: 'cfg-after-crash',
        sessionId: 's',
        changes: { mode: 'full-access' },
      }),
      /ACP runtime has exited; reopen the session/,
    )
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('relaunch surfaces replacement runtime crash during drain and emits runtime_failed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-relaunch-drain-crash-'))
  const log = join(home, 'relaunch.log')
  const fixture = join(home, 'fixture-relaunch-crash.mjs')
  const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk')
  await writeFile(
    fixture,
    `
import { AgentSideConnection, ndJsonStream } from ${JSON.stringify(sdkUrl)}
import { appendFileSync, readFileSync } from 'node:fs'
import { Readable, Writable } from 'node:stream'

appendFileSync(process.env.AGY_TEST_LOG, 'start\\n')
const starts = readFileSync(process.env.AGY_TEST_LOG, 'utf8').trim().split('\\n').filter((l) => l === 'start').length
const nativeId = '11111111-1111-4111-8111-111111111111'
const configOptions = [
  {
    id: 'mode',
    name: 'Mode',
    category: 'mode',
    type: 'select',
    currentValue: 'default',
    options: [
      { value: 'default', name: 'Default' },
      { value: 'accept-edits', name: 'Accept edits' },
    ],
  },
]
const stream = ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin))
new AgentSideConnection((client) => ({
  async initialize() {
    return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
  },
  async authenticate() { return {} },
  async newSession() {
    setImmediate(() => {
      void client.sessionUpdate({
        sessionId: nativeId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { sessionId: nativeId, configOptions }
  },
  async loadSession() {
    appendFileSync(process.env.AGY_TEST_LOG, 'load\\n')
    if (starts >= 2) {
      setImmediate(() => {
        process.stderr.write('relaunch tail crash\\n')
        process.exit(4)
      })
    }
    return { configOptions }
  },
  async setSessionConfigOption({ configId, value }) {
    return {
      configOptions: configOptions.map((o) =>
        o.id === configId ? { ...o, currentValue: String(value) } : o,
      ),
    }
  },
  async cancel() {},
  async prompt() { return { stopReason: 'end_turn' } },
}), stream)
`,
  )
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fixture],
      env: {
        HOME: home,
        PATH: process.env.PATH ?? '',
        AGY_TEST_LOG: log,
      },
    },
    usage: new AgyUsageSource(),
    capabilities: ['session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.open({
      type: 'session.open',
      requestId: 'open-relaunch',
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
    const configsBefore = events.filter(
      (e) => e.type === 'session.config',
    ).length
    await assert.rejects(
      m.handle({
        type: 'session.configure',
        requestId: 'cfg-relaunch-crash',
        sessionId: 's',
        changes: { mode: 'full-access' },
      }),
    )
    const failedEvents = events.filter(
      (e) => e.type === 'session.runtime_failed',
    )
    assert.equal(failedEvents.length, 1)
    assert.match(failedEvents[0].error.message, /code 4/)
    assert.match(failedEvents[0].error.message, /relaunch tail crash/)
    assert.equal(
      events.filter((e) => e.type === 'session.config').length,
      configsBefore,
    )
    await assert.rejects(
      m.handle({
        type: 'session.interrupt',
        requestId: 'int-after-relaunch-crash',
        sessionId: 's',
      }),
      /ACP runtime has exited; reopen the session/,
    )
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test(
  'retry exhaustion with retained background work stops its owner instead of leaking the task',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-retry-owner-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message'],
      emit: (event) => events.push(event),
      stallTimeoutMs: 30,
      retryTimeoutMs: 50,
    })
    try {
      await m.open({
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
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'retry-background',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'retry-background' }],
          },
        },
      })
      const failed = events.find(
        (event) => event.type === 'session.turn' && event.state === 'failed',
      )
      assert.ok(failed?.type === 'session.turn' && failed.state === 'failed')
      assert.match(failed.error?.message ?? '', /attempt 8.*synthetic timeout/)
      await delay(750)
      const log = (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      assert.equal(
        log.filter((row) => row.event === 'start').length,
        2,
        'active background work requires stopping and relaunching its native owner',
      )
      assert.ok(log.some((row) => row.event === 'cancel'))
      assert.equal(
        log.some((row) => row.event === 'leaked-background'),
        false,
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test(
  'retry timeout cannot interrupt an active foreground RPC or leak its old deadline into the next turn',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-retry-foreground-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message'],
      emit: (event) => events.push(event),
      stallTimeoutMs: 10,
      retryTimeoutMs: 20,
    })
    try {
      await m.open({
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
      for (const text of ['retry-foreground-active', 'normal next turn']) {
        await m.handle({
          type: 'session.prompt',
          sessionId: 's',
          prompt: {
            clientMessageId: text,
            delivery: 'auto',
            input: { type: 'message', content: [{ type: 'text', text }] },
          },
        })
      }
      const turns = events.flatMap((event) =>
        event.type === 'session.turn' && event.state !== 'started'
          ? [event.state]
          : [],
      )
      assert.deepEqual(turns, ['completed', 'completed'])
      const log = (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      assert.ok(log.some((row) => row.event === 'held-foreground'))
      assert.equal(
        log.some((row) => row.event === 'cancel'),
        false,
      )
      assert.equal(log.filter((row) => row.event === 'start').length, 1)
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test('context uses latest turn snapshot on cancel/error/restore and preserves native updates', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-snapshot-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message'],
    emit: (e) => events.push(structuredClone(e)),
  })
  const config = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  try {
    await m.open({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config,
    })
    const openedEvent = events.find((e) => e.type === 'session.opened')
    assert.ok(openedEvent?.type === 'session.opened')
    const persistence = openedEvent.persistence!
    let promptIndex = 0
    for (const text of [
      'usage-cancel',
      'usage-empty',
      'usage-error',
      'usage-native',
      'usage-empty',
    ]) {
      events.length = 0
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: `${text}:${++promptIndex}`,
          delivery: 'auto',
          input: { type: 'message', content: [{ type: 'text', text }] },
        },
      })
      const usage = events.findLast((e) => e.type === 'session.usage')
      assert.ok(usage?.type === 'session.usage')
      assert.equal(
        usage.usage.contextWindowUsedTokens,
        text === 'usage-native' || usage.usage.contextWindowMaxTokens === 128000
          ? 1234
          : 70463,
      )
      assert.equal(usage.usage.inputTokens, 600000)
      assert.equal(usage.usage.outputTokens, 4405)
      assert.ok(
        events.some(
          (e) =>
            e.type === 'session.turn' &&
            e.state ===
              (text === 'usage-cancel'
                ? 'canceled'
                : text === 'usage-error'
                  ? 'failed'
                  : 'completed'),
        ),
      )
    }
    await m.handle({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    events.length = 0
    await m.open({
      type: 'session.open',
      requestId: 'restore',
      sessionId: 'restored',
      history: 'skip',
      config,
      persistence,
    })
    const restored = events.findLast((e) => e.type === 'session.usage')
    assert.ok(restored?.type === 'session.usage')
    assert.equal(restored.usage.contextWindowUsedTokens, 70463)
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test(
  'history replay retains completed tools before a partial transcript tail',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-tool-prefix-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: [],
      emit: (e) => events.push(e),
    })
    const session = await m.start('s', home)
    try {
      const native = '11111111-1111-4111-8111-111111111111'
      const dir = join(session.brainRoot, native, '.system_generated/logs')
      await mkdir(dir, { recursive: true })
      const rows = [
        {
          step_index: 0,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'historical-tool',
              name: 'run_command',
              args: { CommandLine: 'echo retained' },
            },
          ],
        },
        {
          step_index: 1,
          type: 'GENERIC',
          status: 'DONE',
          content: 'retained output',
        },
      ]
      await writeFile(
        join(dir, 'transcript_full.jsonl'),
        rows.map((r) => JSON.stringify(r) + '\n').join('') + '{"step_index":2',
      )
      session.state.nativeId = native
      session.state.persisted = true
      session.state.opening = false
      session.state.opened = true
      session.state.loading = false
      session.state.replay = true
      await session.replayTools()
      assert.deepEqual(
        events.flatMap((e) =>
          e.type === 'timeline.item' && e.item.type === 'tool_call'
            ? [
                {
                  callId: e.item.callId,
                  status: e.item.status,
                  detail: e.item.detail,
                },
              ]
            : [],
        ),
        [
          {
            callId: 'historical-tool',
            status: 'completed',
            detail: {
              type: 'shell',
              command: 'echo retained',
              cwd: undefined,
              output: 'retained output',
            },
          },
        ],
      )
      assert.equal(session.state.lastKnownStepIndex, 1)
    } finally {
      await session.close()
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test('native compaction has stable live/history identity, command-only visibility and no invented canceled completion or usage', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-compaction-session-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: {
        HOME: home,
        PATH: process.env.PATH ?? '',
        AGY_TEST_CHECKPOINT_HISTORY: '1',
      },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message', 'prompt.command'],
    emit: (event) => events.push(structuredClone(event)),
  })
  const config = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  const compactions = () =>
    events.flatMap((event) =>
      event.type === 'timeline.item' && event.item.type === 'compaction'
        ? [event]
        : [],
    )
  try {
    await m.open({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config,
    })
    const opened = events.find((event) => event.type === 'session.opened')
    assert.ok(opened?.type === 'session.opened')
    const persistence = opened.persistence!
    for (const text of ['checkpoint-turn', '/compact', 'checkpoint-cancel']) {
      const before = compactions().length
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: text,
          delivery: 'auto',
          input:
            text === '/compact'
              ? { type: 'command', name: 'compact', arguments: '' }
              : { type: 'message', content: [{ type: 'text', text }] },
        },
      })
      assert.equal(
        compactions().length - before,
        text === 'checkpoint-cancel' ? 0 : 1,
      )
      const usage = events.findLast((event) => event.type === 'session.usage')
      assert.ok(usage?.type === 'session.usage')
      assert.equal(usage.usage.contextWindowUsedTokens, 12)
      assert.equal(usage.usage.inputTokens, 500)
      assert.equal(usage.usage.outputTokens, 50)
    }
    const liveIds = compactions().map((event) => event.item.id)
    assert.equal(liveIds.length, 2)
    assert.ok(
      events.some(
        (event) => event.type === 'session.turn' && event.state === 'canceled',
      ),
    )
    await m.handle({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    for (const history of ['replay', 'skip', 'replay'] as const) {
      events.length = 0
      await m.open({
        type: 'session.open',
        requestId: `open:${history}`,
        sessionId: 's',
        history,
        persistence,
        config,
      })
      assert.deepEqual(
        compactions().map((event) => event.item.id),
        history === 'skip' ? [] : liveIds,
      )
      for (const event of compactions()) {
        assert.equal(event.sessionId, 's')
        assert.equal(event.timestamp, '2026-01-01T00:00:00Z')
        assert.deepEqual(event.item, {
          type: 'compaction',
          id: event.item.id,
          status: 'completed',
        })
      }
      const usage = events.findLast((event) => event.type === 'session.usage')
      assert.ok(usage?.type === 'session.usage')
      assert.equal(usage.usage.contextWindowUsedTokens, 12)
      await m.handle({
        type: 'session.close',
        requestId: `close:${history}`,
        sessionId: 's',
      })
    }
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test(
  'Orchestrator catalog probe, session.open, bidirectional configure, relaunch models preservation and standalone thinking validation',
  { timeout: 10000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-orchestrator-session-'))
    const logPath = join(home, 'rpc.log')
    const fixturePath = join(home, 'orchestrator-acp.mjs')
    const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk')
    await writeFile(
      fixturePath,
      `
import { AgentSideConnection, ndJsonStream } from ${JSON.stringify(sdkUrl)}
import { appendFileSync } from 'node:fs'
import { WritableStream } from 'node:stream/web'
import { Readable, Writable } from 'node:stream'

const log = (line) => {
  if (process.env.AGY_TEST_LOG) appendFileSync(process.env.AGY_TEST_LOG, line + '\\n')
}
log('spawn:skip=' + process.argv.includes('--dangerously-skip-permissions'))

let currentModel = 'gemini-pro-orchestrator'
let currentThought = 'medium'
let currentMode = 'default'
let omitModelsNext = false

function buildConfigOptions() {
  const opts = [
    {
      id: 'model',
      name: 'Model',
      type: 'select',
      currentValue: currentModel,
      options: [
        { value: 'gemini-4-carbon', name: 'Gemini 4 Carbon' },
        { value: 'gemini-4-hydrogen', name: 'Gemini 4 Hydrogen' },
        { value: 'gemini-pro-orchestrator', name: 'Orchestrator (Carbon)' },
        { value: 'gemini-pro-orchestrator-argon', name: 'Orchestrator (Argon)' },
        { value: 'gpt-oss-120b-medium', name: 'GPT-OSS 120B (Medium)' },
      ],
    },
  ]
  if (currentModel === 'gemini-4-hydrogen') {
    opts.push({
      id: 'thought_level',
      name: 'Thinking Level',
      type: 'select',
      currentValue: currentThought,
      options: [
        { value: 'low', name: 'Low' },
        { value: 'medium', name: 'Medium' },
        { value: 'high', name: 'High' },
        { value: 'xhigh', name: 'Extra' },
      ],
    })
  } else if (
    currentModel === 'gemini-4-carbon' ||
    currentModel === 'gpt-oss-120b-medium'
  ) {
    if (currentThought === 'xhigh') currentThought = 'high'
    opts.push({
      id: 'thought_level',
      name: 'Thinking Level',
      type: 'select',
      currentValue: currentThought,
      options: [
        { value: 'low', name: 'Low' },
        { value: 'medium', name: 'Medium' },
        { value: 'high', name: 'High' },
      ],
    })
  }
  opts.push({
    id: 'mode',
    name: 'Mode',
    category: 'mode',
    type: 'select',
    currentValue: currentMode,
    options: [
      { value: 'default', name: 'Default' },
      { value: 'accept-edits', name: 'Accept Edits' },
      { value: 'plan', name: 'Plan Mode' },
    ],
  })
  return opts
}

const availableModels = [
  { modelId: 'gemini-4-carbon-low' },
  { modelId: 'gemini-4-carbon-medium' },
  { modelId: 'gemini-4-carbon-high' },
  { modelId: 'gemini-4-hydrogen-high' },
  { modelId: 'gemini-4-hydrogen-xhigh' },
  { modelId: 'gemini-4-hydrogen-medium' },
  { modelId: 'gemini-4-hydrogen-low' },
  { modelId: 'gemini-pro-orchestrator' },
  { modelId: 'gemini-pro-orchestrator-argon' },
  { modelId: 'gpt-oss-120b-medium' },
]

process.stdin.on('end', () => process.exit(0))
const stream = ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin))
const writer = stream.writable.getWriter()
stream.writable = new WritableStream({
  write(message) {
    if (
      'result' in message &&
      message.result &&
      typeof message.result === 'object' &&
      'configOptions' in message.result
    ) {
      if (omitModelsNext) {
        omitModelsNext = false
      } else if ('sessionId' in message.result) {
        message = {
          ...message,
          result: {
            ...message.result,
            models: { availableModels },
          },
        }
      }
    }
    return writer.write(message)
  },
})

new AgentSideConnection((client) => ({
  async initialize() {
    return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
  },
  async authenticate() {
    return {}
  },
  async newSession() {
    const sessionId = '11111111-1111-4111-8111-111111111111'
    setTimeout(() => {
      void client.sessionUpdate({
        sessionId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    }, 0)
    return { sessionId, configOptions: buildConfigOptions() }
  },
  async loadSession({ sessionId }) {
    omitModelsNext = true
    setTimeout(() => {
      void client.sessionUpdate({
        sessionId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    }, 0)
    return { configOptions: buildConfigOptions() }
  },
  async closeSession() {
    return {}
  },
  async setSessionConfigOption({ configId, value }) {
    log('set:' + configId + '=' + value)
    const opts = buildConfigOptions()
    const target = opts.find((o) => o.id === configId)
    if (!target) throw new Error('unadvertised ' + configId)
    if (!target.options.some((o) => o.value === value)) {
      throw new Error('invalid ' + configId + '=' + value)
    }
    if (configId === 'model') currentModel = String(value)
    else if (configId === 'thought_level') currentThought = String(value)
    else if (configId === 'mode') currentMode = String(value)
    return { configOptions: buildConfigOptions() }
  },
  async prompt() {
    return { stopReason: 'end_turn' }
  },
  async cancel() {},
}), stream)
`,
    )
    const provider = await createProvider(new AgyUsageSource()).connect({
      versions: [1],
      capabilities: PROVIDER_CAPABILITIES,
      launch: {
        command: process.execPath,
        args: [fixturePath],
        env: {
          HOME: home,
          PATH: process.env.PATH ?? '',
          AGY_TEST_LOG: logPath,
        },
      },
    })
    const { events, wait } = collectProviderEvents(provider)
    try {
      // 1. Catalog probe when native default is gemini-pro-orchestrator
      await provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
      const catalogEvent = await wait(
        (e) => e.type === 'catalog' && e.requestId === 'cat-1',
      )
      assert.equal(catalogEvent.type, 'catalog')
      assert.equal(catalogEvent.catalog.defaultModel, 'gemini-pro-orchestrator')
      assert.deepEqual(catalogEvent.catalog.thinkingOptions, [])
      assert.equal(catalogEvent.catalog.defaultThinkingOption, undefined)
      const hydrogen = catalogEvent.catalog.models.find(
        (m) => m.id === 'gemini-4-hydrogen',
      )
      assert.deepEqual(hydrogen?.thinkingOptions, [
        { id: 'low', label: 'Low' },
        { id: 'medium', label: 'Medium' },
        { id: 'high', label: 'High' },
        { id: 'xhigh', label: 'Extra' },
      ])
      assert.equal(hydrogen?.defaultThinkingOptionId, 'medium')
      assert.deepEqual(
        catalogEvent.catalog.models.find(
          (m) => m.id === 'gemini-pro-orchestrator',
        )?.thinkingOptions,
        [],
      )

      // 2. session.open on gemini-pro-orchestrator with host-normalized thinkingOption: 'medium'
      events.length = 0
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
          model: 'gemini-pro-orchestrator',
          thinkingOption: 'medium',
        },
      })
      await wait((e) => e.type === 'session.ready' && e.sessionId === 's1')
      let cfg = events.findLast(
        (e) => e.type === 'session.config' && e.sessionId === 's1',
      )
      assert.ok(cfg?.type === 'session.config')
      assert.equal(cfg.config.model, 'gemini-pro-orchestrator')
      assert.equal(cfg.config.thinkingOption, undefined)

      // 3. Whole-bundle session.configure while already on gemini-pro-orchestrator
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-orch',
        sessionId: 's1',
        changes: {
          model: 'gemini-pro-orchestrator',
          mode: 'plan',
          thinkingOption: 'medium',
        },
      })
      cfg = await wait(
        (e) => e.type === 'session.config' && e.sessionId === 's1',
      )
      assert.equal(cfg.type, 'session.config')
      assert.equal(cfg.config.model, 'gemini-pro-orchestrator')
      assert.equal(cfg.config.mode, 'plan')
      assert.equal(cfg.config.thinkingOption, undefined)

      // 4. Orchestrator -> Hydrogen (xhigh) + full-access relaunch preserves models, model, and xhigh
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-h2',
        sessionId: 's1',
        changes: {
          model: 'gemini-4-hydrogen',
          thinkingOption: 'xhigh',
          mode: 'full-access',
        },
      })
      cfg = await wait(
        (e) =>
          e.type === 'session.config' &&
          e.sessionId === 's1' &&
          e.config.mode === 'full-access',
      )
      assert.equal(cfg.type, 'session.config')
      assert.equal(cfg.config.model, 'gemini-4-hydrogen')
      assert.equal(cfg.config.thinkingOption, 'xhigh')
      assert.equal(cfg.config.mode, 'full-access')

      // 5. Hydrogen -> Orchestrator after relaunch (where loadSession omitted models)
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-back',
        sessionId: 's1',
        changes: {
          model: 'gemini-pro-orchestrator',
          thinkingOption: 'medium',
          mode: 'default',
        },
      })
      cfg = await wait(
        (e) =>
          e.type === 'session.config' &&
          e.sessionId === 's1' &&
          e.config.mode === 'default',
      )
      assert.equal(cfg.type, 'session.config')
      assert.equal(cfg.config.model, 'gemini-pro-orchestrator')
      assert.equal(cfg.config.thinkingOption, undefined)
      assert.equal(cfg.config.mode, 'default')

      // 5b. session.open on a model with restricted efforts (e.g. gemini-4-carbon without xhigh)
      // ignores an unsupported best-effort thinkingOption ('xhigh') without failing session.open.
      await provider.send({
        type: 'session.open',
        requestId: 'open-carbon-xhigh',
        sessionId: 's-carbon',
        history: 'skip',
        config: {
          cwd: home,
          env: {},
          mcpServers: {},
          settings: {},
          persist: true,
          model: 'gemini-4-carbon',
          thinkingOption: 'xhigh',
        },
      })
      await wait(
        (e) => e.type === 'session.ready' && e.sessionId === 's-carbon',
      )
      const carbonCfg = events.findLast(
        (e) => e.type === 'session.config' && e.sessionId === 's-carbon',
      )
      assert.ok(carbonCfg?.type === 'session.config')
      assert.equal(carbonCfg.config.model, 'gemini-4-carbon')
      assert.equal(carbonCfg.config.thinkingOption, 'medium')
      await provider.send({
        type: 'session.close',
        requestId: 'close-carbon',
        sessionId: 's-carbon',
      })
      await wait(
        (e) => e.type === 'session.closed' && e.sessionId === 's-carbon',
      )

      // 6. Standalone { mode: 'full-access', thinkingOption: 'high' } without model rejects before relaunching
      const spawnsBefore = (await readFile(logPath, 'utf8'))
        .trim()
        .split('\n')
        .filter((line) => line.startsWith('spawn:')).length
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-reject',
        sessionId: 's1',
        changes: {
          mode: 'full-access',
          thinkingOption: 'high',
        },
      })
      const failed = await wait(
        (e) => e.type === 'request.failed' && e.requestId === 'cfg-reject',
      )
      assert.equal(failed.type, 'request.failed')
      assert.match(failed.error.message, /does not advertise thought_level/)
      const spawnsAfter = (await readFile(logPath, 'utf8'))
        .trim()
        .split('\n')
        .filter((line) => line.startsWith('spawn:')).length
      assert.equal(
        spawnsAfter,
        spawnsBefore,
        'standalone invalid thinkingOption must reject before spawning a relaunch',
      )

      // 7. Suffix-fixed model advertising thought_level ignores bundled thinkingOption and rejects standalone thinkingOption
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-gpt-oss',
        sessionId: 's1',
        changes: {
          model: 'gpt-oss-120b-medium',
          thinkingOption: 'xhigh',
        },
      })
      cfg = await wait(
        (e) =>
          e.type === 'session.config' &&
          e.sessionId === 's1' &&
          e.config.model === 'gpt-oss-120b-medium',
      )
      assert.equal(cfg.type, 'session.config')
      assert.equal(cfg.config.thinkingOption, undefined)

      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-gpt-oss-reject',
        sessionId: 's1',
        changes: {
          mode: 'full-access',
          thinkingOption: 'high',
        },
      })
      const gptFailed = await wait(
        (e) =>
          e.type === 'request.failed' && e.requestId === 'cfg-gpt-oss-reject',
      )
      assert.equal(gptFailed.type, 'request.failed')
      assert.match(gptFailed.error.message, /does not advertise thought_level/)

      // 8. Partial mutation { model: 'gemini-4-hydrogen', thinkingOption: 'invalid', mode: 'full-access' } publishes mutated model without relaunching
      events.length = 0
      await provider.send({
        type: 'session.configure',
        requestId: 'cfg-partial',
        sessionId: 's1',
        changes: {
          model: 'gemini-4-hydrogen',
          thinkingOption: 'invalid',
          mode: 'full-access',
        },
      })
      const partialFailed = await wait(
        (e) => e.type === 'request.failed' && e.requestId === 'cfg-partial',
      )
      assert.equal(partialFailed.type, 'request.failed')
      const partialCfg = events.findLast(
        (e) => e.type === 'session.config' && e.sessionId === 's1',
      )
      assert.ok(partialCfg?.type === 'session.config')
      assert.equal(partialCfg.config.model, 'gemini-4-hydrogen')
      assert.equal(partialCfg.config.mode, 'default')
      const spawnsFinal = (await readFile(logPath, 'utf8'))
        .trim()
        .split('\n')
        .filter((line) => line.startsWith('spawn:')).length
      assert.equal(
        spawnsFinal,
        spawnsBefore,
        'invalid thought_level must abort before spawning a full-access relaunch',
      )
    } finally {
      await provider.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

async function usageHarness(t: TestContext) {
  const home = await mkdtemp(join(tmpdir(), 'agy-session-usage-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message', 'prompt.command', 'session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  t.after(async () => {
    await m.close()
    await rm(home, { recursive: true, force: true })
  })
  const start = m.start.bind(m)
  let session!: Awaited<ReturnType<typeof m.start>>
  t.mock.method(
    m,
    'start',
    async (...args: Parameters<typeof start>) =>
      (session = await start(...args)),
  )
  await m.open({
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
  const write = (rows: Record<string, unknown>[]) =>
    writeBrainTranscript(hBrainRoot(), session.state.nativeId, rows)
  const hBrainRoot = () => session.brainRoot
  let sequence = 0
  const prompt = (text = 'synthetic') =>
    m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: `p${++sequence}`,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  const latest = () => {
    const event = events.findLast((e) => e.type === 'session.usage')
    assert.ok(event?.type === 'session.usage')
    return event.usage
  }
  return { m, session, events, write, prompt, latest }
}

test('zero-token canceled or command turns preserve carriedUsage, pendingReplay, and prior consumption', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 0,
    outputTokens: 0,
    cachedReadTokens: 500,
  }
  h.session.state.wakeup.pendingReplay = true
  h.session.publishUsage({
    inputTokens: 200,
    outputTokens: 20,
    cachedInputTokens: 500,
  })
  const zeroUsage = {
    inputTokens: 0,
    outputTokens: 0,
    cachedReadTokens: 0,
    totalTokens: 0,
  }

  // 1. Zero-token slash command preserves cache-only carriedUsage, pendingReplay, and prior consumption.
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'end_turn' as const,
    usage: zeroUsage,
  }))
  await h.m.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'cmd-zero',
      delivery: 'auto',
      input: { type: 'command', name: 'status', arguments: '' },
    },
  })
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 0,
    outputTokens: 0,
    cachedReadTokens: 500,
  })
  assert.equal(h.session.state.wakeup.pendingReplay, true)
  assert.equal(h.latest().inputTokens, 200)
  assert.equal(h.latest().cachedInputTokens, 500)

  // 2. Zero-token canceled message turn (before user echo) also preserves carriedUsage and pendingReplay.
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'cancelled' as const,
    usage: zeroUsage,
  }))
  await h.prompt('cancel-zero')
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 0,
    outputTokens: 0,
    cachedReadTokens: 500,
  })
  assert.equal(h.session.state.wakeup.pendingReplay, true)
  assert.equal(h.latest().inputTokens, 200)

  // 3. Subsequent billed turn deducts the preserved cache-only carriedUsage cleanly.
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'end_turn' as const,
    usage: {
      inputTokens: 120,
      outputTokens: 15,
      cachedReadTokens: 650,
      totalTokens: 785,
    },
  }))
  await h.prompt('billed-next')
  assert.equal(h.latest().inputTokens, 120)
  assert.equal(h.latest().outputTokens, 15)
  assert.equal(h.latest().cachedInputTokens, 150)
  assert.equal(h.session.state.wakeup.carriedUsage, undefined)
  assert.equal(h.session.state.wakeup.pendingReplay, false)
})

test('pre-echo prompt RPC rejection preserves pendingReplay and carriedUsage for the next turn', async (t) => {
  const h = await usageHarness(t)
  const sender = '22222222-2222-4222-8222-222222222222'
  // Turn 1: Reactive Wakeup produces pendingReplay = true and carriedUsage = {50, 5}.
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Foreground',
        input_tokens: 100,
        output_tokens: 10,
      },
      {
        step_index: 2,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=done\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 3,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Woken reply',
        input_tokens: 50,
        output_tokens: 5,
      },
    ])
    return {
      stopReason: 'end_turn' as const,
      usage: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
    }
  })
  await h.prompt('turn 1')
  assert.equal(h.session.state.wakeup.pendingReplay, true)
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 0,
  })

  // Turn 2: rpc.prompt throws before echoing user_message_chunk (no relaunch).
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    throw new Error('Transient RPC failure before echo')
  })
  await h.prompt('turn 2')
  assert.equal(
    h.session.state.wakeup.pendingReplay,
    true,
    'pendingReplay must survive pre-echo prompt RPC failure when runtime was not relaunched',
  )
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 0,
  })

  // Turn 3: succeeds with cumulative usage (110 - 50 carried = 60).
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      { step_index: 4, type: 'USER_INPUT', content: 'turn 3' },
      {
        step_index: 5,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Turn 3 reply',
        input_tokens: 60,
        output_tokens: 6,
      },
    ])
    return {
      stopReason: 'end_turn' as const,
      usage: { inputTokens: 110, outputTokens: 11, totalTokens: 121 },
    }
  })
  await h.prompt('turn 3')
  assert.equal(h.latest().inputTokens, 60)
  assert.equal(h.latest().outputTokens, 6)
  assert.equal(h.session.state.wakeup.pendingReplay, false)
  assert.equal(h.session.state.wakeup.carriedUsage, undefined)
})

test('runtime relaunch during a turn deducts pre-relaunch carriedUsage while clearing dead-runtime wakeup state', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 80,
    outputTokens: 8,
    cachedReadTokens: 10,
  }
  const sender = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([
      { step_index: 1, type: 'USER_INPUT', content: 'run' },
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        input_tokens: 100,
        output_tokens: 10,
        cache_read_tokens: 20,
        tool_calls: [
          {
            id: 'bg',
            name: 'run_command',
            args: { CommandLine: 'sleep 30' },
          },
        ],
      },
      {
        step_index: 3,
        type: 'GENERIC',
        status: 'RUNNING',
        content: 'Tool is running as a background task with task id: bg-1',
      },
      {
        step_index: 4,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Waiting',
        input_tokens: 100,
        output_tokens: 10,
        cache_read_tokens: 20,
      },
      {
        step_index: 5,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=ping\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 6,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Woken step',
        input_tokens: 40,
        output_tokens: 4,
        cache_read_tokens: 5,
      },
      {
        step_index: 7,
        type: 'ERROR_MESSAGE',
        status: 'DONE',
        created_at: '2020-01-01T00:00:00Z',
        content:
          'Error: The stream was interrupted. Please continue the task you were working on.',
      },
    ])
    return {
      stopReason: 'end_turn' as const,
      usage: {
        inputTokens: 180,
        outputTokens: 18,
        cachedReadTokens: 30,
        totalTokens: 228,
      },
    }
  })
  const oldTransport = h.session.transport
  await h.prompt('woken-stall')
  assert.notEqual(
    h.session.transport,
    oldTransport,
    'stalled woken turn with active background task must relaunch the runtime',
  )
  // 180 - 80 (prevCarried) + 40 (turnWakeupUsage) = 140
  assert.equal(h.latest().inputTokens, 140)
  assert.equal(h.latest().outputTokens, 14)
  assert.equal(h.latest().cachedInputTokens, 25)
  assert.equal(
    h.session.state.wakeup.carriedUsage,
    undefined,
    'fresh runtime after relaunch must not inherit dead-runtime wakeup debt',
  )
  assert.equal(h.session.state.wakeup.pendingReplay, false)
})

test('post-echo zero-usage cancellation clears pendingReplay while preserving carriedUsage', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 0,
  }
  h.session.state.wakeup.pendingReplay = true
  const nativePrompt = h.session.transport.rpc.prompt.bind(
    h.session.transport.rpc,
  )
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    const pending = nativePrompt({
      sessionId: h.session.state.nativeId,
      prompt: [{ type: 'text', text: 'wait' }],
    })
    for (let i = 0; i < 100 && h.session.state.wakeup.suppressEcho; i++) {
      await delay(10)
    }
    await h.session.transport.rpc.cancel({
      sessionId: h.session.state.nativeId,
    })
    await pending
    return {
      stopReason: 'cancelled' as const,
      usage: {
        inputTokens: 0,
        outputTokens: 0,
        cachedReadTokens: 0,
        totalTokens: 0,
      },
    }
  })
  await h.prompt('post-echo-cancel')
  assert.equal(
    h.session.state.wakeup.pendingReplay,
    false,
    'user_message_chunk consumed pendingReplay even though result.usage was all zero',
  )
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 0,
  })
})

test('zero-usage turn with its own wakeup accumulates carriedUsage via addStepUsage', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 10,
  }
  const sender = '33333333-3333-4333-8333-333333333333'
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([
      { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Foreground',
      },
      {
        step_index: 2,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=wake\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 3,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Woken reply',
        input_tokens: 30,
        output_tokens: 3,
        cache_read_tokens: 5,
      },
    ])
    return {
      stopReason: 'end_turn' as const,
      usage: {
        inputTokens: 0,
        outputTokens: 0,
        cachedReadTokens: 0,
        totalTokens: 0,
      },
    }
  })
  await h.prompt('zero-rpc-with-wakeup')
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 80,
    outputTokens: 8,
    cachedReadTokens: 15,
  })
  assert.equal(h.latest().inputTokens, 30)
  assert.equal(h.latest().outputTokens, 3)
  assert.equal(h.latest().cachedInputTokens, 5)
})

test('underflowing positive usage skips carriedUsage deduction and clears stale carriedUsage', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 50,
    outputTokens: 5,
    cachedReadTokens: 500,
  }
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'end_turn' as const,
    usage: {
      inputTokens: 80,
      outputTokens: 8,
      cachedReadTokens: 100,
      totalTokens: 188,
    },
  }))
  await h.prompt('underflow-turn')
  assert.equal(h.latest().inputTokens, 80)
  assert.equal(h.latest().outputTokens, 8)
  assert.equal(h.latest().cachedInputTokens, 100)
  assert.equal(
    h.session.state.wakeup.carriedUsage,
    undefined,
    'positive usage turn must clear stale carriedUsage even on underflow',
  )
})

test('post-waitPrompt settle failure still publishes positive result.usage and clears consumed carriedUsage', async (t) => {
  const h = await usageHarness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 40,
    outputTokens: 4,
    cachedReadTokens: 0,
  }
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([{ step_index: 0, type: 'USER_INPUT', content: 'turn 1' }])
    // Force observation/settle to fail after waitPrompt returns positive usage.
    h.session.state.turnAbort?.abort(new Error('Post-prompt settle aborted'))
    return {
      stopReason: 'end_turn' as const,
      usage: { inputTokens: 120, outputTokens: 12, totalTokens: 132 },
    }
  })
  await h.prompt('post-prompt-error')
  assert.equal(h.latest().inputTokens, 80)
  assert.equal(h.latest().outputTokens, 8)
  assert.equal(
    h.session.state.wakeup.carriedUsage,
    undefined,
    'consumed carriedUsage must be cleared even when post-waitPrompt settling throws',
  )
})

for (const postEcho of [false, true])
  test(`canceled synthetic ask_question continuation with zero usage preserves carriedUsage (postEcho: ${postEcho})`, async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-f2-cont-cancel-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'permission'],
      emit: (e) => events.push(structuredClone(e)),
    })
    t.after(async () => {
      await m.close()
      await rm(home, { recursive: true, force: true })
    })
    const start = m.start.bind(m)
    let session!: Awaited<ReturnType<typeof m.start>>
    t.mock.method(
      m,
      'start',
      async (...args: Parameters<typeof start>) =>
        (session = await start(...args)),
    )
    await m.open({
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
    const sender = '44444444-4444-4444-8444-444444444444'
    const nativePrompt = session.transport.rpc.prompt.bind(
      session.transport.rpc,
    )
    let rpcCount = 0
    t.mock.method(session.transport.rpc, 'prompt', async () => {
      rpcCount++
      if (rpcCount === 1) {
        await writeBrainTranscript(session.brainRoot, session.state.nativeId, [
          { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Foreground',
            input_tokens: 100,
            output_tokens: 10,
          },
          {
            step_index: 2,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${sender} priority=MESSAGE_PRIORITY_HIGH content=wake\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 3,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            input_tokens: 40,
            output_tokens: 4,
            tool_calls: [
              {
                id: 'ask-1',
                name: 'ask_question',
                args: {
                  questions: [
                    {
                      question: 'Proceed?',
                      options: [{ label: 'Yes' }, { label: 'No' }],
                    },
                  ],
                },
              },
            ],
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
        }
      }
      if (postEcho) {
        const pending = nativePrompt({
          sessionId: session.state.nativeId,
          prompt: [{ type: 'text', text: 'wait' }],
        })
        for (let i = 0; i < 100 && !session.state.continuationEchoSeen; i++) {
          await delay(10)
        }
        await session.transport.rpc.cancel({
          sessionId: session.state.nativeId,
        })
        await pending
      }
      return {
        stopReason: 'cancelled' as const,
        usage: {
          inputTokens: 0,
          outputTokens: 0,
          cachedReadTokens: 0,
          totalTokens: 0,
        },
      }
    })

    const promptPromise = m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'p-f2',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'turn 1' }] },
      },
    })
    // Wait for the woken ask_question permission request, then answer it.
    let permissionId: string | undefined
    for (let i = 0; i < 100 && !permissionId; i++) {
      const req = events.find((e) => e.type === 'session.permission')
      if (req?.type === 'session.permission') permissionId = req.request.id
      else await delay(10)
    }
    assert.ok(permissionId, 'woken ask_question must emit session.permission')
    await m.handle({
      type: 'session.permission',
      sessionId: 's',
      permissionId,
      response: {
        behavior: 'allow',
        updatedInput: { answers: { 'Proceed?': 'Yes' } },
      },
    })
    await promptPromise
    assert.deepEqual(
      session.state.wakeup.carriedUsage,
      { inputTokens: 40, outputTokens: 4, cachedReadTokens: 0 },
      'canceled zero-usage continuation must preserve pre-continuation wakeup carriedUsage',
    )
    assert.equal(
      session.state.wakeup.pendingReplay,
      !postEcho,
      'pre-echo canceled continuation must preserve pendingReplay while post-echo clears it',
    )
  })
