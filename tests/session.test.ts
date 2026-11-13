import { test } from 'node:test'
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
import { collectProviderEvents } from './helpers.ts'

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

test(
  'parallel child crash flushes failed child state, cancels running sibling watcher, and relaunches runtime',
  { timeout: 10_000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-sibling-crash-'))
    const logPath = join(home, 'agy.log')
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: {
          HOME: home,
          PATH: process.env.PATH ?? '',
          AGY_TEST_LOG: logPath,
          AGY_TEST_LOG_LOAD: '1',
        },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'session.subsession'],
      emit: (event) => events.push(structuredClone(event)),
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
      const opened = events.find(
        (e) => e.type === 'session.opened' && e.sessionId === 's',
      )
      assert.ok(opened?.type === 'session.opened')
      const nativeId = (opened.persistence!.data as { sessionId: string })
        .sessionId
      const brainRoot = join(home, '.gemini/antigravity-cli/brain')
      const childA = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
      const childB = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'

      const writeJsonl = async (id: string, rows: unknown[]) => {
        const dir = join(brainRoot, id, '.system_generated/logs')
        await mkdir(dir, { recursive: true })
        await writeFile(
          join(dir, 'transcript_full.jsonl'),
          rows.map((r) => JSON.stringify(r) + '\n').join(''),
        )
      }
      const writeSubagent = async (
        id: string,
        spawnStepIndex: number,
        callId: string,
      ) => {
        const subDir = join(brainRoot, nativeId, '.system_generated/subagents')
        await mkdir(subDir, { recursive: true })
        await writeFile(
          join(subDir, `${id}.json`),
          JSON.stringify({
            conversationId: id,
            spawnStepIndex,
            state: 'SUBAGENT_STATE_ALIVE',
          }),
        )
        const msgDir = join(brainRoot, id, '.system_generated/messages')
        await mkdir(msgDir, { recursive: true })
        await writeFile(
          join(msgDir, 'init.json'),
          JSON.stringify({
            id: 'init',
            sender: nativeId,
            recipient: id,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: 'work',
            sourceMetadata: {
              tool: {
                conversationId: nativeId,
                stepIndex: spawnStepIndex,
                toolCall: {
                  id: callId,
                  name: 'invoke_subagent',
                  argumentsJson: '{}',
                },
              },
            },
          }),
        )
        await writeFile(
          join(msgDir, 'read.json'),
          JSON.stringify({ init: true }),
        )
      }

      const sessions = (
        m as unknown as {
          sessions: Map<
            string,
            {
              transport: {
                rpc: { prompt: (...args: unknown[]) => Promise<unknown> }
              }
            }
          >
        }
      ).sessions
      const session = sessions.get('s')!
      t.mock.method(session.transport.rpc, 'prompt', async () => {
        await writeSubagent(childA, 2, 'spawn-a')
        await writeSubagent(childB, 3, 'spawn-b')
        // Start childB running and childA initially running, then childA crashes after 40ms
        // (well before childA's 1000ms watcher interval, exercising flushRunningChildren).
        await writeJsonl(childA, [
          {
            step_index: 0,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work A\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'RUNNING',
            thinking: 'working',
          },
        ])
        await writeJsonl(childB, [
          {
            step_index: 0,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work B\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              {
                id: 'long-cmd',
                name: 'run_command',
                args: { CommandLine: 'sleep 60' },
              },
            ],
          },
        ])
        await writeJsonl(nativeId, [
          { step_index: 0, type: 'USER_INPUT', content: 'parallel work' },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              { id: 'spawn-a', name: 'invoke_subagent', args: { Role: 'A' } },
              { id: 'spawn-b', name: 'invoke_subagent', args: { Role: 'B' } },
            ],
          },
          {
            step_index: 2,
            type: 'GENERIC',
            status: 'DONE',
            content: JSON.stringify({ conversationId: childA }),
          },
          {
            step_index: 3,
            type: 'GENERIC',
            status: 'DONE',
            content: JSON.stringify({ conversationId: childB }),
          },
          {
            step_index: 4,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Waiting for both subagents.',
          },
        ])
        setTimeout(() => {
          void writeJsonl(childA, [
            {
              step_index: 0,
              type: 'SYSTEM_MESSAGE',
              content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work A\n</SYSTEM_MESSAGE>`,
            },
            {
              step_index: 1,
              type: 'ERROR_MESSAGE',
              content: 'Child A crashed fatally',
            },
          ])
        }, 40)
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 120, outputTokens: 12, totalTokens: 132 },
        }
      })

      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'p1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'parallel work' }],
          },
        },
      })

      const rootFailed = events.find(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === 's' &&
          e.state === 'failed',
      )
      assert.ok(rootFailed?.type === 'session.turn')
      assert.match(rootFailed.error?.message ?? '', /Child A crashed fatally/)

      const childATerminal = events.find(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === `s:child:${childA}` &&
          e.state !== 'started',
      )
      assert.ok(childATerminal?.type === 'session.turn')
      assert.equal(
        childATerminal.state,
        'failed',
        'flushRunningChildren must observe childA failure before sibling cancellation runs',
      )

      const childBTerminal = events.find(
        (e) =>
          e.type === 'session.turn' &&
          e.sessionId === `s:child:${childB}` &&
          e.state !== 'started',
      )
      assert.ok(childBTerminal?.type === 'session.turn')
      assert.equal(
        childBTerminal.state,
        'canceled',
        'active sibling childB must be canceled when the root turn fails',
      )

      const logLines = (await readFile(logPath, 'utf8')).trim().split('\n')
      assert.ok(logLines.includes('cancel'))
      assert.ok(
        logLines.includes('load'),
        'runtime must relaunch to terminate leaked sibling subagent work',
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test(
  're-woken idle child crash and child retry-timeout stall both flush child watcher to failed instead of canceled',
  { timeout: 10_000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-rewoken-flush-'))
    const logPath = join(home, 'agy.log')
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: {
          HOME: home,
          PATH: process.env.PATH ?? '',
          AGY_TEST_LOG: logPath,
          AGY_TEST_LOG_LOAD: '1',
        },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'session.subsession'],
      retryTimeoutMs: 40,
      emit: (event) => events.push(structuredClone(event)),
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
      const opened = events.find(
        (e) => e.type === 'session.opened' && e.sessionId === 's',
      )
      assert.ok(opened?.type === 'session.opened')
      const nativeId = (opened.persistence!.data as { sessionId: string })
        .sessionId
      const brainRoot = join(home, '.gemini/antigravity-cli/brain')
      const childA = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
      const childB = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'

      const writeJsonl = async (id: string, rows: unknown[]) => {
        const dir = join(brainRoot, id, '.system_generated/logs')
        await mkdir(dir, { recursive: true })
        await writeFile(
          join(dir, 'transcript_full.jsonl'),
          rows.map((r) => JSON.stringify(r) + '\n').join(''),
        )
      }
      const writeSubagent = async (
        id: string,
        spawnStepIndex: number,
        callId: string,
      ) => {
        const subDir = join(brainRoot, nativeId, '.system_generated/subagents')
        await mkdir(subDir, { recursive: true })
        await writeFile(
          join(subDir, `${id}.json`),
          JSON.stringify({
            conversationId: id,
            spawnStepIndex,
            state: 'SUBAGENT_STATE_ALIVE',
          }),
        )
        const msgDir = join(brainRoot, id, '.system_generated/messages')
        await mkdir(msgDir, { recursive: true })
        await writeFile(
          join(msgDir, 'init.json'),
          JSON.stringify({
            id: 'init',
            sender: nativeId,
            recipient: id,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: 'work',
            sourceMetadata: {
              tool: {
                conversationId: nativeId,
                stepIndex: spawnStepIndex,
                toolCall: {
                  id: callId,
                  name: 'invoke_subagent',
                  argumentsJson: '{}',
                },
              },
            },
          }),
        )
        await writeFile(
          join(msgDir, 'read.json'),
          JSON.stringify({ init: true }),
        )
      }

      const sessions = (
        m as unknown as {
          sessions: Map<
            string,
            {
              transport: {
                rpc: { prompt: (...args: unknown[]) => Promise<unknown> }
              }
            }
          >
        }
      ).sessions
      const session = sessions.get('s')!

      // Turn 1: spawn childA, which completes cleanly (so childA watcher becomes !isRunning()).
      let turn = 0
      t.mock.method(session.transport.rpc, 'prompt', async () => {
        turn++
        if (turn === 1) {
          await writeSubagent(childA, 2, 'spawn-a')
          await writeJsonl(childA, [
            {
              step_index: 0,
              type: 'SYSTEM_MESSAGE',
              content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work A\n</SYSTEM_MESSAGE>`,
            },
            {
              step_index: 1,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Child A turn 1 done',
            },
          ])
          await writeJsonl(nativeId, [
            { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
            {
              step_index: 1,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                { id: 'spawn-a', name: 'invoke_subagent', args: { Role: 'A' } },
              ],
            },
            {
              step_index: 2,
              type: 'GENERIC',
              status: 'DONE',
              content: JSON.stringify({ conversationId: childA }),
            },
            {
              step_index: 3,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Turn 1 complete.',
            },
          ])
          return {
            stopReason: 'end_turn' as const,
            usage: { inputTokens: 50, outputTokens: 5, totalTokens: 55 },
          }
        }
        // Turn 2: re-wake idle childA (crashes in < 1000ms) AND spawn childB (stalls on retry warning -> settled.stalled = true).
        const msgDirA = join(brainRoot, childA, '.system_generated/messages')
        await writeFile(
          join(msgDirA, 'wake2.json'),
          JSON.stringify({
            id: 'wake2',
            sender: nativeId,
            recipient: childA,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: 'wake A',
            sourceMetadata: {
              tool: {
                conversationId: nativeId,
                stepIndex: 6,
                toolCall: {
                  id: 'wake-a',
                  name: 'send_message',
                  argumentsJson: '{}',
                },
              },
            },
          }),
        )
        await writeFile(
          join(msgDirA, 'read.json'),
          JSON.stringify({ init: true, wake2: true }),
        )
        await writeJsonl(childA, [
          {
            step_index: 0,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work A\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Child A turn 1 done',
          },
          {
            step_index: 2,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=wake A\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 3,
            type: 'ERROR_MESSAGE',
            content: 'Re-woken childA crashed immediately',
          },
        ])
        await writeJsonl(nativeId, [
          { step_index: 0, type: 'USER_INPUT', content: 'turn 1' },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              { id: 'spawn-a', name: 'invoke_subagent', args: { Role: 'A' } },
            ],
          },
          {
            step_index: 2,
            type: 'GENERIC',
            status: 'DONE',
            content: JSON.stringify({ conversationId: childA }),
          },
          {
            step_index: 3,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Turn 1 complete.',
          },
          { step_index: 4, type: 'USER_INPUT', content: 'turn 2' },
          {
            step_index: 5,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              {
                id: 'wake-a',
                name: 'send_message',
                args: { Recipient: childA, Message: 'wake A' },
              },
            ],
          },
          {
            step_index: 6,
            type: 'GENERIC',
            status: 'DONE',
            content: 'Message sent',
          },
          {
            step_index: 7,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Waiting on woken childA.',
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
        }
      })

      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'p1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'turn 1' }],
          },
        },
      })
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'p2',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'turn 2' }],
          },
        },
      })

      const childATurns = events.flatMap((e) =>
        e.type === 'session.turn' && e.sessionId === `s:child:${childA}`
          ? [`${e.turnId}:${e.state}`]
          : [],
      )
      assert.deepEqual(childATurns, [
        `s:child:${childA}:turn:1:started`,
        `s:child:${childA}:turn:1:completed`,
        `s:child:${childA}:turn:2:started`,
        `s:child:${childA}:turn:2:failed`,
      ])

      // Turn 3: spawn childB which expires on retry warning (settled.stalled === true); flushActiveChildren must mark childB failed before interrupt relaunches.
      t.mock.method(session.transport.rpc, 'prompt', async () => {
        await writeSubagent(childB, 10, 'spawn-b')
        await writeJsonl(childB, [
          {
            step_index: 0,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} content=work B\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 1,
            type: 'ERROR_MESSAGE',
            status: 'DONE',
            content:
              'Error: The stream was interrupted. Please continue the task you were working on.',
          },
        ])
        await writeJsonl(nativeId, [
          { step_index: 8, type: 'USER_INPUT', content: 'turn 3' },
          {
            step_index: 9,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              { id: 'spawn-b', name: 'invoke_subagent', args: { Role: 'B' } },
            ],
          },
          {
            step_index: 10,
            type: 'GENERIC',
            status: 'DONE',
            content: JSON.stringify({ conversationId: childB }),
          },
          {
            step_index: 11,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Waiting on childB.',
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 150, outputTokens: 15, totalTokens: 165 },
        }
      })

      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'p3',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'turn 3' }],
          },
        },
      })

      const childBTurns = events.flatMap((e) =>
        e.type === 'session.turn' && e.sessionId === `s:child:${childB}`
          ? [`${e.turnId}:${e.state}`]
          : [],
      )
      assert.deepEqual(childBTurns, [
        `s:child:${childB}:turn:1:started`,
        `s:child:${childB}:turn:1:failed`,
      ])
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test(
  'parent turn failure after child already completed flushes child without interrupting or relaunching runtime',
  { timeout: 10_000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-finished-child-fail-'))
    const logPath = join(home, 'agy.log')
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: {
          HOME: home,
          PATH: process.env.PATH ?? '',
          AGY_TEST_LOG: logPath,
          AGY_TEST_LOG_LOAD: '1',
        },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'session.subsession'],
      retryTimeoutMs: 40,
      emit: (event) => events.push(structuredClone(event)),
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
      const opened = events.find(
        (e) => e.type === 'session.opened' && e.sessionId === 's',
      )
      assert.ok(opened?.type === 'session.opened')
      const nativeId = (opened.persistence!.data as { sessionId: string })
        .sessionId
      const brainRoot = join(home, '.gemini/antigravity-cli/brain')
      const childDone = 'eeeeeeee-1111-4111-8111-111111111111'

      const writeJsonl = async (id: string, rows: unknown[]) => {
        const dir = join(brainRoot, id, '.system_generated/logs')
        await mkdir(dir, { recursive: true })
        await writeFile(
          join(dir, 'transcript_full.jsonl'),
          rows.map((r) => JSON.stringify(r) + '\n').join(''),
        )
      }

      const sessions = (
        m as unknown as {
          sessions: Map<
            string,
            {
              interrupt: (mode: 'cancel' | 'relaunch') => Promise<void>
              transport: {
                rpc: { prompt: (...args: unknown[]) => Promise<unknown> }
              }
            }
          >
        }
      ).sessions
      const session = sessions.get('s')!
      const interruptMock = t.mock.method(session, 'interrupt')

      t.mock.method(session.transport.rpc, 'prompt', async () => {
        const subDir = join(brainRoot, nativeId, '.system_generated/subagents')
        await mkdir(subDir, { recursive: true })
        await writeFile(
          join(subDir, `${childDone}.json`),
          JSON.stringify({
            conversationId: childDone,
            spawnStepIndex: 2,
            state: 'SUBAGENT_STATE_ALIVE',
          }),
        )
        const msgDir = join(brainRoot, childDone, '.system_generated/messages')
        await mkdir(msgDir, { recursive: true })
        await writeFile(
          join(msgDir, 'init.json'),
          JSON.stringify({
            id: 'init',
            sender: nativeId,
            recipient: childDone,
            priority: 'MESSAGE_PRIORITY_HIGH',
            content: 'finish quickly',
            sourceMetadata: {
              tool: {
                conversationId: nativeId,
                stepIndex: 2,
                toolCall: {
                  id: 'c-done',
                  name: 'invoke_subagent',
                  argumentsJson: '{}',
                },
              },
            },
          }),
        )
        await writeFile(
          join(msgDir, 'read.json'),
          JSON.stringify({ init: true }),
        )
        await writeJsonl(childDone, [
          {
            step_index: 0,
            type: 'SYSTEM_MESSAGE',
            content: `<SYSTEM_MESSAGE>\n[Message] sender=${nativeId} priority=MESSAGE_PRIORITY_HIGH content=finish quickly\n</SYSTEM_MESSAGE>`,
          },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'Done.',
          },
        ])
        await writeJsonl(nativeId, [
          { step_index: 0, type: 'USER_INPUT', content: 'go' },
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              { id: 'c-done', name: 'invoke_subagent', args: { Role: 'Done' } },
            ],
          },
          {
            step_index: 2,
            type: 'GENERIC',
            status: 'DONE',
            content: JSON.stringify({ conversationId: childDone }),
          },
          {
            step_index: 3,
            type: 'ERROR_MESSAGE',
            status: 'ERROR',
            source: 'MODEL',
            error: 'Parent fatal model error after child finished',
          },
        ])
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 80, outputTokens: 10, totalTokens: 90 },
        }
      })

      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'p1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'go' }],
          },
        },
      })

      const childTurns = events.flatMap((e) =>
        e.type === 'session.turn' && e.sessionId === `s:child:${childDone}`
          ? [`${e.turnId}:${e.state}`]
          : [],
      )
      assert.deepEqual(childTurns, [
        `s:child:${childDone}:turn:1:started`,
        `s:child:${childDone}:turn:1:completed`,
      ])
      assert.equal(
        interruptMock.mock.callCount(),
        0,
        'failed turn with only an already-completed child must not interrupt or relaunch the runtime',
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)
