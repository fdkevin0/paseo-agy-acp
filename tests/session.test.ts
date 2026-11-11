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
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import { readSubagents } from '../server/brain.ts'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'

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
