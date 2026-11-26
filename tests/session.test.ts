import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
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
import { DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderEvent,
  type ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { readSubagents } from '../server/brain-io.ts'
import { createProvider } from '../server/provider.ts'
import { SessionManager } from '../server/session.ts'
import { SubagentRegistry } from '../server/subagents.ts'
import { connectAcp } from '../server/transport.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  collectProviderEvents,
  compactionEvents,
  createEventCollector,
  createSignalGate,
  flushObs,
  HarnessSessionManager,
  isolatedEnv,
  sequenceDouble,
  TrackingObservation,
  wrapConnectAcp,
  writeBrainTranscript,
  type PromptHandler,
  type RpcPrompt,
} from './helpers.ts'

function completedTurnStates(events: readonly ProviderEvent[]) {
  return events
    .filter(
      (
        event,
      ): event is Extract<ProviderEvent, { type: 'session.turn' }> & {
        state: 'completed' | 'canceled' | 'failed'
      } => event.type === 'session.turn' && event.state !== 'started',
    )
    .map((event) => event.state)
}

function childTurnKeys(events: readonly ProviderEvent[], sessionId: string) {
  return events
    .filter(
      (event): event is Extract<ProviderEvent, { type: 'session.turn' }> =>
        event.type === 'session.turn' && event.sessionId === sessionId,
    )
    .map((event) => `${event.turnId}:${event.state}`)
}

function onceOnEvent(
  predicate: (event: ProviderEvent) => boolean,
  action: () => void,
): (event: ProviderEvent) => void {
  let fired = false
  return (event) => {
    if (!fired && predicate(event)) {
      fired = true
      action()
    }
  }
}

test(
  'protocol disconnection kills an active child and shutdown waits for it',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-disconnect-'))
    const pidFile = join(home, 'child.pid')
    const collector = createEventCollector()
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: isolatedEnv(home, { AGY_TEST_PID_FILE: pidFile }),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message'],
      emit: collector.emit,
    })
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
        },
      })
      const pid = Number((await readFile(pidFile, 'utf8')).trim())
      assert.ok(Number.isInteger(pid) && pid > 0)
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'disconnect',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'disconnect' }],
          },
        },
      })
      assert.equal(
        collector.events.some(
          (e) => e.type === 'session.runtime_failed' && e.sessionId === 's',
        ),
        true,
      )
      assert.equal(
        collector.events.some(
          (e) =>
            e.type === 'session.turn' &&
            e.sessionId === 's' &&
            e.state === 'failed',
        ),
        true,
      )
      await m.close()
      assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test(
  'stall failure interrupts the native session and rejects between-turn permissions, while ERROR_MESSAGE failure does not interrupt',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-session-stall-'))
    const collector = createEventCollector()
    const events = collector.events
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: isolatedEnv(home),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message', 'permission', 'session.configure'],
      emit: collector.emit,
      stallTimeoutMs: 30,
      backgroundProbeTimeoutMs: 2000,
    })
    const waitTurn = (state: 'completed' | 'failed' | 'canceled') =>
      collector.wait(
        (e) => e.type === 'session.turn' && e.state === state,
        2000,
      )
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

async function assertBlockedSubagentLink(opts: {
  capabilities: readonly string[]
  unavailable: boolean
}) {
  const home = await mkdtemp(join(tmpdir(), 'agy-blocked-link-'))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const events: ProviderEvent[] = []
  const registry = new SubagentRegistry({
    sessionId: 's',
    cwd: home,
    brainRoot,
    capabilities: opts.capabilities,
    emit: (e) => events.push(structuredClone(e)),
    getNativeId: () => '11111111-1111-4111-8111-111111111111',
    isParentTurnActive: () => false,
    isLoading: () => false,
    isUnavailable: () => opts.unavailable,
  })
  try {
    const item: ProviderTimelineItem = {
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
    }
    registry.linkSubsession('s', '11111111-1111-4111-8111-111111111111', item)
    assert.equal(events.length, 0)
    assert.ok(item.type === 'tool_call' && item.detail.type === 'sub_agent')
    assert.equal(item.detail.childSessionId, undefined)
  } finally {
    registry.closeAll()
    await rm(home, { recursive: true, force: true })
  }
}

test('subagent items clear native IDs when linking is blocked by unavailable session', async () => {
  await assertBlockedSubagentLink({
    capabilities: ['session.subsession'],
    unavailable: true,
  })
})

test('subagent items clear native IDs when linking is blocked by capability', async () => {
  await assertBlockedSubagentLink({
    capabilities: [],
    unavailable: false,
  })
})

async function assertRefreshBoundaryStripsNativeId(
  terminalStatus: 'canceled' | 'failed',
  triggerUnavailable: (ctx: {
    m: SessionManager
    pidFile: string
    collector: ReturnType<typeof createEventCollector>
  }) => Promise<void>,
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-refresh-boundary-'))
  const pidFile = join(home, 'child.pid')
  const collector = createEventCollector()
  let markRefreshEntered!: () => void
  const refreshEntered = new Promise<void>((resolve) => {
    markRefreshEntered = resolve
  })
  let releaseRefresh!: () => void
  const refreshGate = new Promise<void>((resolve) => {
    releaseRefresh = resolve
  })
  const reads = [
    async () => [],
    async () => {
      markRefreshEntered()
      await refreshGate
      return []
    },
  ]
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home, { AGY_TEST_PID_FILE: pidFile }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message', 'session.subsession'],
    emit: collector.emit,
    readSubagents: () => reads.shift()!(),
  })
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
      },
    })
    const running = m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'refresh-boundary',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'message-refresh-boundary' }],
        },
      },
    })
    await refreshEntered
    await triggerUnavailable({ m, pidFile, collector })
    releaseRefresh()
    await running
    assert.equal(
      collector.events.some(
        (e) => e.type === 'session.opened' && e.parentSessionId !== undefined,
      ),
      false,
    )
    const toolEvents = collector.events.filter(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'message-refresh',
    )
    assert.equal(toolEvents.length, 1)
    const emitted = toolEvents[0]
    assert.ok(
      emitted?.type === 'timeline.item' &&
        emitted.item.type === 'tool_call' &&
        emitted.item.detail.type === 'sub_agent',
    )
    assert.equal(emitted.item.detail.childSessionId, undefined)
    assert.equal(emitted.item.status, terminalStatus)
  } finally {
    releaseRefresh()
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
}

test(
  'terminalizing during ownership refresh never exposes native IDs: closing',
  { timeout: 5000 },
  async () => {
    await assertRefreshBoundaryStripsNativeId('canceled', async ({ m }) => {
      await m.handle({
        type: 'session.close',
        requestId: 'close',
        sessionId: 's',
      })
    })
  },
)

test(
  'terminalizing during ownership refresh never exposes native IDs: failed',
  { timeout: 5000 },
  async () => {
    await assertRefreshBoundaryStripsNativeId(
      'failed',
      async ({ pidFile, collector }) => {
        const pid = Number((await readFile(pidFile, 'utf8')).trim())
        assert.ok(Number.isInteger(pid) && pid > 0)
        process.kill(pid, 'SIGKILL')
        await collector.wait(
          (e) => e.type === 'session.runtime_failed' && e.sessionId === 's',
          2000,
        )
      },
    )
  },
)

test(
  'turn teardown rejects re-entrant prompt on permission_resolved and allows immediate prompt on terminal session.turn',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-teardown-reentry-'))
    let m!: SessionManager
    let reentrantOnPermission: Promise<unknown> | undefined
    let followUpOnTurnEnd: Promise<unknown> | undefined
    const promptNext = (clientMessageId: string) =>
      m
        .handle({
          type: 'session.prompt',
          sessionId: 's',
          prompt: {
            clientMessageId,
            delivery: 'auto',
            input: {
              type: 'message',
              content: [{ type: 'text', text: 'cancel-only' }],
            },
          },
        })
        .then(
          () => 'ok',
          (err: unknown) => err,
        )
    const bus = new EventEmitter()
    const turnActions = [
      () => {},
      () => {
        followUpOnTurnEnd = promptNext('followup-turn')
      },
      () => {},
      () => {},
    ]
    bus.once('session.permission_resolved', () => {
      reentrantOnPermission = promptNext('reentrant-perm')
    })
    bus.on('session.turn', () => {
      turnActions.shift()!()
    })
    const collector = createEventCollector((event) => {
      bus.emit(event.type, event)
    })
    m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: isolatedEnv(home),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message', 'permission'],
      emit: collector.emit,
    })
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
        },
      })
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'dangling-turn',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'dangling-permission' }],
          },
        },
      })
      assert.ok(reentrantOnPermission)
      const reentrantError = await reentrantOnPermission
      assert.ok(reentrantError instanceof Error)
      assert.match(reentrantError.message, /Session already has an active turn/)
      assert.ok(followUpOnTurnEnd)
      assert.equal(await followUpOnTurnEnd, 'ok')
      assert.deepEqual(
        collector.events
          .filter(
            (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
              e.type === 'session.turn',
          )
          .map((e) => e.state),
        ['started', 'completed', 'started', 'canceled'],
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

test(
  'synchronous session.close during terminalize timeline.item still emits terminal session.turn',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-close-in-terminalize-'))
    let m!: SessionManager
    let closing: Promise<void> | undefined
    const bus = new EventEmitter()
    const timelineActions = [
      () => {},
      () => {},
      () => {
        closing = m.handle({
          type: 'session.close',
          requestId: 'close',
          sessionId: 's',
        })
      },
    ]
    bus.on('timeline.item', () => {
      timelineActions.shift()!()
    })
    const collector = createEventCollector((event) => {
      bus.emit(event.type, event)
    })
    m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: isolatedEnv(home),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message'],
      emit: collector.emit,
    })
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
        },
      })
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'dangling-tool-turn',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'dangling-tool' }],
          },
        },
      })
      assert.ok(closing)
      await closing
      const turnStates = collector.events
        .filter(
          (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
            e.type === 'session.turn',
        )
        .map((e) => e.state)
      assert.deepEqual(turnStates, ['started', 'completed'])
      const completedIdx = collector.events.findIndex(
        (e) => e.type === 'session.turn' && e.state === 'completed',
      )
      const closedIdx = collector.events.findIndex(
        (e) => e.type === 'session.closed',
      )
      assert.ok(completedIdx >= 0 && closedIdx > completedIdx)
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

async function verifyGrandchildSyncKilled(
  setupExtra: (
    brainRoot: string,
    child: string,
    grand: string,
  ) => Promise<void>,
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-grandchild-cancel-'))
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const events: ProviderEvent[] = []
  const root = '11111111-1111-4111-8111-111111111111'
  const child = '22222222-2222-4222-8222-222222222222'
  const grand = '33333333-3333-4333-8333-333333333333'
  const logs = join(brainRoot, child, '.system_generated/logs')
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
  await setupExtra(brainRoot, child, grand)
  const grandLogs = join(brainRoot, grand, '.system_generated/logs')
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
  const registry = new SubagentRegistry({
    sessionId: 's',
    cwd: home,
    brainRoot,
    capabilities: ['session.subsession'],
    emit: (e) => events.push(structuredClone(e)),
    getNativeId: () => root,
    isParentTurnActive: () => false,
    isLoading: () => false,
    isUnavailable: () => false,
  })
  try {
    registry.linkSubsession('s', root, {
      type: 'tool_call',
      id: 'spawn-child',
      callId: 'spawn-child',
      name: 'invoke_subagent',
      status: 'completed',
      error: null,
      detail: { type: 'sub_agent', log: '', childSessionId: child },
    })
    await registry.waitNestedReady()
    assert.equal(registry.hasChild(grand), true)
    registry.rememberSubagents(
      [
        {
          conversationId: grand,
          state: 'SUBAGENT_STATE_KILLED',
          spawnStepIndex: 1,
        },
      ],
      child,
    )
    await registry.syncKilledChildren([])
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
    assert.equal(registry.isChildResumable(grand), false)
  } finally {
    registry.closeAll()
    await rm(home, { recursive: true, force: true })
  }
}

test('syncKilledChildren waits for grandchild history and cancellation (killed at spawn: false)', async () => {
  await verifyGrandchildSyncKilled(async () => {})
})

test('syncKilledChildren waits for grandchild history and cancellation (killed at spawn: true)', async () => {
  await verifyGrandchildSyncKilled(async (brainRoot, child, grand) => {
    const descriptors = join(brainRoot, child, '.system_generated/subagents')
    await mkdir(descriptors, { recursive: true })
    await writeFile(
      join(descriptors, `${grand}.json`),
      JSON.stringify({
        conversationId: grand,
        state: 'SUBAGENT_STATE_KILLED',
      }),
    )
  })
})

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
      env: isolatedEnv(home),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message', 'session.subsession'],
    emit: (e) => events.push(structuredClone(e)),
  })
  async function message(clientMessageId: string) {
    await m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'message-refresh-boundary' }],
        },
      },
    })
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
    await m.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      persistence: { version: 1, data: { sessionId: root } },
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    assert.equal(await message('msg-1'), undefined)
    assert.equal(events.filter((e) => e.type === 'session.opened').length, 1)
    await rm(join(system, 'subagents'))
    await mkdir(join(system, 'subagents'))
    await writeFile(
      join(system, 'subagents', `${child}.json`),
      JSON.stringify({ conversationId: child }),
    )
    assert.equal(await message('msg-2'), `s:child:${child}`)
    assert.equal(
      events.filter(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.status === 'running',
      ).length,
      2,
    )
    assert.equal(
      events.filter(
        (e) =>
          e.type === 'session.opened' && e.sessionId === `s:child:${child}`,
      ).length,
      1,
    )
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('history:skip coalesces restored context usage before ready', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-skip-usage-config-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home, { AGY_TEST_LOAD_USAGE: '1' }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: [],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.handle({
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
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('interrupt rejects after runtime_failed without reviving or spawning a replacement runtime', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interrupt-failed-'))
  const log = join(home, 'starts.log')
  const fixture = join(home, 'fixture-crash.mjs')
  const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk/unstable')
  await writeFile(
    fixture,
    `
import { agent, methods, ndJsonStream } from ${JSON.stringify(sdkUrl)}
import { appendFileSync } from 'node:fs'
import { Readable, Writable } from 'node:stream'

appendFileSync(process.env.AGY_TEST_LOG, 'start\\n')
const nativeId = '11111111-1111-4111-8111-111111111111'
const stream = ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin))
agent()
  .onRequest(methods.agent.initialize, async () => ({
    protocolVersion: 1,
    agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
    agentCapabilities: { loadSession: true },
  }))
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async ({ client }) => {
    setImmediate(() => {
      void client.notify(methods.client.session.update, {
        sessionId: nativeId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { sessionId: nativeId, configOptions: [] }
  })
  .onRequest(methods.agent.session.load, async () => {
    appendFileSync(process.env.AGY_TEST_LOG, 'load\\n')
    return { configOptions: [] }
  })
  .onNotification(methods.agent.session.cancel, async () => {})
  .onRequest(methods.agent.session.prompt, async () => {
    process.stderr.write('fatal runtime crash\\n')
    process.exit(3)
  })
  .connect(stream)
`,
  )
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fixture],
      env: isolatedEnv(home, { AGY_TEST_LOG: log }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.handle({
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
    const msgDir = join(
      home,
      '.gemini',
      'antigravity-cli',
      'brain',
      '11111111-1111-4111-8111-111111111111',
      '.system_generated',
      'messages',
    )
    await mkdir(msgDir, { recursive: true })
    await writeFile(
      join(msgDir, 'msg-1.json'),
      JSON.stringify({
        id: 'msg-1',
        sender: '22222222-2222-4222-8222-222222222222',
        priority: 'MESSAGE_PRIORITY_HIGH',
        content: 'wake',
      }),
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
  const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk/unstable')
  await writeFile(
    fixture,
    `
import { agent, methods, ndJsonStream } from ${JSON.stringify(sdkUrl)}
import { appendFileSync, readFileSync } from 'node:fs'
import { Readable, Writable } from 'node:stream'

appendFileSync(process.env.AGY_TEST_LOG, 'start\\n')
const starts = readFileSync(process.env.AGY_TEST_LOG, 'utf8').trim().split('\\n').filter((l) => l === 'start').length
const nativeId = '11111111-1111-4111-8111-111111111111'
const configOptions = [
  {
    id: 'model',
    name: 'Model',
    category: 'model',
    type: 'select',
    currentValue: 'fast',
    options: [
      { value: 'fast', name: 'Fast' },
      { value: 'pro', name: 'Pro' },
    ],
  },
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
agent()
  .onRequest(methods.agent.initialize, async () => ({
    protocolVersion: 1,
    agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
    agentCapabilities: { loadSession: true },
  }))
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async ({ client }) => {
    setImmediate(() => {
      void client.notify(methods.client.session.update, {
        sessionId: nativeId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { sessionId: nativeId, configOptions }
  })
  .onRequest(methods.agent.session.load, async () => {
    appendFileSync(process.env.AGY_TEST_LOG, 'load\\n')
    return { configOptions }
  })
  .onRequest(methods.agent.session.setConfigOption, async ({ params: { configId, value } }) => {
    if (starts >= 2) {
      process.stderr.write('relaunch tail crash\\n')
      process.exit(4)
    }
    return {
      configOptions: configOptions.map((o) =>
        o.id === configId ? { ...o, currentValue: String(value) } : o,
      ),
    }
  })
  .onNotification(methods.agent.session.cancel, async () => {})
  .onRequest(methods.agent.session.prompt, async () => ({ stopReason: 'end_turn' }))
  .connect(stream)
`,
  )
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fixture],
      env: isolatedEnv(home, { AGY_TEST_LOG: log }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  try {
    await m.handle({
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
        model: 'pro',
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
        env: isolatedEnv(home),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message'],
      emit: (event) => events.push(event),
      stallTimeoutMs: 30,
      retryTimeoutMs: 50,
    })
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
      await m.handle({
        type: 'session.configure',
        requestId: 'sync-reload',
        sessionId: 's',
        changes: {},
      })
      await m.close()
      const log = (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      const starts = log.filter((row) => row.event === 'start')
      assert.equal(
        starts.length,
        2,
        'active background work requires stopping and relaunching its native owner',
      )
      assert.throws(() => process.kill(starts[0].pid, 0), { code: 'ESRCH' })
      assert.ok(log.some((row) => row.event === 'cancel'))
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
    const gate = await createSignalGate(home, 'foreground-retry.sock')
    const events: ProviderEvent[] = []
    let currentObs: TrackingObservation | undefined
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: isolatedEnv(home, { AGY_RELOAD_GATE: gate.socketPath }),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message'],
      emit: (event) => events.push(event),
      stallTimeoutMs: 10,
      retryTimeoutMs: 20,
      observationPollIntervalMs: 60_000,
      createObservation: (...args) => {
        currentObs = new TrackingObservation(...args)
        return currentObs
      },
    })
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
        },
      })
      const firstTurn = m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'retry-foreground-active',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'retry-foreground-active' }],
          },
        },
      })
      assert.equal(await gate.entered, 'held-foreground')
      // Poll the expired 2020 retry warning while the foreground RPC is still in flight.
      assert.ok(currentObs)
      await flushObs(currentObs)
      gate.release()
      await firstTurn
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'normal next turn',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'normal next turn' }],
          },
        },
      })
      assert.deepEqual(completedTurnStates(events), ['completed', 'completed'])
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
      await gate.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

async function promptAndAssertUsageSnapshot(
  m: SessionManager,
  events: ProviderEvent[],
  clientMessageId: string,
  text: string,
  expected: {
    contextWindowUsedTokens: number
    contextWindowMaxTokens?: number
    turnState: 'completed' | 'canceled' | 'failed'
  },
) {
  events.length = 0
  await m.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId,
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text }] },
    },
  })
  const usage = events.findLast((e) => e.type === 'session.usage')
  assert.ok(usage?.type === 'session.usage')
  assert.equal(
    usage.usage.contextWindowUsedTokens,
    expected.contextWindowUsedTokens,
  )
  assert.equal(
    usage.usage.contextWindowMaxTokens,
    expected.contextWindowMaxTokens,
  )
  assert.equal(usage.usage.inputTokens, 600000)
  assert.equal(usage.usage.outputTokens, 4405)
  assert.ok(
    events.some(
      (e) => e.type === 'session.turn' && e.state === expected.turnState,
    ),
  )
}

test('context uses latest turn snapshot on cancel/error/restore and preserves native updates', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-snapshot-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
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
    await m.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config,
    })
    const openedEvent = events.find((e) => e.type === 'session.opened')
    assert.ok(openedEvent?.type === 'session.opened')
    const persistence = openedEvent.persistence!
    await promptAndAssertUsageSnapshot(
      m,
      events,
      'usage-cancel:1',
      'usage-cancel',
      {
        contextWindowUsedTokens: 70463,
        turnState: 'canceled',
      },
    )
    await promptAndAssertUsageSnapshot(
      m,
      events,
      'usage-empty:2',
      'usage-empty',
      {
        contextWindowUsedTokens: 70463,
        turnState: 'completed',
      },
    )
    await promptAndAssertUsageSnapshot(
      m,
      events,
      'usage-error:3',
      'usage-error',
      {
        contextWindowUsedTokens: 70463,
        turnState: 'failed',
      },
    )
    await promptAndAssertUsageSnapshot(
      m,
      events,
      'usage-native:4',
      'usage-native',
      {
        contextWindowUsedTokens: 1234,
        contextWindowMaxTokens: 128000,
        turnState: 'completed',
      },
    )
    await promptAndAssertUsageSnapshot(
      m,
      events,
      'usage-empty:5',
      'usage-empty',
      {
        contextWindowUsedTokens: 1234,
        contextWindowMaxTokens: 128000,
        turnState: 'completed',
      },
    )
    await m.handle({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    events.length = 0
    await m.handle({
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
    const native = '11111111-1111-4111-8111-111111111111'
    const dir = join(
      home,
      '.gemini/antigravity-cli/brain',
      native,
      '.system_generated/logs',
    )
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
    const transcriptPath = join(dir, 'transcript_full.jsonl')
    await writeFile(
      transcriptPath,
      rows.map((r) => JSON.stringify(r) + '\n').join('') + '{"step_index":2',
    )
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
        env: isolatedEnv(home),
      },
      usage: new AgyUsageSource({ env: isolatedEnv(home) }),
      capabilities: ['prompt.message'],
      emit: (e) => events.push(structuredClone(e)),
    })
    try {
      await m.handle({
        type: 'session.open',
        requestId: 'open',
        sessionId: 's',
        history: 'replay',
        persistence: { version: 1, data: { sessionId: native } },
        config: {
          cwd: home,
          env: { AGY_TEST_CHECKPOINT_HISTORY: '1' },
          mcpServers: {},
          settings: {},
          persist: true,
        },
      })
      const toolEvents = events.filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'tool_call',
      )
      assert.equal(toolEvents.length, 1)
      const toolEvent = toolEvents[0]
      assert.ok(
        toolEvent?.type === 'timeline.item' &&
          toolEvent.item.type === 'tool_call',
      )
      assert.deepEqual(
        {
          callId: toolEvent.item.callId,
          status: toolEvent.item.status,
          detail: toolEvent.item.detail,
        },
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
      )
      await writeFile(
        transcriptPath,
        [
          rows[0],
          {
            step_index: 1,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            content: 'old',
            input_tokens: 999,
            output_tokens: 1,
          },
        ]
          .map((r) => JSON.stringify(r) + '\n')
          .join(''),
      )
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'after-open',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'cancel-only' }],
          },
        },
      })
      assert.equal(events.filter((e) => e.type === 'session.usage').length, 0)
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)

async function runCompactionPrompt(
  m: SessionManager,
  events: ProviderEvent[],
  clientMessageId: string,
  input:
    | { type: 'command'; name: string; arguments: string }
    | { type: 'message'; content: [{ type: 'text'; text: string }] },
  expectedDelta: number,
) {
  const before = compactionEvents(events).length
  await m.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId,
      delivery: 'auto',
      input,
    },
  })
  assert.equal(compactionEvents(events).length - before, expectedDelta)
  const usage = events.findLast((event) => event.type === 'session.usage')
  assert.ok(usage?.type === 'session.usage')
  assert.equal(usage.usage.contextWindowUsedTokens, 12)
  assert.equal(usage.usage.inputTokens, 500)
  assert.equal(usage.usage.outputTokens, 50)
}

async function assertReopenedCompactions(
  m: SessionManager,
  events: ProviderEvent[],
  persistence: NonNullable<
    Extract<ProviderEvent, { type: 'session.opened' }>['persistence']
  >,
  config: {
    cwd: string
    env: Record<string, string>
    mcpServers: Record<string, never>
    settings: Record<string, never>
    persist: boolean
  },
  history: 'replay' | 'skip',
  expectedIds: readonly string[],
) {
  events.length = 0
  await m.handle({
    type: 'session.open',
    requestId: `open:${history}`,
    sessionId: 's',
    history,
    persistence,
    config,
  })
  const replayed = compactionEvents(events)
  assert.deepEqual(
    replayed.map((event) => event.item.id),
    expectedIds,
  )
  assert.deepEqual(
    replayed.map((event) => ({
      sessionId: event.sessionId,
      timestamp: event.timestamp,
      item: event.item,
    })),
    expectedIds.map((id) => ({
      sessionId: 's',
      timestamp: '2026-01-01T00:00:00Z',
      item: {
        type: 'compaction',
        id,
        status: 'completed',
        preTokens: 268_740,
      },
    })),
  )
  const usage = events.findLast((event) => event.type === 'session.usage')
  assert.ok(usage?.type === 'session.usage')
  assert.equal(usage.usage.contextWindowUsedTokens, 12)
  await m.handle({
    type: 'session.close',
    requestId: `close:${history}`,
    sessionId: 's',
  })
}

test('native compaction has stable live/history identity, command-only visibility and no invented canceled completion or usage', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-compaction-session-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home, { AGY_TEST_CHECKPOINT_HISTORY: '1' }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
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
  try {
    await m.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config,
    })
    const opened = events.find((event) => event.type === 'session.opened')
    assert.ok(opened?.type === 'session.opened')
    const persistence = opened.persistence!
    await runCompactionPrompt(
      m,
      events,
      'checkpoint-turn',
      { type: 'message', content: [{ type: 'text', text: 'checkpoint-turn' }] },
      1,
    )
    await runCompactionPrompt(
      m,
      events,
      '/compact',
      { type: 'command', name: 'compact', arguments: '' },
      1,
    )
    await runCompactionPrompt(
      m,
      events,
      'checkpoint-cancel',
      {
        type: 'message',
        content: [{ type: 'text', text: 'checkpoint-cancel' }],
      },
      0,
    )
    const liveIds = compactionEvents(events).map((event) => event.item.id)
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
    await assertReopenedCompactions(
      m,
      events,
      persistence,
      config,
      'replay',
      liveIds,
    )
    await assertReopenedCompactions(m, events, persistence, config, 'skip', [])
    await assertReopenedCompactions(
      m,
      events,
      persistence,
      config,
      'replay',
      liveIds,
    )
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
    const sdkUrl = import.meta.resolve('@agentclientprotocol/sdk/unstable')
    await writeFile(
      fixturePath,
      `
import { agent, methods, ndJsonStream } from ${JSON.stringify(sdkUrl)}
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

agent()
  .onRequest(methods.agent.initialize, async () => ({
    protocolVersion: 1,
    agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
    agentCapabilities: { loadSession: true },
  }))
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async ({ client }) => {
    const sessionId = '11111111-1111-4111-8111-111111111111'
    setImmediate(() => {
      void client.notify(methods.client.session.update, {
        sessionId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { sessionId, configOptions: buildConfigOptions() }
  })
  .onRequest(methods.agent.session.load, async ({ params: { sessionId }, client }) => {
    omitModelsNext = true
    setImmediate(() => {
      void client.notify(methods.client.session.update, {
        sessionId,
        update: { sessionUpdate: 'available_commands_update', availableCommands: [] },
      })
    })
    return { configOptions: buildConfigOptions() }
  })
  .onRequest(methods.agent.session.close, async () => ({}))
  .onRequest(methods.agent.session.setConfigOption, async ({ params: { configId, value } }) => {
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
  })
  .onRequest(methods.agent.session.prompt, async () => ({ stopReason: 'end_turn' }))
  .onNotification(methods.agent.session.cancel, async () => {})
  .connect(stream)
`,
    )
    const provider = await createProvider(
      new AgyUsageSource({ env: isolatedEnv(home) }),
    ).connect({
      versions: [1],
      capabilities: PROVIDER_CAPABILITIES,
      launch: {
        command: process.execPath,
        args: [fixturePath],
        env: isolatedEnv(home, { AGY_TEST_LOG: logPath }),
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

/** An opened session whose child subagents live in a synthetic `brain` tree. */
async function childSession(
  retryTimeoutMs?: number,
  onEvent?: (event: ProviderEvent) => void,
) {
  const home = await mkdtemp(join(tmpdir(), 'agy-child-session-'))
  const logPath = join(home, 'agy.log')
  const events: ProviderEvent[] = []
  let promptHandler: PromptHandler | undefined
  const m = new HarnessSessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home, {
        AGY_TEST_LOG: logPath,
        AGY_TEST_LOG_LOAD: '1',
      }),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message', 'session.subsession'],
    retryTimeoutMs,
    connectAcp: wrapConnectAcp(() => promptHandler),
    emit: (event) => {
      const cloned = structuredClone(event)
      events.push(cloned)
      onEvent?.(cloned)
    },
  })
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
    },
  })
  const opened = events.find(
    (e) => e.type === 'session.opened' && e.sessionId === 's',
  )
  assert.ok(opened?.type === 'session.opened')
  const nativeId = (opened.persistence!.data as { sessionId: string }).sessionId
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
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
    content = 'work',
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
        content,
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
    await writeFile(join(msgDir, 'read.json'), JSON.stringify({ init: true }))
  }
  const session = m.session
  const prompt = (clientMessageId: string, text: string) =>
    m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  return {
    logPath,
    prompt,
    events,
    nativeId,
    brainRoot,
    writeJsonl,
    writeSubagent,
    session,
    setPromptHandler: (handler: PromptHandler | undefined) => {
      promptHandler = handler
    },
    close: async () => {
      await m.close()
      await rm(home, { recursive: true, force: true })
    },
  }
}

test(
  'parallel child crash flushes failed child state, cancels running sibling watcher, and relaunches runtime',
  { timeout: 10_000 },
  async () => {
    const childA = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
    const childB = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
    let triggerCrash: () => void = () => {}
    let crashWrite: Promise<void> | undefined
    const {
      logPath,
      events,
      nativeId,
      prompt,
      writeJsonl,
      writeSubagent,
      session,
      setPromptHandler,
      close,
    } = await childSession(
      40,
      onceOnEvent(
        (event) =>
          event.type === 'session.turn' &&
          event.sessionId === `s:child:${childA}` &&
          event.state === 'started',
        () => triggerCrash(),
      ),
    )
    try {
      setPromptHandler(async () => {
        await writeSubagent(childA, 2, 'spawn-a')
        await writeSubagent(childB, 3, 'spawn-b')
        // Start childB running and childA initially running; once childA's watcher emits turn started,
        // write childA's fatal crash (well before its 1000ms watcher interval, exercising flushRunningChildren).
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
        triggerCrash = () => {
          crashWrite = writeJsonl(childA, [
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
        }
        return {
          stopReason: 'end_turn' as const,
          usage: { inputTokens: 120, outputTokens: 12, totalTokens: 132 },
        }
      })

      await prompt('p1', 'parallel work')

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

      await session.waitForRuntimeReady(new AbortController().signal)
      const logLines = (await readFile(logPath, 'utf8')).trim().split('\n')
      assert.ok(logLines.includes('cancel'))
      assert.ok(
        logLines.includes('load'),
        'runtime must relaunch to terminate leaked sibling subagent work',
      )
    } finally {
      triggerCrash = () => {}
      await crashWrite?.catch(() => undefined)
      await close()
    }
  },
)

test(
  're-woken idle child crash and child retry-timeout stall both flush child watcher to failed instead of canceled',
  { timeout: 10_000 },
  async () => {
    const {
      brainRoot,
      events,
      nativeId,
      prompt,
      writeJsonl,
      writeSubagent,
      setPromptHandler,
      close,
    } = await childSession(40)
    const childA = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
    const childB = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
    try {
      setPromptHandler(
        sequenceDouble([
          async () => {
            // Turn 1: spawn childA, which completes cleanly (so childA watcher becomes !isRunning()).
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
                  {
                    id: 'spawn-a',
                    name: 'invoke_subagent',
                    args: { Role: 'A' },
                  },
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
          },
          async () => {
            // Turn 2: re-wake idle childA (crashes in < 1000ms).
            const msgDirA = join(
              brainRoot,
              childA,
              '.system_generated/messages',
            )
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
                  {
                    id: 'spawn-a',
                    name: 'invoke_subagent',
                    args: { Role: 'A' },
                  },
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
          },
          async () => {
            // Turn 3: spawn childB which expires on retry warning (settled.stalled === true); flushActiveChildren must mark childB failed before interrupt relaunches.
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
                  {
                    id: 'spawn-b',
                    name: 'invoke_subagent',
                    args: { Role: 'B' },
                  },
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
          },
        ]),
      )

      await prompt('p1', 'turn 1')
      await prompt('p2', 'turn 2')

      assert.deepEqual(childTurnKeys(events, `s:child:${childA}`), [
        `s:child:${childA}:turn:1:started`,
        `s:child:${childA}:turn:1:completed`,
        `s:child:${childA}:turn:2:started`,
        `s:child:${childA}:turn:2:failed`,
      ])

      await prompt('p3', 'turn 3')

      assert.deepEqual(childTurnKeys(events, `s:child:${childB}`), [
        `s:child:${childB}:turn:1:started`,
        `s:child:${childB}:turn:1:failed`,
      ])
    } finally {
      await close()
    }
  },
)

test(
  'parent turn failure after child already completed flushes child without interrupting or relaunching runtime',
  { timeout: 10_000 },
  async () => {
    const {
      logPath,
      brainRoot,
      events,
      nativeId,
      prompt,
      writeJsonl,
      session,
      setPromptHandler,
      close,
    } = await childSession(40)
    const childDone = 'eeeeeeee-1111-4111-8111-111111111111'
    const initialTransport = session.transport
    try {
      setPromptHandler(async () => {
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

      await prompt('p1', 'go')

      assert.deepEqual(childTurnKeys(events, `s:child:${childDone}`), [
        `s:child:${childDone}:turn:1:started`,
        `s:child:${childDone}:turn:1:completed`,
      ])
      assert.deepEqual(
        (await readFile(logPath, 'utf8')).trim().split('\n'),
        ['new'],
        'failed turn with only an already-completed child must not cancel or relaunch the runtime',
      )
      assert.equal(session.transport, initialTransport)
    } finally {
      await close()
    }
  },
)

async function usageHarness(t: TestContext) {
  const home = await mkdtemp(join(tmpdir(), 'agy-session-usage-'))
  const events: ProviderEvent[] = []
  let promptHandler: PromptHandler | undefined
  const m = new HarnessSessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message', 'prompt.command', 'session.configure'],
    emit: (e) => events.push(structuredClone(e)),
    connectAcp: wrapConnectAcp(() => promptHandler),
  })
  t.after(async () => {
    await m.close()
    await rm(home, { recursive: true, force: true })
  })
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
    },
  })
  const session = m.session
  const write = (rows: Record<string, unknown>[]) =>
    writeBrainTranscript(session.brainRoot, session.state.nativeId, rows)
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
  return {
    m,
    session,
    events,
    write,
    prompt,
    setPromptHandler: (handler: PromptHandler | undefined) => {
      promptHandler = handler
    },
    latest,
  }
}

test('zero-token canceled or command turns preserve prior consumption', async (t) => {
  const h = await usageHarness(t)
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
  h.setPromptHandler(
    sequenceDouble<[], ReturnType<RpcPrompt>>([
      async () => ({ stopReason: 'end_turn' as const, usage: zeroUsage }),
      async () => ({ stopReason: 'cancelled' as const, usage: zeroUsage }),
      async () => ({
        stopReason: 'end_turn' as const,
        usage: {
          inputTokens: 120,
          outputTokens: 15,
          cachedReadTokens: 650,
          totalTokens: 785,
        },
      }),
    ]),
  )

  // A zero-token slash command does not replace prior consumption.
  await h.m.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'cmd-zero',
      delivery: 'auto',
      input: { type: 'command', name: 'status', arguments: '' },
    },
  })
  assert.equal(h.latest().inputTokens, 200)
  assert.equal(h.latest().cachedInputTokens, 500)

  // A canceled zero-token message turn also preserves prior consumption.
  await h.prompt('cancel-zero')
  assert.equal(h.latest().inputTokens, 200)

  // The next billed turn reports independent totals.
  await h.prompt('billed-next')
  assert.equal(h.latest().inputTokens, 120)
  assert.equal(h.latest().outputTokens, 15)
  assert.equal(h.latest().cachedInputTokens, 650)
})

test('runtime relaunch during a stalled turn retains that turn’s wakeup usage', async (t) => {
  const h = await usageHarness(t)
  const sender = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
  h.setPromptHandler(async () => {
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
  assert.equal(h.latest().inputTokens, 220)
  assert.equal(h.latest().outputTokens, 22)
  assert.equal(h.latest().cachedInputTokens, 35)
})

test('settlement failure still publishes positive prompt result.usage', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-post-wait-settle-error-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: isolatedEnv(home),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message'],
    emit: (e) => events.push(structuredClone(e)),
  })
  const prompt = (clientMessageId: string, text: string) =>
    m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  const latestUsage = () => {
    const event = events.findLast((e) => e.type === 'session.usage')
    assert.ok(event?.type === 'session.usage')
    return event.usage
  }
  const latestTurn = () => {
    const event = events.findLast((e) => e.type === 'session.turn')
    assert.ok(event?.type === 'session.turn')
    return event
  }
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
      },
    })
    const opened = events.find((e) => e.type === 'session.opened')
    assert.ok(opened?.type === 'session.opened' && opened.persistence)
    const nativeId = (opened.persistence.data as { sessionId: string })
      .sessionId
    await prompt('p2', 'corrupt-transcript-positive-usage')
    assert.equal(latestTurn().state, 'failed')
    assert.equal(latestUsage().inputTokens, 120)
    assert.equal(latestUsage().outputTokens, 12)

    await writeFile(
      join(
        home,
        '.gemini/antigravity-cli/brain',
        nativeId,
        '.system_generated/logs/transcript_full.jsonl',
      ),
      '',
    )
    await prompt('p3', 'clean-positive-usage')
    assert.equal(latestTurn().state, 'completed')
    assert.equal(latestUsage().inputTokens, 50)
    assert.equal(latestUsage().outputTokens, 5)
  } finally {
    await m.close()
    await rm(home, { recursive: true, force: true })
  }
})

async function createCommandWaitHarness(
  t: TestContext,
  options: {
    prefix: string
    resumeCapable?: boolean
    newSessionId?: string
  },
) {
  const home = await mkdtemp(join(tmpdir(), options.prefix))
  const events: ProviderEvent[] = []
  let activeClient!: Parameters<typeof connectAcp>[3]
  let waitForCommandsCalls = 0
  let notifyCommandWait!: () => void
  const enteredCommandWait = new Promise<void>((resolve) => {
    notifyCommandWait = resolve
  })
  const stubConnectAcp: typeof connectAcp = (_launch, _cwd, _env, client) => {
    activeClient = client
    let resolveStopped!: () => void
    const stopped = new Promise<void>((resolve) => {
      resolveStopped = resolve
    })
    let closed = false
    const rpc = {
      initialize: async () => ({
        protocolVersion: 1,
        agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
        agentCapabilities: {
          loadSession: true,
          ...(options.resumeCapable
            ? { sessionCapabilities: { resume: {} } }
            : {}),
        },
      }),
      newSession: async () => ({
        sessionId:
          options.newSessionId ?? '11111111-1111-4111-8111-111111111111',
        configOptions: [],
      }),
      loadSession: async () => ({ configOptions: [] }),
      resumeSession: async () => ({ configOptions: [] }),
      closeSession: async () => ({}),
      setSessionConfigOption: async () => ({ configOptions: [] }),
      listSessions: async () => ({ sessions: [] }),
      prompt: async () => ({ stopReason: 'end_turn' as const }),
      cancel: async () => {},
    } as unknown as ReturnType<typeof connectAcp>['rpc']
    return {
      rpc,
      exited: stopped,
      stopped,
      request: async <T>(op: Promise<T>) => op,
      close: async () => {
        closed = true
        resolveStopped()
      },
      get closed() {
        return closed
      },
      get tier() {
        return undefined
      },
      get models() {
        return undefined
      },
    }
  }
  class CommandWaitSessionManager extends SessionManager {
    override async start(...args: Parameters<SessionManager['start']>) {
      const session = await super.start(...args)
      return new Proxy(session, {
        get(target, prop, receiver) {
          if (prop === 'waitForCommands') {
            return (...waitArgs: Parameters<typeof target.waitForCommands>) => {
              waitForCommandsCalls++
              notifyCommandWait()
              return target.waitForCommands(...waitArgs)
            }
          }
          const value = Reflect.get(target, prop, receiver)
          return typeof value === 'function' ? value.bind(target) : value
        },
      })
    }
  }
  const manager = new CommandWaitSessionManager({
    launch: {
      command: 'agy',
      args: ['acp'],
      env: isolatedEnv(home),
    },
    usage: new AgyUsageSource({ env: isolatedEnv(home) }),
    capabilities: ['prompt.message', 'prompt.command', 'session.persistence'],
    emit: (event) => events.push(structuredClone(event)),
    commandWaitTimeoutMs: 60_000,
    connectAcp: stubConnectAcp,
  })
  t.after(async () => {
    await manager.close()
    await rm(home, { recursive: true, force: true })
  })
  return {
    home,
    events,
    manager,
    enteredCommandWait,
    get waitForCommandsCalls() {
      return waitForCommandsCalls
    },
    sendCommands: (
      sessionId: string,
      availableCommands: Array<{ name: string; description: string }>,
    ) =>
      activeClient.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands,
        },
      }),
  }
}

const persistedTestId = '22222222-2222-4222-8222-222222222222'

test(
  'persisted session/load waits for available_commands_update before readiness and forwards later updates',
  { timeout: 2000 },
  async (t) => {
    const h = await createCommandWaitHarness(t, {
      prefix: 'agy-wait-cmd-load-',
    })
    const opening = h.manager.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'replay',
      persistence: { version: 1, data: { sessionId: persistedTestId } },
      config: {
        cwd: h.home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await h.enteredCommandWait
    assert.equal(h.waitForCommandsCalls, 1)
    assert.equal(
      h.events.some((e) => e.type === 'session.ready'),
      false,
    )
    await h.sendCommands(persistedTestId, [
      { name: 'plan', description: 'Plan' },
    ])
    await opening
    assert.ok(
      h.events.findIndex((e) => e.type === 'session.commands') <
        h.events.findIndex((e) => e.type === 'session.ready'),
    )

    await h.sendCommands(persistedTestId, [
      { name: 'compact', description: 'Compact context' },
    ])
    const latestCommands = h.events.findLast(
      (e) => e.type === 'session.commands',
    )
    assert.deepEqual(latestCommands, {
      type: 'session.commands',
      sessionId: 's',
      commands: [
        {
          name: 'compact',
          description: 'Compact context',
          argumentHint: undefined,
        },
      ],
    })
  },
)

test(
  'persisted session/resume waits for available_commands_update before readiness',
  { timeout: 2000 },
  async (t) => {
    const h = await createCommandWaitHarness(t, {
      prefix: 'agy-wait-cmd-resume-',
      resumeCapable: true,
    })
    const opening = h.manager.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      persistence: { version: 1, data: { sessionId: persistedTestId } },
      config: {
        cwd: h.home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await h.enteredCommandWait
    assert.equal(h.waitForCommandsCalls, 1)
    assert.equal(
      h.events.some((e) => e.type === 'session.ready'),
      false,
    )
    await h.sendCommands(persistedTestId, [])
    await opening
    assert.ok(
      h.events.findIndex((e) => e.type === 'session.commands') <
        h.events.findIndex((e) => e.type === 'session.ready'),
    )
  },
)

test(
  'new session/new waits for available_commands_update before readiness',
  { timeout: 2000 },
  async (t) => {
    const newId = '33333333-3333-4333-8333-333333333333'
    const h = await createCommandWaitHarness(t, {
      prefix: 'agy-wait-cmd-new-',
      newSessionId: newId,
    })
    const opening = h.manager.handle({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      history: 'skip',
      config: {
        cwd: h.home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
    })
    await h.enteredCommandWait
    assert.equal(h.waitForCommandsCalls, 1)
    assert.equal(
      h.events.some((e) => e.type === 'session.ready'),
      false,
    )
    await h.sendCommands(newId, [])
    await opening
    assert.ok(
      h.events.findIndex((e) => e.type === 'session.commands') <
        h.events.findIndex((e) => e.type === 'session.ready'),
    )
  },
)

test('promptSession failure cleanup forwards runtimeEpoch and options.hasActiveBackgroundWork without spurious interrupt', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-failure-cleanup-epoch-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const probeCalls: Array<{
    nativeId: string
    afterStepIndex: number | undefined
  }> = []
  let cancelCount = 0
  const events: ProviderEvent[] = []
  const env = isolatedEnv(home)
  const launch = { command: 'agy', args: ['acp'], env }
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    hasActiveBackgroundWork: async (_root, id, opts) => {
      probeCalls.push({
        nativeId: id,
        afterStepIndex: opts?.runtimeEpoch?.afterStepIndex,
      })
      return false
    },
    connectAcp: (_l, _cwd, _e, client) => {
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: {
            loadSession: true,
            sessionCapabilities: { resume: {} },
          },
        }),
        resumeSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { configOptions: [] }
        },
        prompt: async () => {
          await writeBrainTranscript(brainRoot, nativeId, [
            { step_index: 1, type: 'USER_INPUT', content: 'old turn' },
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'old done',
            },
            { step_index: 3, type: 'USER_INPUT', content: 'failing turn' },
            {
              step_index: 4,
              type: 'ERROR_MESSAGE',
              status: 'DONE',
              content: 'upstream terminal failure',
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
        cancel: async () => {
          cancelCount++
        },
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  // Seed prior-epoch history before reopening so runtimeEpoch.afterStepIndex is established at step 2.
  await writeBrainTranscript(brainRoot, nativeId, [
    { step_index: 1, type: 'USER_INPUT', content: 'old turn' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'old done',
    },
  ])
  await manager.handle({
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
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'fail' }] },
    },
  })
  assert.deepEqual(probeCalls, [{ nativeId, afterStepIndex: 2 }])
  assert.equal(cancelCount, 0)
  const turnFailed = events.find(
    (e) => e.type === 'session.turn' && e.state === 'failed',
  )
  assert.ok(turnFailed?.type === 'session.turn')
  assert.equal(turnFailed.error?.message, 'upstream terminal failure')
})

test('notificationLane isolates single-notification failures and requestPermission formats tool names via toolDisplayName', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-notif-perm-name-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const events: ProviderEvent[] = []
  let acpClient!: import('../server/transport.ts').AcpClient
  const env = isolatedEnv(home)
  let onEmit: (event: ProviderEvent) => void = (event) => events.push(event)
  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => onEmit(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      acpClient = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: async () => {
          // 1. Trigger a failure on one notification; lane must not stay poisoned.
          onEmit = (event) => {
            events.push(event)
            throw new Error('transient downstream listener boom')
          }
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'poison' },
            },
          })
          onEmit = (event) => events.push(event)
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: ' recovered' },
            },
          })
          // 2. Permission request resolves MCP tool name via toolDisplayName.
          const permPromise = acpClient.requestPermission({
            sessionId: nativeId,
            toolCall: {
              toolCallId: 'call-mcp-1',
              title: 'Run default_api:mcp_docs_search',
              rawInput: { q: 'paseo' },
            },
            options: [
              { optionId: 'yes', name: 'Allow', kind: 'allow_once' },
              { optionId: 'no', name: 'Deny', kind: 'reject_once' },
            ],
          })
          const permEvent = events.findLast(
            (e): e is Extract<ProviderEvent, { type: 'session.permission' }> =>
              e.type === 'session.permission',
          )
          assert.ok(permEvent)
          assert.equal(permEvent.request.name, 'docs.search')
          await manager.handle({
            type: 'session.permission',
            sessionId: 's',
            permissionId: permEvent.request.id,
            response: { behavior: 'allow', selectedActionId: 'yes' },
          })
          assert.deepEqual(await permPromise, {
            outcome: { outcome: 'selected', optionId: 'yes' },
          })
          return { stopReason: 'end_turn' as const }
        },
        cancel: async () => {},
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: { docs: { type: 'stdio', command: 'docs-mcp' } },
      settings: {},
      persist: true,
    },
  })
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'hi' }] },
    },
  })
  const recovered = events.findLast(
    (e) => e.type === 'timeline.item' && e.item.type === 'assistant_message',
  )
  assert.ok(recovered?.type === 'timeline.item')
  assert.ok(recovered.item.type === 'assistant_message')
  assert.equal(recovered.item.text, 'poison recovered')
})

test('full-access still prompts on interaction_* permissions, surfaces agy.security.warning, and strips toolSummary from permission input', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-interaction-sec-warn-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
  const events: ProviderEvent[] = []
  let acpClient!: import('../server/transport.ts').AcpClient
  const env = isolatedEnv(home)
  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      acpClient = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return {
            sessionId: nativeId,
            configOptions: [
              {
                id: 'mode',
                name: 'Mode',
                category: 'mode',
                type: 'select',
                currentValue: 'default',
                options: [
                  { value: 'default', name: 'Default' },
                  { value: 'accept-edits', name: 'Accept Edits' },
                ],
              },
            ],
          }
        },
        setSessionConfigOption: async () => ({
          configOptions: [
            {
              id: 'mode',
              name: 'Mode',
              category: 'mode',
              type: 'select',
              currentValue: 'accept-edits',
              options: [
                { value: 'default', name: 'Default' },
                { value: 'accept-edits', name: 'Accept Edits' },
              ],
            },
          ],
        }),
        closeSession: async () => ({}),
        prompt: async () => {
          const permPromise = acpClient.requestPermission({
            sessionId: nativeId,
            toolCall: {
              toolCallId: 'interaction_confirm_delete',
              title: 'Delete production database?',
              rawInput: {
                target: 'prod',
                toolSummary: 'Delete prod DB',
              },
            },
            options: [
              {
                optionId: 'allow_once',
                name: 'Proceed',
                kind: 'allow_once',
                _meta: {
                  'agy.security.warning': {
                    title: 'Destructive action',
                    message: 'This permanently deletes data.',
                  },
                },
              },
              {
                optionId: 'reject_once',
                name: 'Cancel',
                kind: 'reject_once',
              },
            ],
          })
          const permEvent = events.findLast(
            (e): e is Extract<ProviderEvent, { type: 'session.permission' }> =>
              e.type === 'session.permission',
          )
          assert.ok(permEvent)
          assert.equal(permEvent.request.kind, 'question')
          assert.equal(
            permEvent.request.description,
            'Destructive action: This permanently deletes data.',
          )
          assert.deepEqual(permEvent.request.input, { target: 'prod' })
          assert.deepEqual(permEvent.request.actions, [
            {
              id: 'allow_once',
              label: 'Proceed',
              behavior: 'allow',
              variant: 'danger',
            },
            {
              id: 'reject_once',
              label: 'Cancel',
              behavior: 'deny',
            },
          ])
          await manager.handle({
            type: 'session.permission',
            sessionId: 's',
            permissionId: permEvent.request.id,
            response: { behavior: 'deny', selectedActionId: 'reject_once' },
          })
          assert.deepEqual(await permPromise, {
            outcome: { outcome: 'selected', optionId: 'reject_once' },
          })
          return { stopReason: 'end_turn' as const }
        },
        cancel: async () => {},
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  await manager.handle({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mode: 'full-access',
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'delete' }] },
    },
  })
})

test('promptSession classifies stopReason refusal and hidden SQLite step_type 17 API errors as failed turns', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-hidden-sql-error-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'
  const convDir = join(home, '.gemini/antigravity-cli/conversations')
  await mkdir(convDir, { recursive: true })
  const dbPath = join(convDir, `${nativeId}.db`)
  const db = new DatabaseSync(dbPath)
  t.after(() => db.close())
  db.exec(
    'CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL, step_payload BLOB)',
  )
  const insertStep = db.prepare(
    'INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)',
  )
  const events: ProviderEvent[] = []
  const env = isolatedEnv(home)
  const promptSteps = [
    async () => {
      insertStep.run(
        1,
        17,
        Buffer.from(
          'reason:QUOTA_EXHAUSTED quotaResetDelay=2m30s quotaResetTimeStamp=2026-11-23T05:00:00Z',
          'utf8',
        ),
      )
      return { stopReason: 'refusal' as const }
    },
    async () => {
      insertStep.run(
        2,
        17,
        Buffer.from(
          'messages.4.content.0.thinking.signature: Field required',
          'utf8',
        ),
      )
      return { stopReason: 'end_turn' as const }
    },
  ]
  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: () => promptSteps.shift()!(),
        cancel: async () => {},
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  await manager.handle({
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

  // Turn 1: stopReason 'refusal' + QUOTA_EXHAUSTED step_type 17 fails with reset info.
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-refusal',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'one' }] },
    },
  })
  const firstFailed = events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
      e.type === 'session.turn' && e.state === 'failed',
  )
  assert.ok(firstFailed)
  assert.match(firstFailed.error?.message ?? '', /quota|rate/i)
  assert.match(firstFailed.error?.message ?? '', /2026-11-23T05:00:00Z/)

  // Turn 2: stopReason 'end_turn' + hidden step_type 17 INVALID_THINKING_SIGNATURE fails clearly.
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-hidden',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'two' }] },
    },
  })
  const secondFailed = events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
      e.type === 'session.turn' && e.state === 'failed',
  )
  assert.ok(secondFailed)
  assert.notEqual(secondFailed.turnId, firstFailed.turnId)
  assert.match(secondFailed.error?.message ?? '', /new session/i)
})

test('fast-path session.interrupt maps SweepPendingToolCalls failed update to canceled while genuine failure stays failed', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-cancel-sweep-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
  const { events, emit, wait } = createEventCollector()
  const env = isolatedEnv(home)
  const launch = { command: 'agy', args: ['acp'], env }
  let releaseCancel!: () => void
  const cancelGate = new Promise<void>((resolve) => {
    releaseCancel = resolve
  })
  let clientRef!: Parameters<
    NonNullable<ConstructorParameters<typeof SessionManager>[0]['connectAcp']>
  >[3]
  const promptTurns = [
    async () => {
      await clientRef.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'call-swept',
          title: 'run_command',
          kind: 'execute',
          status: 'in_progress',
          rawInput: { CommandLine: 'long-task' },
        },
      })
      await cancelGate
      return { stopReason: 'cancelled' as const }
    },
    async () => {
      await clientRef.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'call-real-fail',
          title: 'run_command',
          kind: 'execute',
          status: 'in_progress',
          rawInput: { CommandLine: 'exit 1' },
        },
      })
      await clientRef.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call_update',
          toolCallId: 'call-real-fail',
          status: 'failed',
          rawOutput: 'command failed',
        },
      })
      return { stopReason: 'end_turn' as const }
    },
  ]
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit,
    commandWaitTimeoutMs: 50,
    hasActiveBackgroundWork: async () => false,
    connectAcp: (_l, _cwd, _e, client) => {
      clientRef = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: {
            loadSession: true,
            sessionCapabilities: { resume: {} },
          },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: async () => {
          const turn = promptTurns.shift()
          assert.ok(turn)
          return turn()
        },
        cancel: async () => {
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call_update',
              toolCallId: 'call-swept',
              status: 'failed',
            },
          })
          releaseCancel()
        },
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  await manager.handle({
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

  const canceledPrompt = manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-cancel',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'cancel' }] },
    },
  })
  await wait(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'call-swept' &&
      e.item.status === 'running',
  )
  await manager.handle({
    type: 'session.interrupt',
    requestId: 'interrupt-1',
    sessionId: 's',
  })
  await canceledPrompt

  const sweptFinal = events.findLast(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'call-swept',
  )
  assert.ok(
    sweptFinal?.type === 'timeline.item' &&
      sweptFinal.item.type === 'tool_call',
  )
  assert.equal(sweptFinal.item.status, 'canceled')
  assert.equal(sweptFinal.item.error, null)

  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-fail',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'fail' }] },
    },
  })
  const realFinal = events.findLast(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'call-real-fail',
  )
  assert.ok(
    realFinal?.type === 'timeline.item' && realFinal.item.type === 'tool_call',
  )
  assert.equal(realFinal.item.status, 'failed')
  assert.equal(realFinal.item.error, 'command failed')
})

test('1.5.16+ intra-RPC wakeup with silent trailing PLANNER_RESPONSE preserves PromptResponse.usage without double-counting', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-intra-rpc-silent-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc'
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const taskId = `${nativeId}/task-1`
  const events: ProviderEvent[] = []
  const env = isolatedEnv(home)
  const launch = { command: 'agy', args: ['acp'], env }
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: async () => {
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: 'cmd-1',
              title: 'run_command',
              kind: 'execute',
              status: 'in_progress',
              rawInput: {
                CommandLine: 'build',
                NotificationTimeoutSeconds: 30,
              },
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'Started build.' },
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'Still running.' },
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: 'read-1',
              title: 'read_file',
              kind: 'read',
              status: 'completed',
              rawInput: { Path: '/tmp/report.txt' },
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'usage_update',
              used: 450,
              size: 1_000_000,
            },
          })
          await writeBrainTranscript(brainRoot, nativeId, [
            { step_index: 1, type: 'USER_INPUT', content: 'run build' },
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                {
                  id: 'cmd-1',
                  name: 'run_command',
                  args: {
                    CommandLine: 'build',
                    NotificationTimeoutSeconds: 30,
                  },
                },
              ],
              input_tokens: 100,
              output_tokens: 10,
            },
            {
              step_index: 3,
              type: 'GENERIC',
              status: 'RUNNING',
              content: `Tool is running as a background task with task id: ${taskId}\n`,
            },
            {
              step_index: 4,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Started build.',
              input_tokens: 120,
              output_tokens: 5,
            },
            {
              step_index: 5,
              type: 'SYSTEM_MESSAGE',
              content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-26T00:00:00Z sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Background command is still running after 30s: build\n</SYSTEM_MESSAGE>`,
            },
            {
              step_index: 6,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'Still running.',
              input_tokens: 140,
              output_tokens: 5,
            },
            {
              step_index: 7,
              type: 'SYSTEM_MESSAGE',
              content: `<SYSTEM_MESSAGE>\n[Message] timestamp=2026-11-26T00:00:01Z sender=${taskId} priority=MESSAGE_PRIORITY_HIGH content=Task id "${taskId}" finished with result:\nok\n</SYSTEM_MESSAGE>`,
            },
            {
              step_index: 8,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                {
                  id: 'read-1',
                  name: 'read_file',
                  args: { Path: '/tmp/report.txt' },
                },
              ],
              input_tokens: 160,
              output_tokens: 8,
            },
            {
              step_index: 9,
              type: 'GENERIC',
              status: 'DONE',
              content: 'report body',
            },
            {
              step_index: 10,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              input_tokens: 180,
              output_tokens: 2,
            },
          ])
          return {
            stopReason: 'end_turn' as const,
            usage: {
              inputTokens: 700,
              outputTokens: 30,
              totalTokens: 730,
            },
          }
        },
        cancel: async () => {},
      } as unknown as ReturnType<
        typeof import('../server/transport.ts').connectAcp
      >['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())
  await manager.handle({
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
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm-intra',
      delivery: 'auto',
      input: {
        type: 'message',
        content: [{ type: 'text', text: 'run build' }],
      },
    },
  })
  const finalUsage = events.findLast((e) => e.type === 'session.usage')
  assert.ok(finalUsage?.type === 'session.usage')
  assert.equal(finalUsage.usage.inputTokens, 700)
  assert.equal(finalUsage.usage.outputTokens, 30)
  assert.equal(finalUsage.usage.contextWindowUsedTokens, 450)
  const recoveredRead = events.findLast(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'read-1',
  )
  assert.ok(
    recoveredRead?.type === 'timeline.item' &&
      recoveredRead.item.type === 'tool_call' &&
      recoveredRead.item.detail.type === 'read',
  )
  assert.equal(recoveredRead.item.detail.content, 'report body')
})

test('child-prefixed tool_call updates are isolated on root across live turns and session/load attach without breaking Brain recovery or root-UUID calls', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-child-isolation-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = '11111111-1111-4111-8111-111111111111'
  const childId = '22222222-2222-4222-8222-222222222222'
  const rootSelfCallId = `${nativeId.toUpperCase()}:call-root-self`
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const env = isolatedEnv(home)
  const events: ProviderEvent[] = []
  let acpClient!: import('../server/transport.ts').AcpClient

  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => events.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      acpClient = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: async () => {
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: `${childId}:call-child-1`,
              title: 'Run run_command',
              kind: 'execute',
              status: 'in_progress',
              rawInput: { CommandLine: 'echo child' },
            },
          })
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call_update',
              toolCallId: `${childId}:call-child-1`,
              status: 'completed',
              rawOutput: 'child-out',
            },
          })
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: 'call-root-read',
              title: 'Run read_file',
              kind: 'read',
              status: 'in_progress',
              rawInput: { Path: '/repo/a.ts' },
            },
          })
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call_update',
              toolCallId: 'call-root-read',
              status: 'completed',
            },
          })
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: rootSelfCallId,
              title: 'Run run_command',
              kind: 'execute',
              status: 'completed',
              rawInput: { CommandLine: 'pwd' },
              rawOutput: '/repo',
            },
          })
          await acpClient.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'All done' },
            },
          })
          await writeBrainTranscript(brainRoot, nativeId, [
            { step_index: 1, type: 'USER_INPUT', content: 'go' },
            {
              step_index: 2,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              tool_calls: [
                { name: 'read_file', args: { Path: '/repo/a.ts' } },
                { name: 'run_command', args: { CommandLine: 'pwd' } },
              ],
            },
            {
              step_index: 3,
              type: 'GENERIC',
              status: 'DONE',
              content: 'recovered file content',
            },
            {
              step_index: 4,
              type: 'GENERIC',
              status: 'DONE',
              content: '/repo',
            },
            {
              step_index: 5,
              type: 'PLANNER_RESPONSE',
              status: 'DONE',
              content: 'All done',
            },
          ])
          return { stopReason: 'end_turn' as const }
        },
        cancel: async () => {},
      } as unknown as ReturnType<typeof connectAcp>['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())

  await manager.handle({
    type: 'session.open',
    requestId: 'open-live',
    sessionId: 's-live',
    history: 'skip',
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  await manager.handle({
    type: 'session.prompt',
    sessionId: 's-live',
    prompt: {
      clientMessageId: 'm-live',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'go' }] },
    },
  })

  const liveTools = events.filter(
    (
      e,
    ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>
    } => e.type === 'timeline.item' && e.item.type === 'tool_call',
  )
  assert.deepEqual(
    liveTools.filter((e) => e.item.callId.startsWith(`${childId}:`)),
    [],
  )
  const rootRead = liveTools.findLast(
    (e) => e.item.callId === 'call-root-read',
  )?.item
  assert.ok(rootRead)
  assert.deepEqual(rootRead.detail, {
    type: 'read',
    filePath: '/repo/a.ts',
    content: 'recovered file content',
    offset: undefined,
    limit: undefined,
  })
  const rootSelf = liveTools.findLast(
    (e) => e.item.callId === rootSelfCallId,
  )?.item
  assert.ok(rootSelf)
  assert.equal(rootSelf.detail.type, 'shell')
  const assistantIds = new Set(
    events
      .filter(
        (
          e,
        ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
          item: Extract<ProviderTimelineItem, { type: 'assistant_message' }>
        } => e.type === 'timeline.item' && e.item.type === 'assistant_message',
      )
      .map((e) => e.item.id),
  )
  assert.equal(assistantIds.size, 1)
  const liveTurn = events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
      e.type === 'session.turn',
  )
  assert.equal(liveTurn?.state, 'completed')

  // Attach-time session/load replay drops child tool_call updates and cancels child prompts when !state.turn.
  const attachEvents: ProviderEvent[] = []
  let attachPermOutcome: unknown
  let attachElicitOutcome: unknown
  const attachManager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: (event) => attachEvents.push(event),
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        loadSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: `${childId}:attach-child-1`,
              title: 'Run run_command',
              kind: 'execute',
              status: 'in_progress',
              rawInput: { CommandLine: 'echo attach-child' },
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call_update',
              toolCallId: `${childId}:attach-child-1`,
              status: 'completed',
              rawOutput: 'done',
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'tool_call',
              toolCallId: 'attach-root-1',
              title: 'Run read_file',
              kind: 'read',
              status: 'completed',
              rawInput: { Path: '/repo/a.ts' },
              rawOutput: 'ok',
            },
          })
          await client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: 'Attached reply' },
            },
          })
          attachPermOutcome = await client.requestPermission({
            sessionId: nativeId,
            toolCall: {
              toolCallId: `${childId}:attach-perm-1`,
              title: 'Run run_command',
              rawInput: { CommandLine: 'echo blocked' },
            },
            options: [
              { optionId: 'allow_once', name: 'Allow', kind: 'allow_once' },
              { optionId: 'reject_once', name: 'Deny', kind: 'reject_once' },
            ],
          })
          attachElicitOutcome = await client.requestElicitation!({
            sessionId: nativeId,
            toolCallId: `${childId}:attach-ask-1`,
            message: 'Attach-time child question?',
            requestedSchema: {
              type: 'object',
              properties: {
                q1: {
                  type: 'string',
                  title: 'Attach-time child question?',
                  enum: ['Yes', 'No'],
                },
              },
            },
          })
          return { configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: async () => ({ stopReason: 'end_turn' as const }),
        cancel: async () => {},
      } as unknown as ReturnType<typeof connectAcp>['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => attachManager.close())

  await attachManager.handle({
    type: 'session.open',
    requestId: 'open-attach',
    sessionId: 's-attach',
    history: 'replay',
    persistence: { version: 1, data: { sessionId: nativeId } },
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })

  const attachTools = attachEvents.filter(
    (
      e,
    ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>
    } => e.type === 'timeline.item' && e.item.type === 'tool_call',
  )
  assert.deepEqual(
    attachTools.filter((e) => e.item.callId.startsWith(`${childId}:`)),
    [],
  )
  assert.ok(attachTools.some((e) => e.item.callId === 'attach-root-1'))
  assert.deepEqual(
    attachEvents.filter((e) => e.type === 'session.permission'),
    [],
  )
  assert.deepEqual(attachPermOutcome, {
    outcome: { outcome: 'cancelled' },
  })
  assert.deepEqual(attachElicitOutcome, { action: 'cancel' })
})

test('child-prefixed permissions and elicitations forward cleanly without canceling parent wakeup or hijacking root handledElicitations', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-child-forward-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const nativeId = '33333333-3333-4333-8333-333333333333'
  const childId = '44444444-4444-4444-8444-444444444444'
  const brainRoot = join(home, '.gemini/antigravity-cli/brain')
  const env = isolatedEnv(home)
  const collector = createEventCollector()
  let acpClient!: import('../server/transport.ts').AcpClient
  let cancelCalls = 0

  const turn1InitialRows: Record<string, unknown>[] = [
    { step_index: 1, type: 'USER_INPUT', content: 'turn 1' },
    {
      step_index: 2,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'turn1-ready',
          name: 'run_command',
          args: { CommandLine: 'echo ready' },
        },
        {
          name: 'ask_question',
          args: {
            questions: [
              { question: 'Proceed with deploy?', options: ['Yes', 'No'] },
            ],
          },
        },
      ],
    },
    { step_index: 3, type: 'GENERIC', status: 'DONE', content: 'ready' },
  ]
  const turn1FinalRows: Record<string, unknown>[] = [
    ...turn1InitialRows,
    { step_index: 4, type: 'GENERIC', status: 'DONE', content: 'No, wait' },
    {
      step_index: 5,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'turn1-post-answer',
          name: 'run_command',
          args: { CommandLine: 'echo after' },
        },
      ],
    },
    { step_index: 6, type: 'GENERIC', status: 'DONE', content: 'after' },
    {
      step_index: 7,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'Turn 1 complete',
    },
  ]

  const promptSteps = sequenceDouble([
    async () => {
      const childPermPromise = acpClient.requestPermission({
        sessionId: nativeId,
        toolCall: {
          toolCallId: `${childId}:perm-1`,
          title: 'Run run_command',
          rawInput: { CommandLine: 'rm -rf tmp' },
        },
        options: [
          { optionId: 'allow_once', name: 'Allow', kind: 'allow_once' },
          { optionId: 'reject_once', name: 'Deny', kind: 'reject_once' },
        ],
      })
      const permEvent = (await collector.wait(
        (e) => e.type === 'session.permission' && e.request.kind === 'tool',
      )) as Extract<ProviderEvent, { type: 'session.permission' }>
      await manager.handle({
        type: 'session.permission',
        sessionId: 's',
        permissionId: permEvent.request.id,
        response: { behavior: 'deny', selectedActionId: 'reject_once' },
      })
      assert.deepEqual(await childPermPromise, {
        outcome: { outcome: 'selected', optionId: 'reject_once' },
      })

      const childAskPromise = acpClient.requestElicitation!({
        sessionId: nativeId,
        toolCallId: `${childId}:ask-1`,
        message: 'Proceed with deploy?',
        requestedSchema: {
          type: 'object',
          properties: {
            q1: {
              type: 'string',
              title: 'Proceed with deploy?',
              enum: ['Yes', 'No'],
            },
          },
        },
      })
      const askEvent = (await collector.wait(
        (e) => e.type === 'session.permission' && e.request.kind === 'question',
      )) as Extract<ProviderEvent, { type: 'session.permission' }>
      await manager.handle({
        type: 'session.permission',
        sessionId: 's',
        permissionId: askEvent.request.id,
        response: {
          behavior: 'allow',
          updatedInput: { answers: { 'Question 1': 'Yes' } },
        },
      })
      assert.deepEqual(await childAskPromise, {
        action: 'accept',
        content: { q1: 'Yes' },
      })

      await writeBrainTranscript(brainRoot, nativeId, turn1InitialRows)
      return { stopReason: 'end_turn' as const }
    },
    async () => {
      await writeBrainTranscript(brainRoot, nativeId, [
        ...turn1FinalRows,
        { step_index: 8, type: 'USER_INPUT', content: 'turn 2' },
        {
          step_index: 9,
          type: 'PLANNER_RESPONSE',
          status: 'DONE',
          tool_calls: [
            {
              id: 'turn2-in-settle',
              name: 'run_command',
              args: { CommandLine: 'echo settle' },
            },
            {
              id: 'root-wait-1',
              name: 'run_command',
              args: { CommandLine: 'echo wait' },
            },
          ],
        },
        { step_index: 10, type: 'GENERIC', status: 'DONE', content: 'settle' },
      ])
      return { stopReason: 'end_turn' as const }
    },
  ])

  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      acpClient = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: () => promptSteps(),
        cancel: async () => {
          cancelCalls += 1
        },
      } as unknown as ReturnType<typeof connectAcp>['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())

  await manager.handle({
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

  // Turn 1: child permission & elicitation succeed; child answer must not hijack anonymous root ask_question.
  const turn1Promise = manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'turn 1' }] },
    },
  })
  await collector.wait(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'turn1-ready',
  )
  await writeBrainTranscript(brainRoot, nativeId, turn1FinalRows)
  await turn1Promise

  const postAnswerItem = collector.events.findLast(
    (
      e,
    ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>
    } =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'turn1-post-answer',
  )?.item
  assert.ok(postAnswerItem)
  assert.equal(postAnswerItem.status, 'completed')

  // Turn 2: declining a child elicitation during inSettle must not cancel the parent turn.
  const turn2Promise = manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm2',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'turn 2' }] },
    },
  })
  await collector.wait(
    (e) =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'turn2-in-settle',
  )
  const childDeclinePromise = acpClient.requestElicitation!({
    sessionId: nativeId,
    toolCallId: `${childId}:wakeup-ask-1`,
    message: 'Child question during settle?',
    requestedSchema: {
      type: 'object',
      properties: {
        q1: {
          type: 'string',
          title: 'Child question during settle?',
          enum: ['A', 'B'],
        },
      },
    },
  })
  const childDeclineEvent = (await collector.wait(
    (e) =>
      e.type === 'session.permission' &&
      e.request.title === 'Child question during settle?',
  )) as Extract<ProviderEvent, { type: 'session.permission' }>
  await manager.handle({
    type: 'session.permission',
    sessionId: 's',
    permissionId: childDeclineEvent.request.id,
    response: { behavior: 'deny' },
  })
  assert.deepEqual(await childDeclinePromise, { action: 'decline' })

  await writeBrainTranscript(brainRoot, nativeId, [
    ...turn1FinalRows,
    { step_index: 8, type: 'USER_INPUT', content: 'turn 2' },
    {
      step_index: 9,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      tool_calls: [
        {
          id: 'turn2-in-settle',
          name: 'run_command',
          args: { CommandLine: 'echo settle' },
        },
        {
          id: 'root-wait-1',
          name: 'run_command',
          args: { CommandLine: 'echo wait' },
        },
      ],
    },
    { step_index: 10, type: 'GENERIC', status: 'DONE', content: 'settle' },
    { step_index: 11, type: 'GENERIC', status: 'DONE', content: 'wait done' },
    {
      step_index: 12,
      type: 'PLANNER_RESPONSE',
      status: 'DONE',
      content: 'Turn 2 complete',
    },
  ])
  await turn2Promise

  assert.equal(cancelCalls, 0)
  const turn2Terminal = collector.events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.turn' }> =>
      e.type === 'session.turn',
  )
  assert.equal(turn2Terminal?.state, 'completed')
})

function createModelOnlyMockConnect(
  nativeId: string,
  spawnArgs: Array<readonly string[]>,
  setConfigCalls: Array<{ configId: string; value: string }>,
): typeof connectAcp {
  return (launch, _cwd, _e, client) => {
    spawnArgs.push(launch?.args ?? [])
    let currentModel = 'small'
    const configOptions = () => [
      {
        id: 'model',
        name: 'Model',
        category: 'model',
        type: 'select' as const,
        currentValue: currentModel,
        options: [
          { value: 'small', name: 'Small' },
          { value: 'large', name: 'Large' },
        ],
      },
    ]
    const emitCommands = () => {
      void client.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [],
        },
      })
    }
    const closed = new Promise<void>(() => {})
    const rpc = {
      closed,
      initialize: async () => ({
        protocolVersion: 1,
        agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
        agentCapabilities: {
          loadSession: true,
          sessionCapabilities: { resume: {} },
        },
      }),
      newSession: async () => {
        emitCommands()
        return { sessionId: nativeId, configOptions: configOptions() }
      },
      resumeSession: async () => {
        emitCommands()
        return { configOptions: configOptions() }
      },
      setSessionConfigOption: async (params: {
        configId: string
        value: string
      }) => {
        setConfigCalls.push({ configId: params.configId, value: params.value })
        currentModel = params.value
        return { configOptions: configOptions() }
      },
      closeSession: async () => ({}),
    } as unknown as ReturnType<typeof connectAcp>['rpc']
    return {
      rpc,
      exited: closed,
      stopped: Promise.resolve(),
      close: async () => {},
      request: async <T>(op: Promise<T>) => op,
      get closed() {
        return false
      },
      get tier() {
        return undefined
      },
      get models() {
        return undefined
      },
    }
  }
}

test('configureSession enables full-access even when native mode is unadvertised', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-fullaccess-nomode-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const env = isolatedEnv(home)
  const nativeId = '11111111-1111-4111-8111-111111111121'
  const collector = createEventCollector()
  const spawnArgs: Array<readonly string[]> = []
  const setConfigCalls: Array<{ configId: string; value: string }> = []
  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    commandWaitTimeoutMs: 50,
    connectAcp: createModelOnlyMockConnect(nativeId, spawnArgs, setConfigCalls),
  })
  t.after(() => manager.close())

  await manager.handle({
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
  await manager.handle({
    type: 'session.configure',
    requestId: 'cfg-fa',
    sessionId: 's1',
    changes: { model: 'large', mode: 'full-access' },
  })
  const latestStrict = collector.events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.config' }> =>
      e.type === 'session.config' && e.sessionId === 's1',
  )
  assert.equal(spawnArgs.length, 2)
  assert.equal(spawnArgs[1].includes('--dangerously-skip-permissions'), true)
  assert.deepEqual(setConfigCalls, [
    { configId: 'model', value: 'large' },
    { configId: 'model', value: 'large' },
  ])
  assert.equal(latestStrict?.config.model, 'large')
  assert.equal(latestStrict?.config.mode, 'full-access')

  await manager.handle({
    type: 'session.open',
    requestId: 'open-2',
    sessionId: 's2',
    history: 'skip',
    persistence: { version: 1, data: { sessionId: nativeId } },
    config: {
      cwd: home,
      env: {},
      mcpServers: {},
      settings: {},
      mode: 'full-access',
      persist: true,
    },
  })
  const latestRestore = collector.events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.config' }> =>
      e.type === 'session.config' && e.sessionId === 's2',
  )
  assert.equal(latestRestore?.config.mode, 'full-access')
})

test('concurrent session.close during open startup prevents re-insertion and skips newSession RPC', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-close-mid-open-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const env = isolatedEnv(home)
  const collector = createEventCollector()
  let resolveInitEntered!: () => void
  const initEntered = new Promise<void>((resolve) => {
    resolveInitEntered = resolve
  })
  let releaseInit!: () => void
  const initGate = new Promise<void>((resolve) => {
    releaseInit = resolve
  })
  let newSessionCalls = 0
  let closeCalls = 0

  const manager = new SessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    commandWaitTimeoutMs: 50,
    connectAcp: () => {
      let isClosed = false
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => {
          resolveInitEntered()
          await initGate
          return {
            protocolVersion: 1,
            agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
            agentCapabilities: { loadSession: true },
          }
        },
        newSession: async () => {
          newSessionCalls += 1
          return {
            sessionId: '11111111-1111-4111-8111-111111111122',
            configOptions: [],
          }
        },
        closeSession: async () => ({}),
      } as unknown as ReturnType<typeof connectAcp>['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {
          isClosed = true
          closeCalls += 1
        },
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return isClosed
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())

  const openPromise = manager.handle({
    type: 'session.open',
    requestId: 'open-race',
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
  await initEntered
  const closePromise = manager.handle({
    type: 'session.close',
    requestId: 'close-early',
    sessionId: 's',
  })
  releaseInit()

  await closePromise
  await assert.rejects(openPromise, /Session closed before open/)
  assert.equal(newSessionCalls, 0)
  assert.equal(closeCalls >= 1, true)
  assert.equal(
    collector.events.some(
      (e) => e.type === 'request.completed' && e.requestId === 'close-early',
    ),
    true,
  )
  await assert.rejects(
    manager.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'after-close',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hi' }] },
      },
    }),
    /Session is not open/,
  )
})

test('activeTurn clears per-turn state when idle without leaking deniedToolCallIds into the next turn', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-turn-cleanup-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const env = isolatedEnv(home)
  const nativeId = '11111111-1111-4111-8111-111111111123'
  const collector = createEventCollector()
  let acpClient!: Parameters<typeof connectAcp>[3]
  const promptSteps = sequenceDouble([
    async () => {
      await acpClient.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: 'Working' },
        },
      })
      await acpClient.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'shared-call',
          title: 'Run run_command',
          status: 'in_progress',
          rawInput: { CommandLine: 'echo one' },
        },
      })
      await acpClient.requestPermission({
        sessionId: nativeId,
        toolCall: {
          toolCallId: 'shared-call',
          title: 'Run run_command',
          rawInput: { CommandLine: 'echo one' },
        },
        options: [
          { optionId: 'allow', name: 'Allow', kind: 'allow_once' },
          { optionId: 'deny', name: 'Deny', kind: 'reject_once' },
        ],
      })
      return { stopReason: 'end_turn' as const }
    },
    async () => {
      await acpClient.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'shared-call',
          title: 'Run run_command',
          status: 'in_progress',
          rawInput: { CommandLine: 'echo two' },
        },
      })
      await acpClient.sessionUpdate({
        sessionId: nativeId,
        update: {
          sessionUpdate: 'tool_call_update',
          toolCallId: 'shared-call',
          status: 'failed',
          rawOutput: 'command failed',
        },
      })
      return { stopReason: 'end_turn' as const }
    },
  ])

  const manager = new HarnessSessionManager({
    launch: { command: 'agy', args: ['acp'], env },
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    commandWaitTimeoutMs: 50,
    connectAcp: (_l, _cwd, _e, client) => {
      acpClient = client
      const closed = new Promise<void>(() => {})
      const rpc = {
        closed,
        initialize: async () => ({
          protocolVersion: 1,
          agentInfo: { name: 'Antigravity ACP Server', version: '1.5.16' },
          agentCapabilities: { loadSession: true },
        }),
        newSession: async () => {
          void client.sessionUpdate({
            sessionId: nativeId,
            update: {
              sessionUpdate: 'available_commands_update',
              availableCommands: [],
            },
          })
          return { sessionId: nativeId, configOptions: [] }
        },
        closeSession: async () => ({}),
        prompt: () => promptSteps(),
      } as unknown as ReturnType<typeof connectAcp>['rpc']
      return {
        rpc,
        exited: closed,
        stopped: Promise.resolve(),
        close: async () => {},
        request: async <T>(op: Promise<T>) => op,
        get closed() {
          return false
        },
        get tier() {
          return undefined
        },
        get models() {
          return undefined
        },
      }
    },
  })
  t.after(() => manager.close())

  await manager.handle({
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

  const turn1 = manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm1',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'first' }] },
    },
  })
  const perm = (await collector.wait(
    (e) => e.type === 'session.permission',
  )) as Extract<ProviderEvent, { type: 'session.permission' }>
  await manager.handle({
    type: 'session.permission',
    sessionId: 's',
    permissionId: perm.request.id,
    response: { behavior: 'deny' },
  })
  await turn1

  const turn1Tool = collector.events.findLast(
    (
      e,
    ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>
    } =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'shared-call',
  )?.item
  assert.equal(turn1Tool?.status, 'canceled')
  assert.equal(manager.session.state.activeTurn, null)

  await manager.handle({
    type: 'session.can_stop_runtime',
    requestId: 'can-stop-idle',
    sessionId: 's',
  })
  const canStopEvent = collector.events.findLast(
    (e): e is Extract<ProviderEvent, { type: 'session.can_stop_runtime' }> =>
      e.type === 'session.can_stop_runtime',
  )
  assert.equal(canStopEvent?.canStop, true)

  await manager.handle({
    type: 'session.prompt',
    sessionId: 's',
    prompt: {
      clientMessageId: 'm2',
      delivery: 'auto',
      input: { type: 'message', content: [{ type: 'text', text: 'second' }] },
    },
  })
  const turn2Tool = collector.events.findLast(
    (
      e,
    ): e is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'tool_call' }>
    } =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.callId === 'shared-call',
  )?.item
  assert.equal(turn2Tool?.status, 'failed')
  assert.equal(manager.session.state.activeTurn, null)
})
