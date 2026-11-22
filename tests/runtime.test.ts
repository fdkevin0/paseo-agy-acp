import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderLaunch,
} from '@getpaseo/plugin/server/provider'
import { dataDirectory } from '../server/preflight.ts'
import { createProvider } from '../server/provider.ts'
import {
  SharedControlRuntime,
  StandbyRuntimePool,
  isLaunchCwdIndependent,
  standbyPoolKey,
  type ControlTransport,
  type IdleTimerHandle,
  type IdleTimerScheduler,
} from '../server/runtime.ts'
import { SessionManager } from '../server/session.ts'
import type { AcpClient, connectAcp } from '../server/transport.ts'
import { AgyUsageSource } from '../server/usage.ts'
import {
  collectProviderEvents,
  createEventCollector,
  isolatedEnv,
} from './helpers.ts'

const mockAcpPath = fileURLToPath(new URL('./mock-acp.ts', import.meta.url))

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

interface FakeTransportRecord {
  readonly id: number
  readonly launch: ProviderLaunch | undefined
  readonly cwd: string
  readonly env: Readonly<Record<string, string>>
  readonly client: AcpClient
  readonly transport: ControlTransport
  readonly closedBarrier: ReturnType<typeof deferred<void>>
  readonly closeCalled: () => number
  readonly listCalls: () => Array<{ cwd?: string | null }>
  readonly triggerExit: (error: Error) => void
  setObserved: (next: {
    models?: ControlTransport['models']
    tier?: ControlTransport['tier']
  }) => void
}

function createFakeConnect(
  scripts?: readonly {
    init?: () => Promise<{
      agentInfo?: { version?: string }
      agentCapabilities?: Record<string, unknown>
    }>
    listSessions?: (params: { cwd?: string | null }) => Promise<{
      sessions: unknown[]
    }>
    close?: () => Promise<void>
  }[],
) {
  const created: FakeTransportRecord[] = []
  const connect: typeof connectAcp = (launch, cwd, env, client, onExit) => {
    const id = created.length + 1
    const script = scripts?.[id - 1]
    const closedBarrier = deferred<void>()
    const recordedListCalls: Array<{ cwd?: string | null }> = []
    let isClosed = false
    let closeCalls = 0
    let closing: Promise<void> | undefined
    let models: ControlTransport['models']
    let tier: ControlTransport['tier']
    const rpc = {
      closed: closedBarrier.promise,
      initialize: async () => {
        const result = script?.init
          ? await script.init()
          : {
              agentInfo: { version: '1.5.12' },
              agentCapabilities: {
                loadSession: true,
                sessionCapabilities: { list: {} },
              },
            }
        return { protocolVersion: 1, ...result }
      },
      listSessions: async (params: { cwd?: string | null }) => {
        recordedListCalls.push(params)
        if (script?.listSessions) return script.listSessions(params)
        return { sessions: [] }
      },
    } as unknown as ControlTransport['rpc']
    const transport: ControlTransport = {
      rpc,
      exited: closedBarrier.promise,
      stopped: closedBarrier.promise,
      close: () => {
        if (closing) return closing
        closeCalls++
        isClosed = true
        closing = (async () => {
          if (script?.close) await script.close()
          closedBarrier.resolve()
        })()
        return closing
      },
      request: async (operation) => operation,
      get closed() {
        return isClosed
      },
      get tier() {
        return tier
      },
      get models() {
        return models
      },
    }
    created.push({
      id,
      launch,
      cwd,
      env,
      client,
      transport,
      closedBarrier,
      closeCalled: () => closeCalls,
      listCalls: () => recordedListCalls,
      triggerExit: (error) => {
        isClosed = true
        onExit(error)
      },
      setObserved: (next) => {
        models = next.models
        tier = next.tier
      },
    })
    return transport
  }
  return { connect, created }
}

function createManualScheduler() {
  const scheduled: Array<{
    fn: () => void
    ms: number
    unreffed: boolean
    cleared: boolean
    handle: IdleTimerHandle
  }> = []
  const scheduler: IdleTimerScheduler = {
    setTimeout(fn, ms) {
      const entry = {
        fn,
        ms,
        unreffed: false,
        cleared: false,
        handle: {
          unref() {
            entry.unreffed = true
          },
        },
      }
      scheduled.push(entry)
      return entry.handle
    },
    clearTimeout(handle) {
      const entry = scheduled.find((item) => item.handle === handle)
      if (entry) entry.cleared = true
    },
  }
  return { scheduler, scheduled }
}

async function readPids(path: string): Promise<number[]> {
  const raw = await readFile(path, 'utf8')
  return raw
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => Number(line))
}

async function readRpcPidLog(
  path: string,
): Promise<Array<{ pid: number; method: string }>> {
  const raw = await readFile(path, 'utf8')
  return raw
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const [pid, method] = line.split(':')
      return { pid: Number(pid), method }
    })
}

function uniqueMethodPids(
  entries: readonly { pid: number; method: string }[],
  method: string,
): number[] {
  return [
    ...new Set(entries.filter((e) => e.method === method).map((e) => e.pid)),
  ]
}

function assertProcessAlive(pid: number) {
  assert.doesNotThrow(() => process.kill(pid, 0))
}

function assertProcessExited(pid: number) {
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
}

async function createMockProvider(
  home: string,
  extraEnv: Record<string, string> = {},
  extraArgs: readonly string[] = [],
) {
  const env = isolatedEnv(home, extraEnv)
  const usage = new AgyUsageSource({ env })
  const launch: ProviderLaunch = {
    command: process.execPath,
    args: [mockAcpPath, ...extraArgs],
    env,
  }
  const provider = await createProvider(usage).connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch,
  })
  const collector = collectProviderEvents(provider)
  return { provider, usage, launch, ...collector }
}

test('cwd-independent catalog refreshes and session listings reuse one ACP process PID and clean up temporary artifacts while staying alive', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-reuse-'))
  const pidLog = join(home, 'pids.log')
  const rpcPidLog = join(home, 'rpc-pids.log')
  const callLog = join(home, 'calls.log')
  const appData = dataDirectory(undefined, { HOME: home })
  await mkdir(join(appData, 'conversations'), { recursive: true })
  await mkdir(join(appData, 'brain'), { recursive: true })
  await writeFile(join(appData, 'conversations', 'summary.db'), 'keep')
  const h = await createMockProvider(home, {
    AGY_TEST_PID_LOG: pidLog,
    AGY_TEST_RPC_PID_LOG: rpcPidLog,
    AGY_TEST_LOG: callLog,
    AGY_TEST_ARTIFACTS_DIR: appData,
  })
  try {
    await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
    const cat1 = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'cat-1',
    )
    assert.equal(cat1.type, 'catalog')

    await h.provider.send({ type: 'sessions', requestId: 'list-1', cwd: home })
    const list1 = await h.wait(
      (e) => e.type === 'sessions' && e.requestId === 'list-1',
    )
    assert.equal(list1.type, 'sessions')

    await h.provider.send({ type: 'catalog', requestId: 'cat-2', cwd: home })
    const cat2 = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'cat-2',
    )
    assert.equal(cat2.type, 'catalog')

    await h.provider.send({ type: 'sessions', requestId: 'list-2', cwd: home })
    const list2 = await h.wait(
      (e) => e.type === 'sessions' && e.requestId === 'list-2',
    )
    assert.equal(list2.type, 'sessions')

    const rpcEntries = await readRpcPidLog(rpcPidLog)
    const controlNewPids = uniqueMethodPids(rpcEntries, 'new')
    assert.equal(controlNewPids.length, 1)
    assertProcessAlive(controlNewPids[0])
    assert.equal(
      rpcEntries.filter(
        (e) => e.pid === controlNewPids[0] && e.method === 'list',
      ).length,
      2,
    )
    assert.equal(await readFile(callLog, 'utf8'), 'new\nclose\nnew\nclose\n')
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])

    const spawnedBeforeClose = await readPids(pidLog)
    assert.ok(spawnedBeforeClose.length <= 2)
    await h.provider.close()
    const allSpawned = await readPids(pidLog)
    allSpawned.forEach(assertProcessExited)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('concurrent catalog probes for different cwds serialize on one shared transport without cross-request metadata overwrite', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-concurrent-'))
  const dirAlpha = join(home, 'alpha')
  const dirBeta = join(home, 'beta')
  const pidLog = join(home, 'pids.log')
  await mkdir(dirAlpha)
  await mkdir(dirBeta)

  const fake = createFakeConnect()
  const runtime = new SharedControlRuntime({
    launch: { command: 'agy', args: ['acp'], env: isolatedEnv(home) },
    connectAcp: fake.connect,
  })
  const probeAGate = deferred<void>()
  const probeAStarted = deferred<void>()
  const order: string[] = []
  try {
    const first = runtime.run(
      dirAlpha,
      async ({ transport }) => {
        order.push('alpha:start')
        fake.created[0].setObserved({
          models: { availableModels: [{ modelId: 'alpha-low' }] },
          tier: { id: 'alpha', name: 'Tier Alpha' },
        })
        probeAStarted.resolve()
        await probeAGate.promise
        const observed = {
          modelId: transport.models?.availableModels?.[0]?.modelId,
          tier: transport.tier?.name,
        }
        order.push('alpha:end')
        return observed
      },
      { serialize: true },
    )
    const second = runtime.run(
      dirBeta,
      async ({ transport }) => {
        order.push('beta:start')
        fake.created[0].setObserved({
          models: { availableModels: [{ modelId: 'beta-high' }] },
          tier: { id: 'beta', name: 'Tier Beta' },
        })
        const observed = {
          modelId: transport.models?.availableModels?.[0]?.modelId,
          tier: transport.tier?.name,
        }
        order.push('beta:end')
        return observed
      },
      { serialize: true },
    )
    await probeAStarted.promise
    assert.deepEqual(order, ['alpha:start'])
    probeAGate.resolve()
    const [alphaObserved, betaObserved] = await Promise.all([first, second])
    assert.equal(fake.created.length, 1)
    assert.deepEqual(order, [
      'alpha:start',
      'alpha:end',
      'beta:start',
      'beta:end',
    ])
    assert.deepEqual(alphaObserved, {
      modelId: 'alpha-low',
      tier: 'Tier Alpha',
    })
    assert.deepEqual(betaObserved, {
      modelId: 'beta-high',
      tier: 'Tier Beta',
    })

    const h = await createMockProvider(home, {
      AGY_TEST_RPC_PID_LOG: pidLog,
      AGY_TEST_CATALOG_BY_CWD: '1',
    })
    try {
      await Promise.all([
        h.provider.send({
          type: 'catalog',
          requestId: 'cat-alpha',
          cwd: dirAlpha,
        }),
        h.provider.send({
          type: 'catalog',
          requestId: 'cat-beta',
          cwd: dirBeta,
        }),
      ])
      const catAlpha = await h.wait(
        (e) => e.type === 'catalog' && e.requestId === 'cat-alpha',
      )
      const catBeta = await h.wait(
        (e) => e.type === 'catalog' && e.requestId === 'cat-beta',
      )
      assert.ok(catAlpha.type === 'catalog')
      assert.ok(catBeta.type === 'catalog')
      assert.equal(catAlpha.catalog.defaultModel, 'alpha')
      assert.deepEqual(
        catAlpha.catalog.models[0].thinkingOptions?.map((o) => o.id),
        ['low'],
      )
      assert.equal(catBeta.catalog.defaultModel, 'beta')
      assert.deepEqual(
        catBeta.catalog.models[0].thinkingOptions?.map((o) => o.id),
        ['low'],
      )
      const catalogNewPids = uniqueMethodPids(
        await readRpcPidLog(pidLog),
        'new',
      )
      assert.equal(catalogNewPids.length, 1)
    } finally {
      await h.provider.close()
    }
  } finally {
    await runtime.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('unexpected exit or failed startup evicts the dead shared control transport and recovers transparently', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-crash-'))
  const pidLog = join(home, 'pids.log')
  const crashFlag = join(home, 'crash-list')
  const fake = createFakeConnect([
    { init: async () => ({ agentInfo: { version: '1.5.9' } }) },
    { init: async () => ({ agentInfo: { version: '1.5.12' } }) },
    { init: async () => ({ agentInfo: { version: '1.5.12' } }) },
  ])
  const manual = createManualScheduler()
  const runtime = new SharedControlRuntime({
    launch: { command: 'agy', args: ['acp'], env: isolatedEnv(home) },
    connectAcp: fake.connect,
    idleTimeoutMs: 60_000,
    scheduler: manual.scheduler,
  })
  try {
    await assert.rejects(
      runtime.run(home, async () => 'unreachable'),
      /too old/,
    )
    assert.equal(fake.created[0].closeCalled(), 1)

    const secondId = await runtime.run(home, async ({ transport }) => {
      assert.equal(transport, fake.created[1].transport)
      return fake.created[1].id
    })
    assert.equal(secondId, 2)
    assert.equal(manual.scheduled.length, 1)

    fake.created[1].triggerExit(new Error('unexpected crash'))
    assert.equal(manual.scheduled[0].cleared, true)

    const thirdId = await runtime.run(home, async ({ transport }) => {
      assert.equal(transport, fake.created[2].transport)
      return fake.created[2].id
    })
    assert.equal(thirdId, 3)

    const workDir = join(home, 'work')
    await mkdir(workDir)
    const h = await createMockProvider(home, {
      AGY_TEST_RPC_PID_LOG: pidLog,
      AGY_TEST_CRASH_ON_LIST_FLAG: crashFlag,
    })
    try {
      await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
      await h.wait((e) => e.type === 'catalog' && e.requestId === 'cat-1')
      const [pid1] = uniqueMethodPids(await readRpcPidLog(pidLog), 'new')

      await writeFile(crashFlag, workDir)
      await h.provider.send({
        type: 'sessions',
        requestId: 'list-crash',
        cwd: workDir,
      })
      await h.wait(
        (e) => e.type === 'request.failed' && e.requestId === 'list-crash',
      )

      await h.provider.send({
        type: 'sessions',
        requestId: 'list-recover',
        cwd: workDir,
      })
      const recoveredList = await h.wait(
        (e) => e.type === 'sessions' && e.requestId === 'list-recover',
      )
      assert.equal(recoveredList.type, 'sessions')

      await h.provider.send({
        type: 'catalog',
        requestId: 'cat-recover',
        cwd: home,
      })
      const recoveredCatalog = await h.wait(
        (e) => e.type === 'catalog' && e.requestId === 'cat-recover',
      )
      assert.equal(recoveredCatalog.type, 'catalog')

      const controlPids = uniqueMethodPids(await readRpcPidLog(pidLog), 'new')
      assert.equal(controlPids.length, 2)
      assert.notEqual(controlPids[1], pid1)
      assertProcessAlive(controlPids[1])
    } finally {
      await h.provider.close()
    }
  } finally {
    await runtime.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('session/close failure during probeCatalog still returns the catalog, retires the unhealthy process, cleans up artifacts, and recovers next probe', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-close-err-'))
  const pidLog = join(home, 'pids.log')
  const closeErrorFlag = join(home, 'fail-close')
  const appData = dataDirectory(undefined, { HOME: home })
  await mkdir(join(appData, 'conversations'), { recursive: true })
  await mkdir(join(appData, 'brain'), { recursive: true })
  await writeFile(join(appData, 'conversations', 'summary.db'), 'keep')
  await writeFile(closeErrorFlag, '1')
  const h = await createMockProvider(home, {
    AGY_TEST_RPC_PID_LOG: pidLog,
    AGY_TEST_CLOSE_ERROR_FLAG: closeErrorFlag,
    AGY_TEST_ARTIFACTS_DIR: appData,
  })
  try {
    await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
    const cat1 = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'cat-1',
    )
    assert.ok(cat1.type === 'catalog' && cat1.catalog.models.length > 0)
    assert.equal(
      h.events.some((e) => e.type === 'request.failed'),
      false,
    )

    const [pid1] = uniqueMethodPids(await readRpcPidLog(pidLog), 'new')
    assertProcessExited(pid1)
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])

    await h.provider.send({ type: 'catalog', requestId: 'cat-2', cwd: home })
    const cat2 = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'cat-2',
    )
    assert.ok(cat2.type === 'catalog' && cat2.catalog.models.length > 0)

    const pids = uniqueMethodPids(await readRpcPidLog(pidLog), 'new')
    assert.equal(pids.length, 2)
    assert.notEqual(pids[1], pid1)
    assertProcessAlive(pids[1])
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('cwd-dependent launches never reuse a shared control transport and close one-shot processes immediately', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-cwd-dep-'))
  const pidLog = join(home, 'pids.log')
  const fakeRelDir = createFakeConnect()
  const relDirRuntime = new SharedControlRuntime({
    launch: {
      command: 'agy',
      args: ['acp', '--gemini-dir', './custom'],
      env: isolatedEnv(home),
    },
    connectAcp: fakeRelDir.connect,
  })
  const fakeRelCmd = createFakeConnect()
  const relCmdRuntime = new SharedControlRuntime({
    launch: {
      command: './agy',
      args: ['acp'],
      env: isolatedEnv(home),
    },
    connectAcp: fakeRelCmd.connect,
  })
  try {
    assert.equal(relDirRuntime.isReusable(home), false)
    assert.equal(relCmdRuntime.isReusable(home), false)

    await relDirRuntime.run(home, async ({ reusable }) => {
      assert.equal(reusable, false)
    })
    assert.equal(fakeRelDir.created[0].closeCalled(), 1)
    await relDirRuntime.run(home, async ({ reusable }) => {
      assert.equal(reusable, false)
    })
    assert.equal(fakeRelDir.created.length, 2)
    assert.equal(fakeRelDir.created[1].closeCalled(), 1)

    await relCmdRuntime.run(home, async ({ reusable }) => {
      assert.equal(reusable, false)
    })
    assert.equal(fakeRelCmd.created[0].closeCalled(), 1)

    const h = await createMockProvider(home, { AGY_TEST_PID_LOG: pidLog }, [
      '--gemini-dir',
      './custom',
    ])
    try {
      await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
      await h.wait((e) => e.type === 'catalog' && e.requestId === 'cat-1')
      const [pid1] = await readPids(pidLog)
      assertProcessExited(pid1)

      await h.provider.send({
        type: 'sessions',
        requestId: 'list-1',
        cwd: home,
      })
      await h.wait((e) => e.type === 'sessions' && e.requestId === 'list-1')
      const pids = await readPids(pidLog)
      assert.equal(pids.length, 2)
      assert.notEqual(pids[1], pid1)
      assertProcessExited(pids[1])
    } finally {
      await h.provider.close()
    }
  } finally {
    await relDirRuntime.close()
    await relCmdRuntime.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('idle timeout arms an unreffed timer, cancels on reuse, evicts and closes when idle, and restarts on next call', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-idle-'))
  const fake = createFakeConnect()
  const manual = createManualScheduler()
  const runtime = new SharedControlRuntime({
    launch: { command: 'agy', args: ['acp'], env: isolatedEnv(home) },
    connectAcp: fake.connect,
    idleTimeoutMs: 42_000,
    scheduler: manual.scheduler,
  })
  try {
    await runtime.run(home, async ({ transport }) => {
      assert.equal(transport, fake.created[0].transport)
    })
    assert.equal(manual.scheduled.length, 1)
    assert.equal(manual.scheduled[0].ms, 42_000)
    assert.equal(manual.scheduled[0].unreffed, true)
    assert.equal(manual.scheduled[0].cleared, false)

    await runtime.run(home, async ({ transport }) => {
      assert.equal(manual.scheduled[0].cleared, true)
      assert.equal(transport, fake.created[0].transport)
    })
    assert.equal(fake.created.length, 1)
    assert.equal(manual.scheduled.length, 2)
    assert.equal(manual.scheduled[1].unreffed, true)

    manual.scheduled[1].fn()
    await fake.created[0].closedBarrier.promise
    assert.equal(fake.created[0].closeCalled(), 1)

    await runtime.run(home, async ({ transport }) => {
      assert.equal(transport, fake.created[1].transport)
    })
    assert.equal(fake.created.length, 2)
  } finally {
    await runtime.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('SharedControlRuntime.close shuts down idle, in-flight startup, and ephemeral transports cleanly with zero leaked handles', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-runtime-close-'))
  const manual = createManualScheduler()
  const idleFake = createFakeConnect()
  const idleRuntime = new SharedControlRuntime({
    launch: { command: 'agy', args: ['acp'], env: isolatedEnv(home) },
    connectAcp: idleFake.connect,
    idleTimeoutMs: 60_000,
    scheduler: manual.scheduler,
  })

  const startupGate = deferred<{ agentInfo?: { version?: string } }>()
  const startupEntered = deferred<void>()
  const startupFake = createFakeConnect([
    {
      init: () => {
        startupEntered.resolve()
        return startupGate.promise
      },
    },
  ])
  const startupRuntime = new SharedControlRuntime({
    launch: { command: 'agy', args: ['acp'], env: isolatedEnv(home) },
    connectAcp: startupFake.connect,
  })

  const ephemeralGate = deferred<void>()
  const ephemeralEntered = deferred<void>()
  const ephemeralFake = createFakeConnect()
  const ephemeralRuntime = new SharedControlRuntime({
    launch: {
      command: './agy',
      args: ['acp'],
      env: isolatedEnv(home),
    },
    connectAcp: ephemeralFake.connect,
  })

  try {
    await idleRuntime.run(home, async () => 'ok')
    await idleRuntime.close()
    assert.equal(manual.scheduled[0].cleared, true)
    assert.equal(idleFake.created[0].closeCalled(), 1)
    await assert.rejects(
      idleRuntime.run(home, async () => 'nope'),
      /Provider connection is closed/,
    )

    const pendingStartup = startupRuntime.run(home, async () => 'never')
    await startupEntered.promise
    const closingStartup = startupRuntime.close()
    startupGate.resolve({ agentInfo: { version: '1.5.12' } })
    await closingStartup
    await assert.rejects(pendingStartup, /Provider connection is closed/)
    assert.equal(startupFake.created[0].closeCalled(), 1)

    const pendingEphemeral = ephemeralRuntime.run(home, async () => {
      ephemeralEntered.resolve()
      await ephemeralGate.promise
    })
    await ephemeralEntered.promise
    const closingEphemeral = ephemeralRuntime.close()
    ephemeralGate.resolve()
    await Promise.all([closingEphemeral, pendingEphemeral])
    assert.equal(ephemeralFake.created[0].closeCalled(), 1)
  } finally {
    await Promise.all([
      idleRuntime.close(),
      startupRuntime.close(),
      ephemeralRuntime.close(),
    ])
    await rm(home, { recursive: true, force: true })
  }
})

test('StandbyRuntimePool warms with initialize + listSessions, joins in-flight warmup, enforces exclusive take, and rebinds callbacks dynamically', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-warm-'))
  const workDir = join(home, 'project')
  await mkdir(workDir)
  const listGate = deferred<{ sessions: unknown[] }>()
  const listEntered = deferred<void>()
  const fake = createFakeConnect([
    {
      listSessions: () => {
        listEntered.resolve()
        return listGate.promise
      },
    },
  ])
  const manual = createManualScheduler()
  const launch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const pool = new StandbyRuntimePool({
    launch,
    connectAcp: fake.connect,
    idleTimeoutMs: 30_000,
    scheduler: manual.scheduler,
  })
  try {
    pool.warm(launch, workDir, {})
    // Duplicate warm while in-flight is a no-op.
    pool.warm(launch, workDir, {})
    assert.equal(fake.created.length, 1)
    assert.equal(fake.created[0].cwd, home)
    await listEntered.promise
    assert.deepEqual(fake.created[0].listCalls(), [{ cwd: home }])

    // Two concurrent takes: first claims and joins the in-flight warmup; second gets undefined immediately.
    const firstTake = pool.take(launch, workDir, {})
    const secondTake = pool.take(launch, workDir, {})
    assert.equal(await secondTake, undefined)

    listGate.resolve({ sessions: [] })
    const runtime = await firstTake
    assert.ok(runtime)
    assert.equal(runtime.transport, fake.created[0].transport)
    assert.equal(runtime.reusable, true)
    assert.equal(manual.scheduled.length, 0)

    // Verify dynamic client + exit callback rebinding.
    const updates: string[] = []
    const exits: string[] = []
    runtime.attach(
      {
        sessionUpdate: async (n) => {
          updates.push(n.sessionId)
        },
        requestPermission: async () => ({
          outcome: { outcome: 'selected', optionId: 'allow' },
        }),
        requestElicitation: async () => ({ action: 'accept' }),
      },
      (error) => {
        exits.push(error.message)
      },
    )
    await fake.created[0].client.sessionUpdate({
      sessionId: 'sess-1',
      update: {
        sessionUpdate: 'available_commands_update',
        availableCommands: [],
      },
    })
    assert.deepEqual(updates, ['sess-1'])
    const perm = await fake.created[0].client.requestPermission({
      sessionId: 'sess-1',
      toolCall: { toolCallId: 't1', title: 'Tool' },
      options: [],
    })
    assert.deepEqual(perm, {
      outcome: { outcome: 'selected', optionId: 'allow' },
    })

    // Recycling reverts to CONTROL_CLIENT so the old session receives nothing further.
    assert.equal(pool.recycle(runtime, launch, home, {}), true)
    await fake.created[0].client.sessionUpdate({
      sessionId: 'sess-2',
      update: {
        sessionUpdate: 'available_commands_update',
        availableCommands: [],
      },
    })
    assert.deepEqual(updates, ['sess-1'])
    const idlePerm = await fake.created[0].client.requestPermission({
      sessionId: 'sess-2',
      toolCall: { toolCallId: 't2', title: 'Tool' },
      options: [],
    })
    assert.deepEqual(idlePerm, { outcome: { outcome: 'cancelled' } })
    assert.deepEqual(exits, [])

    await pool.close()
    assert.equal(fake.created[0].closeCalled(), 1)
  } finally {
    await pool.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('StandbyRuntimePool isolates skipPermissions and session env keys and rejects cwd-dependent launches', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-keys-'))
  const fake = createFakeConnect()
  const baseLaunch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const skipLaunch: ProviderLaunch = {
    command: 'agy',
    args: ['acp', '--dangerously-skip-permissions'],
    env: isolatedEnv(home),
  }
  const relDirLaunch: ProviderLaunch = {
    command: 'agy',
    args: ['acp', '--gemini-dir', './rel-gemini'],
    env: isolatedEnv(home),
  }
  const relCmdLaunch: ProviderLaunch = {
    command: './agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const pool = new StandbyRuntimePool({
    launch: baseLaunch,
    connectAcp: fake.connect,
  })
  try {
    assert.notEqual(
      standbyPoolKey(baseLaunch, {}),
      standbyPoolKey(skipLaunch, {}),
    )
    assert.notEqual(
      standbyPoolKey(baseLaunch, { CUSTOM: 'a' }),
      standbyPoolKey(baseLaunch, { CUSTOM: 'b' }),
    )
    assert.equal(isLaunchCwdIndependent(home, baseLaunch, {}), true)
    assert.equal(isLaunchCwdIndependent(home, relDirLaunch, {}), false)
    assert.equal(isLaunchCwdIndependent(home, relCmdLaunch, {}), false)

    // Cwd-dependent launches never warm or take.
    pool.warm(relDirLaunch, home, {})
    pool.warm(relCmdLaunch, home, {})
    assert.equal(fake.created.length, 0)
    assert.equal(await pool.take(relDirLaunch, home, {}), undefined)
    assert.equal(await pool.take(relCmdLaunch, home, {}), undefined)

    // Warm base launch with empty env: skipPermissions and distinct session env cannot take it.
    pool.warm(baseLaunch, home, {})
    assert.equal(fake.created.length, 1)
    assert.equal(await pool.take(skipLaunch, home, {}), undefined)
    assert.equal(await pool.take(baseLaunch, home, { FOO: '1' }), undefined)

    // Warm distinct session env and skipPermissions slots independently.
    pool.warm(baseLaunch, home, { FOO: '1' })
    pool.warm(skipLaunch, home, {})
    assert.equal(fake.created.length, 3)

    const baseRt = await pool.take(baseLaunch, home, {})
    const envRt = await pool.take(baseLaunch, home, { FOO: '1' })
    const skipRt = await pool.take(skipLaunch, home, {})
    assert.equal(baseRt?.transport, fake.created[0].transport)
    assert.equal(envRt?.transport, fake.created[1].transport)
    assert.equal(skipRt?.transport, fake.created[2].transport)

    // Recycling under a mismatched key or cwd-dependent launch is rejected.
    assert.equal(pool.recycle(baseRt!, skipLaunch, home, {}), false)
    assert.equal(pool.recycle(baseRt!, baseLaunch, home, { FOO: '1' }), false)
    assert.equal(pool.recycle(baseRt!, relDirLaunch, home, {}), false)

    await Promise.all([
      baseRt!.transport.close(),
      envRt!.transport.close(),
      skipRt!.transport.close(),
    ])
  } finally {
    await pool.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('StandbyRuntimePool evicts dead standby and failed warmup cleanly so take falls back to undefined', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-fail-'))
  const fake = createFakeConnect([
    { init: async () => ({ agentInfo: { version: '1.5.9' } }) },
    { init: async () => ({ agentInfo: { version: '1.5.12' } }) },
    { init: async () => ({ agentInfo: { version: '1.5.12' } }) },
  ])
  const manual = createManualScheduler()
  const launch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const pool = new StandbyRuntimePool({
    launch,
    connectAcp: fake.connect,
    idleTimeoutMs: 60_000,
    scheduler: manual.scheduler,
  })
  try {
    // 1. Failed warmup (unsupported version) closes transport and yields undefined on take.
    pool.warm(launch, home, {})
    assert.equal(await pool.take(launch, home, {}), undefined)
    assert.equal(fake.created[0].closeCalled(), 1)

    // 2. Healthy standby that exits while idle clears its timer, evicts itself, and yields undefined.
    pool.warm(launch, home, {})
    const readyRt = await pool.take(launch, home, {})
    assert.ok(readyRt)
    assert.equal(pool.recycle(readyRt, launch, home, {}), true)
    assert.equal(manual.scheduled.length, 1)
    assert.equal(manual.scheduled[0].unreffed, true)

    fake.created[1].triggerExit(new Error('unexpected standby crash'))
    assert.equal(manual.scheduled[0].cleared, true)
    assert.equal(await pool.take(launch, home, {}), undefined)

    // 3. Subsequent warm recovers cleanly.
    pool.warm(launch, home, {})
    const recovered = await pool.take(launch, home, {})
    assert.equal(recovered?.transport, fake.created[2].transport)
    await recovered!.transport.close()
  } finally {
    await pool.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('StandbyRuntimePool enforces maxSlots LRU eviction and unreffed idle reaping', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-evict-'))
  const fake = createFakeConnect()
  const manual = createManualScheduler()
  const launch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const pool = new StandbyRuntimePool({
    launch,
    connectAcp: fake.connect,
    maxSlots: 2,
    idleTimeoutMs: 45_000,
    scheduler: manual.scheduler,
  })
  try {
    pool.warm(launch, home, { SLOT: '1' })
    pool.warm(launch, home, { SLOT: '2' })
    assert.equal(fake.created.length, 2)

    // Touch slot 1 via take + recycle so slot 2 becomes oldest (LRU).
    const slot1 = await pool.take(launch, home, { SLOT: '1' })
    assert.equal(slot1?.transport, fake.created[0].transport)
    assert.equal(pool.recycle(slot1!, launch, home, { SLOT: '1' }), true)

    // Adding slot 3 evicts and closes slot 2 (oldest idle).
    pool.warm(launch, home, { SLOT: '3' })
    assert.equal(fake.created.length, 3)
    await fake.created[1].closedBarrier.promise
    assert.equal(fake.created[1].closeCalled(), 1)
    assert.equal(await pool.take(launch, home, { SLOT: '2' }), undefined)

    // Idle timer on recycled slot 1 is unreffed and closes slot 1 when fired.
    const slot1Timer = manual.scheduled.find(
      (s) => !s.cleared && s.ms === 45_000,
    )
    assert.ok(slot1Timer)
    assert.equal(slot1Timer.unreffed, true)
    slot1Timer.fn()
    await fake.created[0].closedBarrier.promise
    assert.equal(fake.created[0].closeCalled(), 1)
    assert.equal(await pool.take(launch, home, { SLOT: '1' }), undefined)

    // Slot 3 is still available and taking it cancels its idle timer.
    const slot3 = await pool.take(launch, home, { SLOT: '3' })
    assert.equal(slot3?.transport, fake.created[2].transport)
    await slot3!.transport.close()
  } finally {
    await pool.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('StandbyRuntimePool.close shuts down idle, recycled, and in-flight-warming transports cleanly', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-close-'))
  const warmGate = deferred<{ agentInfo?: { version?: string } }>()
  const warmEntered = deferred<void>()
  const fake = createFakeConnect([
    {},
    {
      init: () => {
        warmEntered.resolve()
        return warmGate.promise
      },
    },
  ])
  const manual = createManualScheduler()
  const launch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: isolatedEnv(home),
  }
  const pool = new StandbyRuntimePool({
    launch,
    connectAcp: fake.connect,
    idleTimeoutMs: 60_000,
    scheduler: manual.scheduler,
  })
  try {
    // Ready standby (slot A) + in-flight warming standby (slot B).
    pool.warm(launch, home, { SLOT: 'A' })
    const rtA = await pool.take(launch, home, { SLOT: 'A' })
    assert.ok(rtA)
    assert.equal(pool.recycle(rtA, launch, home, { SLOT: 'A' }), true)

    pool.warm(launch, home, { SLOT: 'B' })
    await warmEntered.promise
    const pendingTakeB = pool.take(launch, home, { SLOT: 'B' })

    const closing = pool.close()
    warmGate.resolve({ agentInfo: { version: '1.5.12' } })
    await closing

    assert.equal(await pendingTakeB, undefined)
    assert.equal(fake.created[0].closeCalled(), 1)
    assert.equal(fake.created[1].closeCalled(), 1)
    assert.equal(
      manual.scheduled.every((s) => s.cleared),
      true,
    )

    // Idempotent close and post-close no-ops.
    await pool.close()
    pool.warm(launch, home, { SLOT: 'C' })
    assert.equal(fake.created.length, 2)
    assert.equal(await pool.take(launch, home, { SLOT: 'C' }), undefined)
  } finally {
    await pool.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('clean unprompted draft open+close recycles its PID into standby, removes temp artifacts while alive, and reuses it for new and persisted opens without stale bleed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-recycle-'))
  const rpcPidLog = join(home, 'rpc-pids.log')
  const appData = dataDirectory(undefined, { HOME: home })
  await mkdir(join(appData, 'conversations'), { recursive: true })
  await mkdir(join(appData, 'brain'), { recursive: true })
  await writeFile(join(appData, 'conversations', 'summary.db'), 'keep')
  const h = await createMockProvider(home, {
    AGY_TEST_RPC_PID_LOG: rpcPidLog,
    AGY_TEST_ARTIFACTS_DIR: appData,
    AGY_TEST_STALE_EVENTS: '1',
  })
  const baseConfig = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  try {
    // Draft #1 opens cold, closes unprompted -> recycles PID into standby and cleans artifacts while alive.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-draft-1',
      sessionId: 'draft-1',
      config: baseConfig,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'draft-1')
    const [draft1Pid] = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')
    assertProcessAlive(draft1Pid)

    await h.provider.send({
      type: 'session.close',
      requestId: 'close-draft-1',
      sessionId: 'draft-1',
    })
    await h.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'draft-1',
    )
    assertProcessAlive(draft1Pid)
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])

    // Draft #2 checks out the EXACT SAME recycled PID, closes, and recycles it again.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-draft-2',
      sessionId: 'draft-2',
      config: baseConfig,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'draft-2')
    assert.deepEqual(uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new'), [
      draft1Pid,
    ])
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-draft-2',
      sessionId: 'draft-2',
    })
    await h.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'draft-2',
    )
    assertProcessAlive(draft1Pid)
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])

    // Persisted session.open checks out that EXACT SAME recycled PID for session/load.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-persisted',
      sessionId: 'persisted-1',
      config: baseConfig,
      persistence: {
        version: 1,
        data: { sessionId: '11111111-1111-4111-8111-111111111111' },
      },
      history: 'skip',
    })
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'persisted-1',
    )
    assert.deepEqual(uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'load'), [
      draft1Pid,
    ])
    assert.equal(
      h.events.some(
        (e) =>
          e.type === 'session.permission' ||
          e.type === 'timeline.item' ||
          e.type === 'session.usage',
      ),
      false,
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('unprompted session with configured MCP servers or session/close error does not recycle and stops process before cleanup', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-norecycle-'))
  const rpcPidLog = join(home, 'rpc-pids.log')
  const closeErrorFlag = join(home, 'close-err')
  const appData = dataDirectory(undefined, { HOME: home })
  await mkdir(join(appData, 'conversations'), { recursive: true })
  await mkdir(join(appData, 'brain'), { recursive: true })
  await writeFile(join(appData, 'conversations', 'summary.db'), 'keep')
  const h = await createMockProvider(home, {
    AGY_TEST_RPC_PID_LOG: rpcPidLog,
    AGY_TEST_ARTIFACTS_DIR: appData,
    AGY_TEST_CLOSE_ERROR_FLAG: closeErrorFlag,
  })
  try {
    // 1. Session with MCP servers must stop on close rather than recycling.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-mcp',
      sessionId: 'mcp-1',
      config: {
        cwd: home,
        env: {},
        mcpServers: {
          demo: { type: 'http', url: 'https://example.invalid/mcp' },
        },
        settings: {},
        persist: true,
      },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'mcp-1')
    const [mcpPid] = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-mcp',
      sessionId: 'mcp-1',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 'mcp-1')
    assertProcessExited(mcpPid)
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])

    // 2. Session whose session/close fails must stop before artifact cleanup rather than recycling.
    await writeFile(closeErrorFlag, '1')
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-err',
      sessionId: 'err-1',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'err-1')
    const newPids = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')
    assert.equal(newPids.length, 2)
    const errPid = newPids[1]
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-err',
      sessionId: 'err-1',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 'err-1')
    assertProcessExited(errPid)
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('prompted sessions do not recycle, first prompt and persisted open replenish non-full-access standby, and replaceRuntime consumes standby', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-replenish-'))
  const rpcPidLog = join(home, 'rpc-pids.log')
  const env = isolatedEnv(home, { AGY_TEST_RPC_PID_LOG: rpcPidLog })
  const launch: ProviderLaunch = {
    command: process.execPath,
    args: [mockAcpPath],
    env,
  }
  const pool = new StandbyRuntimePool({ launch })
  const collector = createEventCollector()
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    standby: pool,
  })
  const baseConfig = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  try {
    // 1. Full-access session open + prompt must NOT replenish a standby runtime.
    await manager.handle({
      type: 'session.open',
      requestId: 'open-fa',
      sessionId: 'fa',
      config: { ...baseConfig, mode: 'full-access' },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'fa',
    )
    await manager.handle({
      type: 'session.prompt',
      sessionId: 'fa',
      prompt: {
        clientMessageId: 'fa-p1',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hi' }] },
      },
    })
    await collector.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 'fa' &&
        e.state === 'completed',
    )
    assert.equal(await pool.take(launch, home, {}), undefined)
    await manager.handle({
      type: 'session.close',
      requestId: 'close-fa',
      sessionId: 'fa',
    })
    await collector.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'fa',
    )

    // 2. Normal session first prompt replenishes standby; closing prompted session stops its own PID.
    await manager.handle({
      type: 'session.open',
      requestId: 'open-norm',
      sessionId: 'norm',
      config: baseConfig,
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'norm',
    )
    const normPid = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')[1]
    const normPrompt = manager.handle({
      type: 'session.prompt',
      sessionId: 'norm',
      prompt: {
        clientMessageId: 'norm-p1',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    const perm = await collector.wait(
      (e) => e.type === 'session.permission' && e.sessionId === 'norm',
    )
    assert.ok(perm.type === 'session.permission')
    await manager.handle({
      type: 'session.permission',
      sessionId: 'norm',
      permissionId: perm.request.id,
      response: { behavior: 'allow', selectedActionId: 'yes' },
    })
    await normPrompt
    await collector.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 'norm' &&
        e.state === 'completed',
    )
    await manager.handle({
      type: 'session.close',
      requestId: 'close-norm',
      sessionId: 'norm',
    })
    await collector.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'norm',
    )
    assertProcessExited(normPid)

    // The replenished standby from norm's first prompt is checked out by a persisted open,
    // which in turn replenishes a new standby!
    const warmedFromPrompt = await pool.take(launch, home, {})
    assert.ok(warmedFromPrompt)
    assert.equal(pool.recycle(warmedFromPrompt, launch, home, {}), true)
    const initPidsBeforePersisted = uniqueMethodPids(
      await readRpcPidLog(rpcPidLog),
      'initialize',
    )
    await manager.handle({
      type: 'session.open',
      requestId: 'open-persisted',
      sessionId: 'persisted',
      config: baseConfig,
      persistence: {
        version: 1,
        data: { sessionId: '11111111-1111-4111-8111-111111111111' },
      },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'persisted',
    )
    const persistedLoadPid = uniqueMethodPids(
      await readRpcPidLog(rpcPidLog),
      'load',
    )[0]
    assert.notEqual(persistedLoadPid, normPid)
    assert.ok(initPidsBeforePersisted.includes(persistedLoadPid))

    // 3. replaceRuntime() (full-access -> default) consumes the standby replenished by persisted open!
    const replenishedBeforeRelaunch = await pool.take(launch, home, {})
    assert.ok(replenishedBeforeRelaunch)
    assert.equal(
      pool.recycle(replenishedBeforeRelaunch, launch, home, {}),
      true,
    )
    const standbyPidForRelaunch = uniqueMethodPids(
      await readRpcPidLog(rpcPidLog),
      'initialize',
    ).at(-1)!
    assert.notEqual(standbyPidForRelaunch, persistedLoadPid)

    await manager.handle({
      type: 'session.open',
      requestId: 'open-fa2',
      sessionId: 'fa2',
      config: { ...baseConfig, mode: 'full-access' },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'fa2',
    )
    await manager.handle({
      type: 'session.configure',
      requestId: 'cfg-default',
      sessionId: 'fa2',
      changes: { mode: 'default' },
    })
    await collector.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'cfg-default',
    )
    const loadPids = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'load')
    assert.equal(loadPids.includes(standbyPidForRelaunch), true)
  } finally {
    await Promise.all([manager.close(), pool.close()])
    await rm(home, { recursive: true, force: true })
  }
})

test('ProviderConnection.close shuts down active sessions, shared control runtime, and standby pool cleanly', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-provider-shutdown-'))
  const pidLog = join(home, 'pids.log')
  const h = await createMockProvider(home, { AGY_TEST_PID_LOG: pidLog })
  const baseConfig = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  try {
    // 1. Catalog starts shared control runtime + warms standby.
    await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
    await h.wait((e) => e.type === 'catalog' && e.requestId === 'cat-1')

    // 2. Persisted open takes standby and triggers a fresh background standby warmup.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-p1',
      sessionId: 'p1',
      config: baseConfig,
      persistence: {
        version: 1,
        data: { sessionId: '11111111-1111-4111-8111-111111111111' },
      },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'p1')

    // 3. Second active session runs concurrently.
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-s2',
      sessionId: 's2',
      config: baseConfig,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's2')

    const pidsBeforeClose = await readPids(pidLog)
    assert.ok(pidsBeforeClose.length >= 3)
    pidsBeforeClose.forEach(assertProcessAlive)

    await h.provider.close()
    const allPids = await readPids(pidLog)
    allPids.forEach(assertProcessExited)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

function createControlledSessionConnect(
  scripts: readonly {
    newSessionId?: string
    onSetConfigOption?: () => Promise<void>
    onListSessions?: () => Promise<void>
    onClose?: () => Promise<void>
  }[],
) {
  const created: Array<{
    closeCalled: () => number
    triggerExit: (error: Error) => void
  }> = []
  const connect: typeof connectAcp = (_launch, _cwd, _env, client, onExit) => {
    const script = scripts[created.length] ?? {}
    const closedBarrier = deferred<void>()
    let isClosed = false
    let closeCalls = 0
    let closing: Promise<void> | undefined
    const configOptions = [
      {
        id: 'model',
        name: 'Model',
        type: 'select' as const,
        currentValue: 'test',
        options: [
          { value: 'test', name: 'Test' },
          { value: 'other', name: 'Other' },
        ],
      },
      {
        id: 'mode',
        name: 'Mode',
        category: 'mode' as const,
        type: 'select' as const,
        currentValue: 'default',
        options: [
          { value: 'default', name: 'Default' },
          { value: 'accept-edits', name: 'Accept edits' },
        ],
      },
    ]
    const rpc = {
      closed: closedBarrier.promise,
      initialize: async () => ({
        protocolVersion: 1,
        agentInfo: { name: 'Antigravity ACP Server', version: '1.5.12' },
        agentCapabilities: {
          loadSession: true,
          sessionCapabilities: { list: {}, resume: {} },
        },
      }),
      newSession: async () => {
        const sessionId =
          script.newSessionId ?? '11111111-1111-4111-8111-111111111111'
        void client.sessionUpdate({
          sessionId,
          update: {
            sessionUpdate: 'available_commands_update',
            availableCommands: [],
          },
        })
        return { sessionId, configOptions }
      },
      loadSession: async () => ({ configOptions }),
      resumeSession: async () => ({ configOptions }),
      listSessions: async () => {
        if (script.onListSessions) await script.onListSessions()
        return { sessions: [] }
      },
      closeSession: async () => ({}),
      setSessionConfigOption: async (params: {
        configId: string
        value: string
      }) => {
        if (script.onSetConfigOption) await script.onSetConfigOption()
        return {
          configOptions: configOptions.map((o) =>
            o.id === params.configId ? { ...o, currentValue: params.value } : o,
          ),
        }
      },
      prompt: async () => ({ stopReason: 'end_turn' as const }),
      cancel: async () => {},
    } as unknown as ControlTransport['rpc']
    const transport: ControlTransport = {
      rpc,
      exited: closedBarrier.promise,
      stopped: closedBarrier.promise,
      close: () => {
        if (closing) return closing
        closeCalls++
        isClosed = true
        closing = (async () => {
          if (script.onClose) await script.onClose()
          closedBarrier.resolve()
        })()
        return closing
      },
      request: async (op) => op,
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
    created.push({
      closeCalled: () => closeCalls,
      triggerExit: (error) => {
        isClosed = true
        onExit(error)
      },
    })
    return transport
  }
  return { connect, created }
}

test('replaceRuntime racing with session.close awaits closing checked-out standby transport', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-replace-close-race-'))
  const env = isolatedEnv(home)
  const launch: ProviderLaunch = { command: 'agy', args: ['acp'], env }
  const warmupEntered = deferred<void>()
  const releaseWarmup = deferred<void>()
  const takeEntered = deferred<void>()
  const standbyCloseEntered = deferred<void>()
  const releaseStandbyClose = deferred<void>()
  const fake = createControlledSessionConnect([
    // #1: initial full-access session
    {},
    // #2: standby runtime for default mode, paused during warmup and close
    {
      onListSessions: async () => {
        warmupEntered.resolve()
        await releaseWarmup.promise
      },
      onClose: async () => {
        standbyCloseEntered.resolve()
        await releaseStandbyClose.promise
      },
    },
  ])
  const onTake = [() => {}, () => takeEntered.resolve()]
  class ObservingStandbyPool extends StandbyRuntimePool {
    override take(...args: Parameters<StandbyRuntimePool['take']>) {
      onTake.shift()?.()
      return super.take(...args)
    }
  }
  const pool = new ObservingStandbyPool({ launch, connectAcp: fake.connect })
  const collector = createEventCollector()
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    connectAcp: fake.connect,
    standby: pool,
  })
  try {
    await manager.handle({
      type: 'session.open',
      requestId: 'open-fa',
      sessionId: 'fa',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
        mode: 'full-access',
      },
      history: 'skip',
    })
    await collector.wait((e) => e.type === 'session.ready')

    // Start warming default standby; leave its listSessions pending so replaceRuntime awaits take().
    pool.warm(launch, home, {})
    await warmupEntered.promise

    // Toggle full-access -> default triggers replaceRuntime(), which enters standby.take().
    let configureDone = false
    const configuring = manager
      .handle({
        type: 'session.configure',
        requestId: 'cfg-default',
        sessionId: 'fa',
        changes: { mode: 'default' },
      })
      .finally(() => {
        configureDone = true
      })
    void configuring.catch(() => undefined)
    await takeEntered.promise

    // Close the session while replaceRuntime is awaiting standby.take().
    await manager.handle({
      type: 'session.close',
      requestId: 'close-fa',
      sessionId: 'fa',
    })
    releaseWarmup.resolve()

    // replaceRuntime must await standby transport close before settling.
    await standbyCloseEntered.promise
    assert.equal(configureDone, false)
    releaseStandbyClose.resolve()
    await assert.rejects(configuring, /Session is closing/)
    assert.equal(fake.created[1].closeCalled(), 1)
  } finally {
    await Promise.all([manager.close(), pool.close()])
    await rm(home, { recursive: true, force: true })
  }
})

test('unprompted full-access and --dangerously-skip-permissions drafts never recycle into StandbyRuntimePool', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-standby-no-fullaccess-'))
  const rpcPidLog = join(home, 'rpc-pids.log')
  const env = isolatedEnv(home, { AGY_TEST_RPC_PID_LOG: rpcPidLog })
  const baseLaunch: ProviderLaunch = {
    command: process.execPath,
    args: [mockAcpPath],
    env,
  }
  const skipLaunch: ProviderLaunch = {
    command: process.execPath,
    args: [mockAcpPath, '--dangerously-skip-permissions'],
    env,
  }
  const pool = new StandbyRuntimePool({ launch: baseLaunch })
  const skipPool = new StandbyRuntimePool({ launch: skipLaunch })
  const collector = createEventCollector()
  const manager = new SessionManager({
    launch: baseLaunch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    standby: pool,
  })
  const skipManager = new SessionManager({
    launch: skipLaunch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    standby: skipPool,
  })
  try {
    // 1. Session opened in full-access mode must stop on close rather than recycling.
    await manager.handle({
      type: 'session.open',
      requestId: 'open-fa',
      sessionId: 'fa',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
        mode: 'full-access',
      },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'fa',
    )
    const [faPid] = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')
    await manager.handle({
      type: 'session.close',
      requestId: 'close-fa',
      sessionId: 'fa',
    })
    await collector.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'fa',
    )
    assertProcessExited(faPid)
    assert.equal(await pool.take(skipLaunch, home, {}), undefined)
    assert.equal(await pool.take(baseLaunch, home, {}), undefined)

    // 2. Connection-level --dangerously-skip-permissions draft must also stop rather than recycling.
    await skipManager.handle({
      type: 'session.open',
      requestId: 'open-skip',
      sessionId: 'skip-draft',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'skip-draft',
    )
    const skipPid = uniqueMethodPids(await readRpcPidLog(rpcPidLog), 'new')[1]
    await skipManager.handle({
      type: 'session.close',
      requestId: 'close-skip',
      sessionId: 'skip-draft',
    })
    await collector.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'skip-draft',
    )
    assertProcessExited(skipPid)
    assert.equal(await skipPool.take(skipLaunch, home, {}), undefined)
  } finally {
    await Promise.all([
      manager.close(),
      skipManager.close(),
      pool.close(),
      skipPool.close(),
    ])
    await rm(home, { recursive: true, force: true })
  }
})

test('draft session.close while session.configure is in flight stops instead of recycling', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-close-during-config-'))
  const env = isolatedEnv(home)
  const launch: ProviderLaunch = { command: 'agy', args: ['acp'], env }
  const cfgEntered = deferred<void>()
  const releaseCfg = deferred<void>()
  const fake = createControlledSessionConnect([
    {
      onSetConfigOption: async () => {
        cfgEntered.resolve()
        await releaseCfg.promise
      },
    },
  ])
  const pool = new StandbyRuntimePool({ launch, connectAcp: fake.connect })
  const collector = createEventCollector()
  const manager = new SessionManager({
    launch,
    usage: new AgyUsageSource({ env }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    connectAcp: fake.connect,
    standby: pool,
  })
  try {
    await manager.handle({
      type: 'session.open',
      requestId: 'open-draft',
      sessionId: 'draft',
      config: {
        cwd: home,
        env: {},
        mcpServers: {},
        settings: {},
        persist: true,
      },
      history: 'skip',
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'draft',
    )

    const configuring = manager
      .handle({
        type: 'session.configure',
        requestId: 'cfg-model',
        sessionId: 'draft',
        changes: { model: 'other' },
      })
      .catch(() => undefined)
    await cfgEntered.promise

    await manager.handle({
      type: 'session.close',
      requestId: 'close-draft',
      sessionId: 'draft',
    })
    await collector.wait(
      (e) => e.type === 'session.closed' && e.sessionId === 'draft',
    )
    releaseCfg.resolve()
    await configuring

    assert.equal(fake.created[0].closeCalled(), 1)
    assert.equal(await pool.take(launch, home, {}), undefined)
  } finally {
    await Promise.all([manager.close(), pool.close()])
    await rm(home, { recursive: true, force: true })
  }
})

test('isLaunchCwdIndependent and StandbyRuntimePool resolve USERPROFILE and ignore empty HOME strings', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-runtime-userprofile-'))
  const winHome = join(root, 'win-home')
  const workspace = join(root, 'workspace')
  await mkdir(winHome, { recursive: true })
  await mkdir(workspace, { recursive: true })
  const seenCwds: string[] = []
  const fake = createControlledSessionConnect([{}])
  const trackingConnect: typeof connectAcp = (
    launch,
    cwd,
    env,
    client,
    onExit,
  ) => {
    seenCwds.push(cwd)
    return fake.connect(launch, cwd, env, client, onExit)
  }
  const launch: ProviderLaunch = {
    command: 'agy',
    args: ['acp'],
    env: { HOME: '', USERPROFILE: winHome },
  }
  const pool = new StandbyRuntimePool({ launch, connectAcp: trackingConnect })
  try {
    assert.equal(isLaunchCwdIndependent(workspace, launch, {}), true)
    assert.equal(
      isLaunchCwdIndependent(workspace, {
        command: 'agy',
        args: ['acp'],
        env: { HOME: '' },
      }),
      true,
    )
    pool.warm(launch, workspace, {})
    const warmed = await pool.take(launch, workspace, {})
    assert.ok(warmed)
    assert.deepEqual(seenCwds, [winHome])
    await warmed.transport.close()
  } finally {
    await pool.close()
    await rm(root, { recursive: true, force: true })
  }
})
