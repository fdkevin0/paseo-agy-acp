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
  type ControlTransport,
  type IdleTimerHandle,
  type IdleTimerScheduler,
} from '../server/runtime.ts'
import type { connectAcp } from '../server/transport.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { collectProviderEvents, isolatedEnv } from './helpers.ts'

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
  readonly cwd: string
  readonly transport: ControlTransport
  readonly closedBarrier: ReturnType<typeof deferred<void>>
  readonly closeCalled: () => number
  readonly triggerExit: (error: Error) => void
  setObserved: (next: {
    models?: ControlTransport['models']
    tier?: ControlTransport['tier']
  }) => void
}

function createFakeConnect(
  scripts?: readonly {
    init?: () => Promise<{ agentInfo?: { version?: string } }>
    close?: () => Promise<void>
  }[],
) {
  const created: FakeTransportRecord[] = []
  const connect: typeof connectAcp = (_launch, cwd, _env, _client, onExit) => {
    const id = created.length + 1
    const script = scripts?.[id - 1]
    const closedBarrier = deferred<void>()
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
          : { agentInfo: { version: '1.5.12' } }
        return { protocolVersion: 1, ...result }
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
      cwd,
      transport,
      closedBarrier,
      closeCalled: () => closeCalls,
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
  const callLog = join(home, 'calls.log')
  const appData = dataDirectory(undefined, { HOME: home })
  await mkdir(join(appData, 'conversations'), { recursive: true })
  await mkdir(join(appData, 'brain'), { recursive: true })
  await writeFile(join(appData, 'conversations', 'summary.db'), 'keep')
  const h = await createMockProvider(home, {
    AGY_TEST_PID_LOG: pidLog,
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

    const pids = await readPids(pidLog)
    assert.equal(pids.length, 1)
    assertProcessAlive(pids[0])
    assert.equal(await readFile(callLog, 'utf8'), 'new\nclose\nnew\nclose\n')
    assert.deepEqual(await readdir(join(appData, 'conversations')), [
      'summary.db',
    ])
    assert.deepEqual(await readdir(join(appData, 'brain')), [])

    await h.provider.close()
    assertProcessExited(pids[0])
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
      AGY_TEST_PID_LOG: pidLog,
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
      assert.equal((await readPids(pidLog)).length, 1)
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

    const h = await createMockProvider(home, {
      AGY_TEST_PID_LOG: pidLog,
      AGY_TEST_CRASH_ON_LIST_FLAG: crashFlag,
    })
    try {
      await h.provider.send({ type: 'catalog', requestId: 'cat-1', cwd: home })
      await h.wait((e) => e.type === 'catalog' && e.requestId === 'cat-1')
      const [pid1] = await readPids(pidLog)

      await writeFile(crashFlag, '1')
      await h.provider.send({
        type: 'sessions',
        requestId: 'list-crash',
        cwd: home,
      })
      await h.wait(
        (e) => e.type === 'request.failed' && e.requestId === 'list-crash',
      )

      await h.provider.send({
        type: 'sessions',
        requestId: 'list-recover',
        cwd: home,
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

      const pids = await readPids(pidLog)
      assert.equal(pids.length, 2)
      assert.notEqual(pids[1], pid1)
      assertProcessAlive(pids[1])
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
    AGY_TEST_PID_LOG: pidLog,
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

    const [pid1] = await readPids(pidLog)
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

    const pids = await readPids(pidLog)
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
