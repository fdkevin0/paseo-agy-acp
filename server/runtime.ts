/** Shared ACP initialization, idle-reaped control runtime, and pre-warmed standby runtime pool. */
import { createHash } from 'node:crypto'
import { statSync } from 'node:fs'
import { join } from 'node:path'
import { PROTOCOL_VERSION } from '@agentclientprotocol/sdk/unstable'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'
import { PACKAGE_VERSION } from './common.ts'
import {
  agyVersionDiagnostic,
  isCwdIndependentLaunch,
  resolveHome,
} from './preflight.ts'
import { connectAcp, type AcpClient } from './transport.ts'

export type ControlTransport = ReturnType<typeof connectAcp>
export type InitializedAcp = Awaited<ReturnType<typeof initializeAcp>>

export const SKIP_PERMISSIONS_FLAG = '--dangerously-skip-permissions'

export interface ControlLease {
  readonly transport: ControlTransport
  readonly reusable: boolean
  retire(): Promise<void>
}

export interface IdleTimerHandle {
  unref?: () => void
}

export interface IdleTimerScheduler {
  setTimeout(fn: () => void, ms: number): IdleTimerHandle
  clearTimeout(handle: IdleTimerHandle): void
}

export interface SharedControlRuntimeOptions {
  launch?: ProviderLaunch
  connectAcp?: typeof connectAcp
  idleTimeoutMs?: number
  scheduler?: IdleTimerScheduler
}

export interface StandbyRuntimePoolOptions {
  launch?: ProviderLaunch
  connectAcp?: typeof connectAcp
  idleTimeoutMs?: number
  maxSlots?: number
  scheduler?: IdleTimerScheduler
}

export interface AttachableRuntime {
  readonly transport: ControlTransport
  readonly initialized: InitializedAcp
  readonly key: string
  readonly reusable: boolean
  attach(client: AcpClient, onExit: (error: Error) => void): void
}

const DEFAULT_IDLE_TIMEOUT_MS = 5 * 60_000
const DEFAULT_MAX_STANDBY_SLOTS = 4

const defaultScheduler: IdleTimerScheduler = {
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (handle) => clearTimeout(handle as NodeJS.Timeout),
}

export const CONTROL_CLIENT: AcpClient = {
  sessionUpdate: async () => {},
  requestPermission: async () => ({
    outcome: { outcome: 'cancelled' as const },
  }),
  requestElicitation: async () => ({ action: 'cancel' }),
}

/** Whether a launch configuration includes `--dangerously-skip-permissions`. */
export function hasSkipPermissionsFlag(launch?: ProviderLaunch): boolean {
  return Boolean(launch?.args?.includes(SKIP_PERMISSIONS_FLAG))
}

/** Whether a single ACP process is valid across working directories for this launch and env. */
export function isLaunchCwdIndependent(
  spawnCwd: string,
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
): boolean {
  const fallbackHome = resolveHome(env, launch?.env)
  return (
    isCwdIndependentLaunch(spawnCwd, fallbackHome, launch, env, fallbackHome) &&
    isCwdIndependentLaunch(
      spawnCwd,
      join(spawnCwd, '__paseo_cwd_probe__'),
      launch,
      env,
      fallbackHome,
    )
  )
}

/** Canonical SHA-256 standby pool key covering command, args, and resolved environment. */
export function standbyPoolKey(
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
): string {
  const merged: Record<string, string | undefined> = {
    ...(launch?.env ?? process.env),
    ...env,
  }
  const sortedEnv = Object.entries(merged)
    .filter((entry): entry is [string, string] => entry[1] !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
  return createHash('sha256')
    .update(
      JSON.stringify([
        launch?.command ?? 'agy',
        launch?.args ?? ['acp'],
        sortedEnv,
      ]),
    )
    .digest('hex')
}

function isExistingDirectorySync(path: string): boolean {
  try {
    return statSync(path).isDirectory()
  } catch {
    return false
  }
}

function resolveWarmCwd(
  spawnCwd: string,
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
): string {
  const fallbackHome = resolveHome(env, launch?.env)
  if (spawnCwd === fallbackHome) return spawnCwd
  return isExistingDirectorySync(fallbackHome) ? fallbackHome : spawnCwd
}

/** Connect an ACP transport whose client and exit callbacks can be rebound without respawning. */
export function createAttachableTransport(
  connect: typeof connectAcp,
  launch: ProviderLaunch | undefined,
  cwd: string,
  env: Readonly<Record<string, string>>,
  initialClient: AcpClient,
  initialOnExit: (error: Error) => void,
): {
  transport: ControlTransport
  bind: (client: AcpClient, onExit: (error: Error) => void) => void
} {
  const binding = {
    client: initialClient,
    onExit: initialOnExit,
  }
  const transport = connect(
    launch,
    cwd,
    env,
    {
      sessionUpdate: (params) => binding.client.sessionUpdate(params),
      requestPermission: (params) => binding.client.requestPermission(params),
      requestElicitation: (params) =>
        binding.client.requestElicitation
          ? binding.client.requestElicitation(params)
          : Promise.resolve({ action: 'cancel' }),
    },
    (error) => {
      binding.onExit(error)
    },
  )
  return {
    transport,
    bind: (client, onExit) => {
      binding.client = client
      binding.onExit = onExit
    },
  }
}

/** Run ACP `initialize` and reject runtimes below the minimum supported `agy` version. */
export async function initializeAcp(transport: ControlTransport) {
  const initialized = await transport.request(
    transport.rpc.initialize({
      protocolVersion: PROTOCOL_VERSION,
      clientInfo: { name: 'paseo-agy-acp', version: PACKAGE_VERSION },
      clientCapabilities: {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: false,
        elicitation: { form: {} },
      },
    }),
  )
  const diagnostic = agyVersionDiagnostic(initialized.agentInfo?.version)
  if (diagnostic) throw new Error(diagnostic)
  return initialized
}

interface SharedSlot {
  transport: ControlTransport
  ready: Promise<void>
  inFlight: number
  idleTimer: IdleTimerHandle | undefined
  retired: boolean
}

/**
 * Own one lazily started, idle-reaped `agy acp` process per cwd-independent provider connection
 * for stateless control RPCs (`catalog` and `sessions`), falling back to one-shot runtimes when
 * the launch depends on `cwd`.
 */
export class SharedControlRuntime {
  private readonly launch: ProviderLaunch | undefined
  private readonly connect: typeof connectAcp
  private readonly idleTimeoutMs: number
  private readonly scheduler: IdleTimerScheduler
  private active: SharedSlot | undefined
  private readonly transports = new Set<ControlTransport>()
  private serializedLane: Promise<void> = Promise.resolve()
  private closed = false
  private closing: Promise<void> | undefined

  constructor(options: SharedControlRuntimeOptions = {}) {
    this.launch = options.launch
    this.connect = options.connectAcp ?? connectAcp
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
    this.scheduler = options.scheduler ?? defaultScheduler
  }

  /** Whether a single shared control runtime is valid across working directories for this launch. */
  isReusable(spawnCwd: string): boolean {
    return isLaunchCwdIndependent(spawnCwd, this.launch)
  }

  async run<T>(
    spawnCwd: string,
    fn: (lease: ControlLease) => Promise<T>,
    options?: { serialize?: boolean },
  ): Promise<T> {
    if (this.closed) throw new Error('Provider connection is closed')
    if (!this.isReusable(spawnCwd)) {
      return this.runEphemeral(spawnCwd, fn)
    }
    const exec = () => this.runShared(spawnCwd, fn)
    return options?.serialize ? this.runSerialized(exec) : exec()
  }

  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    if (this.active) this.evict(this.active)
    this.closing = Promise.all(
      [...this.transports].map((transport) =>
        this.closeTransport(transport).catch(() => undefined),
      ),
    ).then(() => undefined)
    return this.closing
  }

  private runSerialized<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.serializedLane.then(fn, fn)
    this.serializedLane = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  private trackTransport(transport: ControlTransport) {
    this.transports.add(transport)
    void transport.stopped.then(
      () => this.transports.delete(transport),
      () => this.transports.delete(transport),
    )
  }

  private async closeTransport(transport: ControlTransport): Promise<void> {
    try {
      await transport.close()
    } finally {
      this.transports.delete(transport)
    }
  }

  private cancelIdleTimer(slot: SharedSlot) {
    if (slot.idleTimer !== undefined) {
      this.scheduler.clearTimeout(slot.idleTimer)
      slot.idleTimer = undefined
    }
  }

  private armIdleTimer(slot: SharedSlot) {
    if (
      this.closed ||
      slot.retired ||
      this.active !== slot ||
      slot.inFlight > 0 ||
      slot.transport.closed ||
      !Number.isFinite(this.idleTimeoutMs) ||
      this.idleTimeoutMs < 0
    )
      return
    this.cancelIdleTimer(slot)
    const timer = this.scheduler.setTimeout(() => {
      if (slot.idleTimer === timer) slot.idleTimer = undefined
      if (slot.inFlight > 0 || slot.retired) return
      this.evict(slot)
      void this.closeTransport(slot.transport).catch(() => undefined)
    }, this.idleTimeoutMs)
    timer.unref?.()
    slot.idleTimer = timer
  }

  private evict(slot: SharedSlot) {
    slot.retired = true
    this.cancelIdleTimer(slot)
    if (this.active === slot) this.active = undefined
  }

  private startShared(spawnCwd: string): SharedSlot {
    let exitError: Error | undefined
    let slot: SharedSlot | undefined
    const transport = this.connect(
      this.launch,
      spawnCwd,
      {},
      CONTROL_CLIENT,
      (error) => {
        exitError = error
        if (slot) this.evict(slot)
        void this.closeTransport(transport).catch(() => undefined)
      },
    )
    slot = {
      transport,
      ready: undefined as unknown as Promise<void>,
      inFlight: 0,
      idleTimer: undefined,
      retired: false,
    }
    this.trackTransport(transport)
    const activeSlot = slot
    activeSlot.ready = (async () => {
      try {
        await initializeAcp(transport)
        if (this.closed) throw new Error('Provider connection is closed')
        if (activeSlot.retired || transport.closed)
          throw (
            exitError ?? new Error('ACP control runtime closed during startup')
          )
      } catch (error) {
        this.evict(activeSlot)
        await this.closeTransport(transport).catch(() => undefined)
        throw error
      }
    })()
    // Prevent unhandled rejection if startup fails after all waiters detach.
    void activeSlot.ready.catch(() => undefined)
    return activeSlot
  }

  private async acquireShared(spawnCwd: string): Promise<SharedSlot> {
    while (true) {
      if (this.closed) throw new Error('Provider connection is closed')
      let slot = this.active
      if (!slot || slot.retired || slot.transport.closed) {
        slot = this.startShared(spawnCwd)
        this.active = slot
      }
      this.cancelIdleTimer(slot)
      slot.inFlight++
      try {
        await slot.ready
      } catch (error) {
        slot.inFlight--
        throw error
      }
      if (this.closed) {
        slot.inFlight--
        throw new Error('Provider connection is closed')
      }
      if (slot.retired || slot.transport.closed) {
        slot.inFlight--
        if (this.active === slot) this.evict(slot)
        continue
      }
      return slot
    }
  }

  private async runShared<T>(
    spawnCwd: string,
    fn: (lease: ControlLease) => Promise<T>,
  ): Promise<T> {
    const slot = await this.acquireShared(spawnCwd)
    const lease: ControlLease = {
      transport: slot.transport,
      reusable: true,
      retire: () => {
        this.evict(slot)
        return this.closeTransport(slot.transport)
      },
    }
    try {
      return await fn(lease)
    } finally {
      slot.inFlight--
      if (slot.inFlight === 0) this.armIdleTimer(slot)
    }
  }

  private async runEphemeral<T>(
    spawnCwd: string,
    fn: (lease: ControlLease) => Promise<T>,
  ): Promise<T> {
    const transport = this.connect(
      this.launch,
      spawnCwd,
      {},
      CONTROL_CLIENT,
      () => {},
    )
    this.trackTransport(transport)
    try {
      await initializeAcp(transport)
      if (this.closed) throw new Error('Provider connection is closed')
      const lease: ControlLease = {
        transport,
        reusable: false,
        retire: () => this.closeTransport(transport),
      }
      return await fn(lease)
    } finally {
      await this.closeTransport(transport).catch(() => undefined)
    }
  }
}

interface StandbySlot {
  readonly key: string
  readonly transport: ControlTransport
  ready: Promise<AttachableRuntime>
  idleTimer: IdleTimerHandle | undefined
  retired: boolean
  exited: boolean
  exitError: Error | undefined
  claimed: boolean
}

/**
 * Maintain a bounded, idle-reaped pool of pre-warmed `agy acp` runtimes keyed by launch
 * command/args/environment for exclusive checkout by interactive sessions on cwd-independent
 * launches.
 */
export class StandbyRuntimePool {
  private readonly launch: ProviderLaunch | undefined
  private readonly connect: typeof connectAcp
  private readonly idleTimeoutMs: number
  private readonly maxSlots: number
  private readonly scheduler: IdleTimerScheduler
  private readonly slots = new Map<string, StandbySlot>()
  private readonly transports = new Set<ControlTransport>()
  private readonly watchedStops = new WeakSet<ControlTransport>()
  private closed = false
  private closing: Promise<void> | undefined

  constructor(options: StandbyRuntimePoolOptions = {}) {
    this.launch = options.launch
    this.connect = options.connectAcp ?? connectAcp
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
    this.maxSlots = options.maxSlots ?? DEFAULT_MAX_STANDBY_SLOTS
    this.scheduler = options.scheduler ?? defaultScheduler
  }

  /** Start warming one standby runtime in the background if the slot is empty and reusable. */
  warm(
    effectiveLaunch: ProviderLaunch | undefined,
    spawnCwd: string,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): void {
    const launch = effectiveLaunch ?? this.launch
    if (this.closed || this.maxSlots <= 0) return
    if (!isLaunchCwdIndependent(spawnCwd, launch, sessionEnv)) return
    const key = standbyPoolKey(launch, sessionEnv)
    const existing = this.slots.get(key)
    if (
      existing &&
      !existing.retired &&
      !existing.exited &&
      !existing.transport.closed
    )
      return
    if (existing) this.evictSlot(existing)
    this.evictToFit()

    const warmCwd = resolveWarmCwd(spawnCwd, launch, sessionEnv)
    let slot!: StandbySlot
    const { transport, bind } = createAttachableTransport(
      this.connect,
      launch,
      warmCwd,
      sessionEnv,
      CONTROL_CLIENT,
      (error) => {
        if (slot) {
          slot.exited = true
          slot.exitError = error
          this.evictSlot(slot)
        }
        void this.closeTransport(transport).catch(() => undefined)
      },
    )
    this.trackTransport(transport)
    slot = {
      key,
      transport,
      ready: undefined as unknown as Promise<AttachableRuntime>,
      idleTimer: undefined,
      retired: false,
      exited: false,
      exitError: undefined,
      claimed: false,
    }
    this.slots.set(key, slot)
    slot.ready = (async (): Promise<AttachableRuntime> => {
      try {
        const initialized = await initializeAcp(transport)
        if (this.closed || slot.retired || slot.exited || transport.closed) {
          throw (
            slot.exitError ??
            new Error('Standby ACP runtime closed during startup')
          )
        }
        if (initialized.agentCapabilities?.sessionCapabilities?.list) {
          await transport
            .request(transport.rpc.listSessions({ cwd: warmCwd }))
            .catch(() => undefined)
        }
        if (this.closed || slot.retired || slot.exited || transport.closed) {
          throw (
            slot.exitError ??
            new Error('Standby ACP runtime closed during warmup')
          )
        }
        const runtime: AttachableRuntime = {
          transport,
          initialized,
          key,
          reusable: true,
          attach: (client, onExit) => bind(client, onExit),
        }
        if (!slot.claimed && this.slots.get(key) === slot && !this.closed) {
          this.armIdleTimer(slot)
        }
        return runtime
      } catch (error) {
        this.evictSlot(slot)
        await this.closeTransport(transport).catch(() => undefined)
        throw error
      }
    })()
    // Prevent unhandled rejection when background warmup fails or is evicted.
    void slot.ready.catch(() => undefined)
  }

  /** Atomically claim and return a pre-warmed runtime for exclusive session ownership. */
  async take(
    effectiveLaunch: ProviderLaunch | undefined,
    spawnCwd: string,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): Promise<AttachableRuntime | undefined> {
    const launch = effectiveLaunch ?? this.launch
    if (this.closed) return undefined
    if (!isLaunchCwdIndependent(spawnCwd, launch, sessionEnv)) return undefined
    const key = standbyPoolKey(launch, sessionEnv)
    const slot = this.slots.get(key)
    if (!slot) return undefined
    slot.claimed = true
    this.slots.delete(key)
    this.cancelIdleTimer(slot)
    if (slot.retired || slot.exited || slot.transport.closed) {
      await this.closeTransport(slot.transport).catch(() => undefined)
      return undefined
    }
    let runtime: AttachableRuntime
    try {
      runtime = await slot.ready
    } catch {
      await this.closeTransport(slot.transport).catch(() => undefined)
      return undefined
    }
    if (
      this.closed ||
      slot.retired ||
      slot.exited ||
      runtime.transport.closed
    ) {
      await this.closeTransport(runtime.transport).catch(() => undefined)
      return undefined
    }
    this.transports.delete(runtime.transport)
    return runtime
  }

  /** Return a clean, healthy, unprompted runtime to standby; returns `false` if caller must close it. */
  recycle(
    runtime: AttachableRuntime,
    effectiveLaunch: ProviderLaunch | undefined,
    spawnCwd: string,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): boolean {
    const launch = effectiveLaunch ?? this.launch
    if (
      this.closed ||
      this.maxSlots <= 0 ||
      !runtime.reusable ||
      runtime.transport.closed ||
      hasSkipPermissionsFlag(launch)
    )
      return false
    if (!isLaunchCwdIndependent(spawnCwd, launch, sessionEnv)) return false
    const key = standbyPoolKey(launch, sessionEnv)
    if (runtime.key !== key) return false
    const existing = this.slots.get(key)
    if (
      existing &&
      !existing.retired &&
      !existing.exited &&
      !existing.transport.closed
    )
      return false
    if (existing) this.evictSlot(existing)
    this.evictToFit()

    const slot: StandbySlot = {
      key,
      transport: runtime.transport,
      ready: Promise.resolve(runtime),
      idleTimer: undefined,
      retired: false,
      exited: false,
      exitError: undefined,
      claimed: false,
    }
    runtime.attach(CONTROL_CLIENT, (error) => {
      slot.exited = true
      slot.exitError = error
      this.evictSlot(slot)
      void this.closeTransport(runtime.transport).catch(() => undefined)
    })
    this.trackTransport(runtime.transport)
    this.slots.set(key, slot)
    this.armIdleTimer(slot)
    return true
  }

  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    for (const slot of [...this.slots.values()]) this.evictSlot(slot)
    this.slots.clear()
    this.closing = Promise.all(
      [...this.transports].map((transport) =>
        this.closeTransport(transport).catch(() => undefined),
      ),
    ).then(() => undefined)
    return this.closing
  }

  private trackTransport(transport: ControlTransport) {
    this.transports.add(transport)
    if (!this.watchedStops.has(transport)) {
      this.watchedStops.add(transport)
      void transport.stopped.then(
        () => this.transports.delete(transport),
        () => this.transports.delete(transport),
      )
    }
  }

  private async closeTransport(transport: ControlTransport): Promise<void> {
    try {
      await transport.close()
    } finally {
      this.transports.delete(transport)
    }
  }

  private cancelIdleTimer(slot: StandbySlot) {
    if (slot.idleTimer !== undefined) {
      this.scheduler.clearTimeout(slot.idleTimer)
      slot.idleTimer = undefined
    }
  }

  private armIdleTimer(slot: StandbySlot) {
    if (
      this.closed ||
      slot.retired ||
      slot.exited ||
      slot.claimed ||
      this.slots.get(slot.key) !== slot ||
      slot.transport.closed ||
      !Number.isFinite(this.idleTimeoutMs) ||
      this.idleTimeoutMs < 0
    )
      return
    this.cancelIdleTimer(slot)
    const timer = this.scheduler.setTimeout(() => {
      if (slot.idleTimer === timer) slot.idleTimer = undefined
      if (slot.claimed || slot.retired) return
      this.evictSlot(slot)
      void this.closeTransport(slot.transport).catch(() => undefined)
    }, this.idleTimeoutMs)
    timer.unref?.()
    slot.idleTimer = timer
  }

  private evictSlot(slot: StandbySlot) {
    slot.retired = true
    this.cancelIdleTimer(slot)
    if (this.slots.get(slot.key) === slot) this.slots.delete(slot.key)
  }

  private evictToFit() {
    while (this.slots.size >= this.maxSlots) {
      const oldest = this.slots.values().next().value
      if (!oldest) break
      this.evictSlot(oldest)
      void this.closeTransport(oldest.transport).catch(() => undefined)
    }
  }
}
