/** Shared ACP initialization, idle-reaped control runtime, and pre-warmed standby runtime pool. */
import { statSync } from 'node:fs'
import { join } from 'node:path'
import { PROTOCOL_VERSION } from '@agentclientprotocol/sdk/unstable'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'
import { PACKAGE_VERSION } from './common.ts'
import {
  agyVersionDiagnostic,
  binaryFingerprint,
  hashLaunchKey,
  isCwdIndependentLaunch,
  resolveHome,
} from './preflight.ts'
import { connectAcp, type AcpClient } from './transport.ts'

export type ControlTransport = ReturnType<typeof connectAcp>
export type InitializedAcp = Awaited<ReturnType<typeof initializeAcp>>

export const SKIP_PERMISSIONS_FLAG = '--dangerously-skip-permissions'

interface ControlLease {
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

interface SharedControlRuntimeOptions {
  launch?: ProviderLaunch
  connectAcp?: typeof connectAcp
  idleTimeoutMs?: number
  scheduler?: IdleTimerScheduler
  resolveFingerprint?: typeof binaryFingerprint
}

interface StandbyRuntimePoolOptions {
  launch?: ProviderLaunch
  connectAcp?: typeof connectAcp
  idleTimeoutMs?: number
  maxSlots?: number
  scheduler?: IdleTimerScheduler
  resolveFingerprint?: typeof binaryFingerprint
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

/** Canonical SHA-256 standby pool key covering command, args, resolved environment, and optional binary fingerprint. */
export function standbyPoolKey(
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
  fingerprint?: string,
): string {
  return hashLaunchKey(launch, env, fingerprint)
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

export function abortError(
  signal: AbortSignal,
  fallback = 'Provider connection is closed',
): Error {
  return signal.reason instanceof Error ? signal.reason : new Error(fallback)
}

export async function raceAbort<T>(
  promise: Promise<T>,
  signal: AbortSignal,
  fallback?: string,
): Promise<T> {
  if (signal.aborted) {
    void promise.catch(() => undefined)
    throw abortError(signal, fallback)
  }
  let onAbort!: () => void
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(abortError(signal, fallback))
  })
  signal.addEventListener('abort', onAbort, { once: true })
  try {
    const value = await Promise.race([promise, aborted])
    if (signal.aborted) throw abortError(signal, fallback)
    return value
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
}

/** Best-effort close of a never-prompted ACP session; returns whether the transport remained healthy. */
export async function closeDisposableSession(
  transport: ControlTransport,
  nativeId: string,
): Promise<boolean> {
  try {
    await transport.request(
      transport.rpc.closeSession({ sessionId: nativeId }),
      { timeoutMs: 1500, fatalOnTimeout: false },
    )
    return !transport.closed
  } catch {
    return false
  }
}

/** Track live ACP transports, auto-removing each once stopped and draining all on close. */
export class TransportTracker {
  private readonly active = new Set<ControlTransport>()
  private readonly watchedStops = new WeakSet<ControlTransport>()

  track(transport: ControlTransport): void {
    this.active.add(transport)
    if (!this.watchedStops.has(transport)) {
      this.watchedStops.add(transport)
      void transport.stopped.then(
        () => this.active.delete(transport),
        () => this.active.delete(transport),
      )
    }
  }

  untrack(transport: ControlTransport): void {
    this.active.delete(transport)
  }

  async closeOne(transport: ControlTransport): Promise<void> {
    try {
      await transport.close()
    } finally {
      this.active.delete(transport)
    }
  }

  closeAll(): Promise<void> {
    return Promise.all(
      [...this.active].map((transport) =>
        this.closeOne(transport).catch(() => undefined),
      ),
    ).then(() => undefined)
  }
}

class IdleTimer {
  private handle: IdleTimerHandle | undefined
  private readonly scheduler: IdleTimerScheduler
  private readonly timeoutMs: number

  constructor(scheduler: IdleTimerScheduler, timeoutMs: number) {
    this.scheduler = scheduler
    this.timeoutMs = timeoutMs
  }

  cancel(): void {
    if (this.handle !== undefined) {
      this.scheduler.clearTimeout(this.handle)
      this.handle = undefined
    }
  }

  arm(onExpire: () => void): void {
    if (!Number.isFinite(this.timeoutMs) || this.timeoutMs < 0) return
    this.cancel()
    const handle = this.scheduler.setTimeout(() => {
      if (this.handle === handle) this.handle = undefined
      onExpire()
    }, this.timeoutMs)
    handle.unref?.()
    this.handle = handle
  }
}

/** Run ACP `initialize` and reject runtimes below the minimum supported `agy` version. */
export async function initializeAcp(
  transport: ControlTransport,
  signal?: AbortSignal,
) {
  if (signal?.aborted) throw abortError(signal)
  const initPromise = transport.request(
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
  const initialized = signal
    ? await raceAbort(initPromise, signal)
    : await initPromise
  const diagnostic = agyVersionDiagnostic(initialized.agentInfo?.version)
  if (diagnostic) throw new Error(diagnostic)
  return initialized
}

interface SharedSlot {
  readonly fingerprint: string | undefined
  transport: ControlTransport
  ready: Promise<void>
  inFlight: number
  readonly idleTimer: IdleTimer
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
  private readonly resolveFingerprint: typeof binaryFingerprint
  private active: SharedSlot | undefined
  private readonly transports = new TransportTracker()
  private serializedLane: Promise<void> = Promise.resolve()
  private closed = false
  private closeController = new AbortController()
  private closing: Promise<void> | undefined

  constructor(options: SharedControlRuntimeOptions = {}) {
    this.launch = options.launch
    this.connect = options.connectAcp ?? connectAcp
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
    this.scheduler = options.scheduler ?? defaultScheduler
    this.resolveFingerprint = options.resolveFingerprint ?? binaryFingerprint
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
    this.closeController.abort(new Error('Provider connection is closed'))
    if (this.active) this.evict(this.active)
    this.closing = this.transports.closeAll()
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

  private armIdleTimer(slot: SharedSlot) {
    if (
      this.closed ||
      slot.retired ||
      this.active !== slot ||
      slot.inFlight > 0 ||
      slot.transport.closed
    )
      return
    slot.idleTimer.arm(() => {
      if (slot.inFlight > 0 || slot.retired) return
      this.evict(slot)
      void this.transports.closeOne(slot.transport).catch(() => undefined)
    })
  }

  private evict(slot: SharedSlot) {
    slot.retired = true
    slot.idleTimer.cancel()
    if (this.active === slot) this.active = undefined
  }

  private startShared(
    spawnCwd: string,
    fingerprint: string | undefined,
  ): SharedSlot {
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
        void this.transports.closeOne(transport).catch(() => undefined)
      },
    )
    slot = {
      fingerprint,
      transport,
      ready: undefined as unknown as Promise<void>,
      inFlight: 0,
      idleTimer: new IdleTimer(this.scheduler, this.idleTimeoutMs),
      retired: false,
    }
    this.transports.track(transport)
    const activeSlot = slot
    activeSlot.ready = (async () => {
      try {
        await initializeAcp(transport, this.closeController.signal)
        if (this.closed) throw new Error('Provider connection is closed')
        if (exitError || transport.closed)
          throw (
            exitError ?? new Error('ACP control runtime closed during startup')
          )
      } catch (error) {
        this.evict(activeSlot)
        await this.transports.closeOne(transport).catch(() => undefined)
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
      const fingerprint = await this.resolveFingerprint(this.launch).catch(
        () => undefined,
      )
      if (this.closed) throw new Error('Provider connection is closed')
      let slot = this.active
      if (
        slot &&
        !slot.retired &&
        !slot.transport.closed &&
        slot.fingerprint !== fingerprint
      ) {
        this.evict(slot)
        if (slot.inFlight === 0) {
          void this.transports.closeOne(slot.transport).catch(() => undefined)
        }
        slot = undefined
      }
      if (!slot || slot.retired || slot.transport.closed) {
        slot = this.startShared(spawnCwd, fingerprint)
        this.active = slot
      }
      slot.idleTimer.cancel()
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
        if (slot.inFlight === 0 && slot.retired) {
          void this.transports.closeOne(slot.transport).catch(() => undefined)
        }
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
        return this.transports.closeOne(slot.transport)
      },
    }
    try {
      return await fn(lease)
    } finally {
      slot.inFlight--
      if (slot.inFlight === 0) {
        if (slot.retired) {
          void this.transports.closeOne(slot.transport).catch(() => undefined)
        } else {
          this.armIdleTimer(slot)
        }
      }
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
    this.transports.track(transport)
    try {
      await initializeAcp(transport, this.closeController.signal)
      if (this.closed) throw new Error('Provider connection is closed')
      const lease: ControlLease = {
        transport,
        reusable: false,
        retire: () => this.transports.closeOne(transport),
      }
      return await fn(lease)
    } finally {
      await this.transports.closeOne(transport).catch(() => undefined)
    }
  }
}

interface StandbySlot {
  readonly key: string
  readonly baseKey: string
  readonly transport: ControlTransport
  ready: Promise<AttachableRuntime>
  readonly idleTimer: IdleTimer
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
  private readonly resolveFingerprint: typeof binaryFingerprint
  private readonly slots = new Map<string, StandbySlot>()
  private readonly warming = new Map<string, Promise<string | undefined>>()
  private readonly transports = new TransportTracker()
  private closed = false
  private closeController = new AbortController()
  private closing: Promise<void> | undefined

  constructor(options: StandbyRuntimePoolOptions = {}) {
    this.launch = options.launch
    this.connect = options.connectAcp ?? connectAcp
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS
    this.maxSlots = options.maxSlots ?? DEFAULT_MAX_STANDBY_SLOTS
    this.scheduler = options.scheduler ?? defaultScheduler
    this.resolveFingerprint = options.resolveFingerprint ?? binaryFingerprint
  }

  /** Resolve the executable fingerprint using this pool's configured resolver. */
  resolveBinaryFingerprint(
    effectiveLaunch: ProviderLaunch | undefined,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): Promise<string | undefined> {
    return this.resolveFingerprint(effectiveLaunch ?? this.launch, {
      env: sessionEnv,
    }).catch(() => undefined)
  }

  /** Start warming one standby runtime in the background if the slot is empty and reusable. */
  warm(
    effectiveLaunch: ProviderLaunch | undefined,
    spawnCwd: string,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): void {
    const launch = effectiveLaunch ?? this.launch
    if (this.closed || this.maxSlots <= 0 || hasSkipPermissionsFlag(launch))
      return
    if (!isLaunchCwdIndependent(spawnCwd, launch, sessionEnv)) return
    const baseKey = standbyPoolKey(launch, sessionEnv)
    if (this.warming.has(baseKey)) return
    let pending!: Promise<string | undefined>
    pending = (async (): Promise<string | undefined> => {
      try {
        const fingerprint = await this.resolveFingerprint(launch, {
          env: sessionEnv,
        }).catch(() => undefined)
        if (this.closed || this.maxSlots <= 0) return fingerprint
        const key = standbyPoolKey(launch, sessionEnv, fingerprint)
        this.evictStaleSlots(baseKey, key)
        const existing = this.slots.get(key)
        if (
          existing &&
          !existing.retired &&
          !existing.exited &&
          !existing.transport.closed
        )
          return fingerprint
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
            void this.transports.closeOne(transport).catch(() => undefined)
          },
        )
        this.transports.track(transport)
        slot = {
          key,
          baseKey,
          transport,
          ready: undefined as unknown as Promise<AttachableRuntime>,
          idleTimer: new IdleTimer(this.scheduler, this.idleTimeoutMs),
          retired: false,
          exited: false,
          exitError: undefined,
          claimed: false,
        }
        this.slots.set(key, slot)
        slot.ready = (async (): Promise<AttachableRuntime> => {
          try {
            const initialized = await initializeAcp(
              transport,
              this.closeController.signal,
            )
            if (
              this.closed ||
              slot.retired ||
              slot.exited ||
              transport.closed
            ) {
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
            if (
              this.closed ||
              slot.retired ||
              slot.exited ||
              transport.closed
            ) {
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
            await this.transports.closeOne(transport).catch(() => undefined)
            throw error
          }
        })()
        // Prevent unhandled rejection when background warmup fails or is evicted.
        void slot.ready.catch(() => undefined)
        return fingerprint
      } catch {
        return undefined
      } finally {
        if (this.warming.get(baseKey) === pending) {
          this.warming.delete(baseKey)
        }
      }
    })()
    this.warming.set(baseKey, pending)
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
    const baseKey = standbyPoolKey(launch, sessionEnv)
    const pendingWarm = this.warming.get(baseKey)
    const fingerprint = pendingWarm
      ? await pendingWarm
      : await this.resolveFingerprint(launch, {
          env: sessionEnv,
        }).catch(() => undefined)
    if (this.closed) return undefined
    const key = standbyPoolKey(launch, sessionEnv, fingerprint)
    this.evictStaleSlots(baseKey, key)
    const slot = this.slots.get(key)
    if (!slot) return undefined
    slot.claimed = true
    this.slots.delete(key)
    slot.idleTimer.cancel()
    if (slot.retired || slot.exited || slot.transport.closed) {
      await this.transports.closeOne(slot.transport).catch(() => undefined)
      return undefined
    }
    let runtime: AttachableRuntime
    try {
      runtime = await slot.ready
    } catch {
      await this.transports.closeOne(slot.transport).catch(() => undefined)
      return undefined
    }
    if (
      this.closed ||
      slot.retired ||
      slot.exited ||
      runtime.transport.closed
    ) {
      await this.transports.closeOne(runtime.transport).catch(() => undefined)
      return undefined
    }
    this.transports.untrack(runtime.transport)
    return runtime
  }

  /** Return a clean, healthy, unprompted runtime to standby; returns `false` if caller must close it. */
  async recycle(
    runtime: AttachableRuntime,
    effectiveLaunch: ProviderLaunch | undefined,
    spawnCwd: string,
    sessionEnv: Readonly<Record<string, string>> = {},
  ): Promise<boolean> {
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
    const baseKey = standbyPoolKey(launch, sessionEnv)
    const fingerprint = await this.resolveFingerprint(launch, {
      env: sessionEnv,
    }).catch(() => undefined)
    if (this.closed || this.maxSlots <= 0 || runtime.transport.closed)
      return false
    const key = standbyPoolKey(launch, sessionEnv, fingerprint)
    this.evictStaleSlots(baseKey, key)
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
      baseKey,
      transport: runtime.transport,
      ready: Promise.resolve(runtime),
      idleTimer: new IdleTimer(this.scheduler, this.idleTimeoutMs),
      retired: false,
      exited: false,
      exitError: undefined,
      claimed: false,
    }
    runtime.attach(CONTROL_CLIENT, (error) => {
      slot.exited = true
      slot.exitError = error
      this.evictSlot(slot)
      void this.transports.closeOne(runtime.transport).catch(() => undefined)
    })
    this.transports.track(runtime.transport)
    this.slots.set(key, slot)
    this.armIdleTimer(slot)
    return true
  }

  close(): Promise<void> {
    if (this.closing) return this.closing
    this.closed = true
    this.closeController.abort(
      new Error('Standby ACP runtime closed during startup'),
    )
    for (const slot of [...this.slots.values()]) this.evictSlot(slot)
    this.slots.clear()
    this.closing = this.transports.closeAll()
    return this.closing
  }

  private armIdleTimer(slot: StandbySlot) {
    if (
      this.closed ||
      slot.retired ||
      slot.exited ||
      slot.claimed ||
      this.slots.get(slot.key) !== slot ||
      slot.transport.closed
    )
      return
    slot.idleTimer.arm(() => {
      if (slot.claimed || slot.retired) return
      this.evictSlot(slot)
      void this.transports.closeOne(slot.transport).catch(() => undefined)
    })
  }

  private evictSlot(slot: StandbySlot) {
    slot.retired = true
    slot.idleTimer.cancel()
    if (this.slots.get(slot.key) === slot) this.slots.delete(slot.key)
  }

  private evictStaleSlots(baseKey: string, currentKey: string) {
    for (const slot of [...this.slots.values()]) {
      if (slot.baseKey === baseKey && slot.key !== currentKey) {
        this.evictSlot(slot)
        void this.transports.closeOne(slot.transport).catch(() => undefined)
      }
    }
  }

  private evictToFit() {
    while (this.slots.size >= this.maxSlots) {
      const oldest = this.slots.values().next().value
      if (!oldest) break
      this.evictSlot(oldest)
      void this.transports.closeOne(oldest.transport).catch(() => undefined)
    }
  }
}
