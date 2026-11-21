/** Shared ACP initialization and idle-reaped control runtime for stateless provider probes. */
import { homedir } from 'node:os'
import { join } from 'node:path'
import { PROTOCOL_VERSION } from '@agentclientprotocol/sdk/unstable'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'
import { PACKAGE_VERSION } from './common.ts'
import { agyVersionDiagnostic, isCwdIndependentLaunch } from './preflight.ts'
import { connectAcp } from './transport.ts'

export type ControlTransport = ReturnType<typeof connectAcp>

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

const DEFAULT_IDLE_TIMEOUT_MS = 5 * 60_000

const defaultScheduler: IdleTimerScheduler = {
  setTimeout: (fn, ms) => setTimeout(fn, ms),
  clearTimeout: (handle) => clearTimeout(handle as NodeJS.Timeout),
}

const CONTROL_CLIENT = {
  sessionUpdate: async () => {},
  requestPermission: async () => ({
    outcome: { outcome: 'cancelled' as const },
  }),
  requestElicitation: async () => ({ action: 'cancel' }),
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
    const fallbackHome = this.launch?.env?.HOME ?? homedir()
    return (
      isCwdIndependentLaunch(
        spawnCwd,
        fallbackHome,
        this.launch,
        {},
        fallbackHome,
      ) &&
      isCwdIndependentLaunch(
        spawnCwd,
        join(spawnCwd, '__paseo_cwd_probe__'),
        this.launch,
        {},
        fallbackHome,
      )
    )
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
