import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  utimesSync,
} from 'node:fs'
import { mkdir, rename, utimes, writeFile } from 'node:fs/promises'
import { connect, createServer, type Socket } from 'node:net'
import { basename, dirname, join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { setTimeout as delay } from 'node:timers/promises'
import {
  ProviderEventSchema,
  type ProviderConnection,
  type ProviderEvent,
  type ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { TranscriptReader } from '../server/brain-io.ts'
import {
  BrainTurnObservation,
  type BrainRead,
} from '../server/brain-observer.ts'
import { SessionManager } from '../server/session.ts'
import { connectAcp } from '../server/transport.ts'

/** Create an externally resolvable promise barrier. */
export function deferred<T = void>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => {
    resolve = yes
    reject = no
  })
  return { promise, resolve, reject }
}

/** Return true when `pid` no longer exists in the OS process table. */
export function isProcessGone(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return false
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'ESRCH'
  }
}

/** Return true when `pid` has exited (including unreaped Linux zombies). */
export function isProcessStopped(pid: number): boolean {
  if (isProcessGone(pid)) return true
  if (process.platform === 'linux') {
    try {
      return /^\d+ \([^)]*\) Z /.test(readFileSync(`/proc/${pid}/stat`, 'utf8'))
    } catch {
      return true
    }
  }
  return false
}

/** Best-effort process-group signal that ignores already-exited targets. */
export function killProcessGroupIgnoreMissing(
  pid: number,
  signal: NodeJS.Signals = 'SIGKILL',
): void {
  try {
    process.kill(-pid, signal)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
  }
}

/** Wait for an external OS process to exit (scoped strictly to PID-exit checks). */
export async function waitForPidExit(
  pid: number,
  timeoutMs = 5000,
): Promise<void> {
  const stepMs = 10
  const maxSteps = Math.max(1, Math.ceil(timeoutMs / stepMs))
  for (let step = 0; step < maxSteps; step++) {
    if (isProcessStopped(pid)) return
    await delay(stepMs)
  }
  assert.fail(`Timed out waiting for process ${pid} to exit`)
}

/** Collect emitted `ProviderEvent` items with push-based waiting. */
export function createEventCollector(onEvent?: (event: ProviderEvent) => void) {
  const events: ProviderEvent[] = []
  const listeners = new Set<() => void>()

  function emit(event: ProviderEvent) {
    const cloned = structuredClone(event)
    events.push(cloned)
    onEvent?.(cloned)
    for (const notify of [...listeners]) notify()
  }

  async function waitCondition(
    predicate: () => boolean,
    timeoutMs = 5000,
    assertHealthy?: () => void,
  ): Promise<void> {
    assertHealthy?.()
    if (predicate()) return
    let check!: () => void
    let timer!: ReturnType<typeof setTimeout>
    try {
      await new Promise<void>((resolve, reject) => {
        check = () => {
          try {
            assertHealthy?.()
          } catch (err) {
            reject(err)
            return
          }
          if (predicate()) resolve()
        }
        timer = setTimeout(() => {
          try {
            assertHealthy?.()
            assert.fail(`Timed out: ${JSON.stringify(events)}`)
          } catch (err) {
            reject(err)
          }
        }, timeoutMs)
        listeners.add(check)
        check()
      })
    } finally {
      clearTimeout(timer)
      listeners.delete(check)
    }
  }

  async function wait(
    predicate: (event: ProviderEvent) => boolean,
    timeoutMs = 5000,
    assertHealthy?: () => void,
  ): Promise<ProviderEvent> {
    let matched: ProviderEvent | undefined
    await waitCondition(
      () => {
        matched = events.find(predicate)
        return matched !== undefined
      },
      timeoutMs,
      assertHealthy,
    )
    return matched!
  }

  return { events, emit, wait, waitCondition }
}

/** Validate and collect all events emitted on a `ProviderConnection`. */
export function collectProviderEvents(
  provider: ProviderConnection,
  onEvent?: (event: ProviderEvent) => void,
) {
  const opened = new Map<string, readonly string[]>()
  const protocolErrors: string[] = []
  const collector = createEventCollector((event) => {
    ProviderEventSchema.parse(event)
    if (event.type === 'session.opened') {
      if (
        event.parentSessionId &&
        !opened.get(event.parentSessionId)?.includes('session.subsession')
      ) {
        protocolErrors.push(
          `Parent lacks session.subsession: ${event.parentSessionId}`,
        )
      }
      opened.set(event.sessionId, event.capabilities)
    } else if (event.type === 'session.closed') {
      opened.delete(event.sessionId)
    }
    onEvent?.(event)
  })

  provider.onEvent(collector.emit)

  return {
    events: collector.events,
    protocolErrors,
    wait: (predicate: (event: ProviderEvent) => boolean, timeoutMs = 5000) =>
      collector.wait(predicate, timeoutMs, () =>
        assert.deepEqual(protocolErrors, []),
      ),
  }
}

/** Build an isolated environment anchored under `home` without inheriting host config or credentials. */
export function isolatedEnv(
  home: string,
  extra: Record<string, string> = {},
): Record<string, string> {
  return {
    HOME: home,
    XDG_CONFIG_HOME: join(home, '.config'),
    XDG_CACHE_HOME: join(home, '.cache'),
    PATH: '',
    ...extra,
  }
}

/** Per-call boundary double returning each scripted handler once in call order. */
export function sequenceDouble<Args extends unknown[], R>(
  handlers: readonly ((...args: Args) => R)[],
): (...args: Args) => R {
  let index = 0
  return (...args: Args): R => {
    assert.ok(
      index < handlers.length,
      `Unexpected call #${index + 1} (scripted ${handlers.length})`,
    )
    return handlers[index++](...args)
  }
}

/** Drain any in-flight observation pass, then run one fresh read strictly after prior disk writes. */
export async function flushObs(obs: { poll(): Promise<void> }): Promise<void> {
  await obs.poll()
  await obs.poll()
}

/** Write `content` (and optional `mtime`) atomically via a sibling `.tmp` file + `rename`. */
export async function writeFileAtomic(
  path: string,
  content: string,
  mtime?: Date,
): Promise<void> {
  const dir = dirname(path)
  await mkdir(dir, { recursive: true })
  const tmp = join(dir, `.${basename(path)}.${randomUUID()}.tmp`)
  await writeFile(tmp, content)
  if (mtime) await utimes(tmp, mtime, mtime)
  await rename(tmp, path)
}

/** Write a log file with a deterministic epoch-second `mtime` atomically. */
export async function writeTimedLog(
  path: string,
  content: string,
  epochSeconds: number,
): Promise<void> {
  await writeFileAtomic(path, content, new Date(epochSeconds * 1000))
}

/** Populate a SQLite DB into a temp sibling, set its `mtime`, and atomically swap it into `dbPath`. */
export function writeTimedDb(
  dbPath: string,
  populate: (db: DatabaseSync) => void,
  epochSeconds: number,
): void {
  const dir = dirname(dbPath)
  mkdirSync(dir, { recursive: true })
  const tmp = join(dir, `.${basename(dbPath)}.${randomUUID()}.tmp`)
  if (existsSync(dbPath)) copyFileSync(dbPath, tmp)
  const db = new DatabaseSync(tmp)
  try {
    populate(db)
  } finally {
    db.close()
  }
  utimesSync(tmp, epochSeconds, epochSeconds)
  renameSync(tmp, dbPath)
}

/** Write JSONL rows to `<brainRoot>/<nativeId>/.system_generated/logs/transcript_full.jsonl`. */
export async function writeBrainTranscript(
  brainRoot: string,
  nativeId: string,
  rows: readonly Record<string, unknown>[],
  mtime?: Date,
): Promise<string> {
  const path = join(
    brainRoot,
    nativeId,
    '.system_generated/logs/transcript_full.jsonl',
  )
  const payload = rows.map((row) => JSON.stringify(row)).join('\n') + '\n'
  if (mtime) {
    await writeFileAtomic(path, payload, mtime)
  } else {
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, payload)
  }
  return path
}

/** Deterministic Unix-socket IPC gate for synchronizing mock ACP subprocesses with tests. */
export async function createSignalGate(dir: string, filename = 'gate.sock') {
  const socketPath = join(dir, filename)
  const firstEntered = deferred<string>()
  let firstResolved = false
  let released = false
  const pendingSockets = new Set<Socket>()
  const allSockets = new Set<Socket>()
  const server = createServer((socket) => {
    allSockets.add(socket)
    socket.on('close', () => {
      allSockets.delete(socket)
      pendingSockets.delete(socket)
    })
    let buf = ''
    socket.on('data', (chunk) => {
      buf += chunk.toString('utf8')
      const nl = buf.indexOf('\n')
      if (nl < 0) return
      const tag = buf.slice(0, nl).trim()
      if (!firstResolved) {
        firstResolved = true
        firstEntered.resolve(tag)
      }
      if (released) socket.end('go\n')
      else pendingSockets.add(socket)
    })
  })
  await new Promise<void>((resolve) => server.listen(socketPath, resolve))
  return {
    socketPath,
    entered: firstEntered.promise,
    release() {
      released = true
      for (const socket of pendingSockets) {
        if (!socket.destroyed) socket.end('go\n')
      }
      pendingSockets.clear()
    },
    async close() {
      for (const socket of allSockets) socket.destroy()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    },
  }
}

/** Client side of `createSignalGate` for mock ACP subprocesses. */
export function awaitSignalGate(
  socketPath: string | undefined,
  tag: string,
): Promise<void> {
  if (!socketPath) return Promise.resolve()
  return new Promise<void>((resolve, reject) => {
    const socket = connect(socketPath, () => {
      socket.write(`${tag}\n`)
    })
    socket.once('data', () => {
      socket.destroy()
      resolve()
    })
    socket.once('end', () => {
      socket.destroy()
      resolve()
    })
    socket.once('error', reject)
  })
}

export type RpcPrompt = ReturnType<typeof connectAcp>['rpc']['prompt']
export type PromptHandler = (
  params: Parameters<RpcPrompt>[0],
  realPrompt: RpcPrompt,
) => ReturnType<RpcPrompt>

/** `SessionManager` subclass exposing the most recently started session for tests. */
export class HarnessSessionManager extends SessionManager {
  session!: Awaited<ReturnType<SessionManager['start']>>
  override async start(...args: Parameters<SessionManager['start']>) {
    this.session = await super.start(...args)
    return this.session
  }
}

/** Wrap `connectAcp` so tests can intercept `rpc.prompt` calls cleanly. */
export function wrapConnectAcp(
  getPromptHandler: () => PromptHandler | undefined,
  onRealPrompt?: (realPrompt: RpcPrompt) => void,
): typeof connectAcp {
  return (...args) => {
    const transport = connectAcp(...args)
    const realPrompt = transport.rpc.prompt.bind(transport.rpc)
    onRealPrompt?.(realPrompt)
    const wrappedRpc = new Proxy(transport.rpc, {
      get(target, prop) {
        if (prop === 'prompt') {
          return (params: Parameters<typeof realPrompt>[0]) => {
            const handler = getPromptHandler()
            return handler ? handler(params, realPrompt) : realPrompt(params)
          }
        }
        const value = Reflect.get(target, prop, target)
        return typeof value === 'function' ? value.bind(target) : value
      },
    })
    return new Proxy(transport, {
      get(target, prop) {
        if (prop === 'rpc') return wrappedRpc
        return Reflect.get(target, prop, target)
      },
    })
  }
}

/** `BrainTurnObservation` subclass exposing `attached` and post-attach `initialPoll` barriers for deterministic handoff tests. */
export class TrackingObservation extends BrainTurnObservation {
  readonly attached = deferred<void>()
  readonly initialPoll = deferred<void>()
  private isAttached = false
  override async attach(
    consumer: Parameters<BrainTurnObservation['attach']>[0],
  ) {
    await super.attach(consumer)
    this.isAttached = true
    this.attached.resolve()
  }
  override poll(): Promise<void> {
    const pass = super.poll()
    if (this.isAttached) {
      void pass.then(
        () => this.initialPoll.resolve(),
        () => this.initialPoll.resolve(),
      )
    }
    return pass
  }
}

type ScriptedReadStep = BrainRead | (() => Promise<BrainRead>)

/** Per-call scripted `TranscriptReader` double for deterministic read-sequence tests. */
export class ScriptedTranscriptReader extends TranscriptReader {
  reads = 0
  private readonly nextRead: () => Promise<BrainRead>

  constructor(steps: readonly ScriptedReadStep[]) {
    super()
    this.nextRead = sequenceDouble(
      steps.map((step) =>
        typeof step === 'function' ? step : async () => step,
      ),
    )
  }

  override async read(): Promise<BrainRead> {
    this.reads++
    return this.nextRead()
  }
}

export type ToolCallTimelineItem = Extract<
  ProviderTimelineItem,
  { type: 'tool_call' }
>

/** Extract all `tool_call` timeline items from a collected `ProviderEvent` stream. */
export function toolCalls(
  events: readonly ProviderEvent[],
): ToolCallTimelineItem[] {
  return events
    .filter(
      (event): event is Extract<ProviderEvent, { type: 'timeline.item' }> =>
        event.type === 'timeline.item',
    )
    .map((event) => event.item)
    .filter((item): item is ToolCallTimelineItem => item.type === 'tool_call')
}

/** Extract all `compaction` timeline events from a collected `ProviderEvent` stream. */
export function compactionEvents(events: readonly ProviderEvent[]) {
  return events.filter(
    (
      event,
    ): event is Extract<ProviderEvent, { type: 'timeline.item' }> & {
      item: Extract<ProviderTimelineItem, { type: 'compaction' }>
    } => event.type === 'timeline.item' && event.item.type === 'compaction',
  )
}
