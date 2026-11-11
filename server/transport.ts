import { z } from 'zod'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { setTimeout as delay } from 'node:timers/promises'
import { RequestPermissionRequestSchema } from './permissions.ts'
import { TransformStream, WritableStream } from 'node:stream/web'
import { nativeModelsSchema, type SessionMetadata } from './mapping.ts'
import { record } from './common.ts'
import { Readable, Writable } from 'node:stream'
import {
  ClientSideConnection,
  ndJsonStream,
  type Client,
} from '@agentclientprotocol/sdk'
import { spawnProcess, terminateProcess } from '@getpaseo/plugin/server'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'

const execFileAsync = promisify(execFile)

async function listProcesses(): Promise<number[][]> {
  const options = { encoding: 'utf8' as const, timeout: 1000 }
  // procps/BSD accept -A; BusyBox lists every process by default and rejects -A.
  const { stdout } = await execFileAsync(
    'ps',
    ['-A', '-o', 'pid=,ppid=,pgid='],
    options,
  ).catch(() => execFileAsync('ps', ['-o', 'pid,ppid,pgid'], options))
  return stdout
    .split('\n')
    .map((line) => line.trim().split(/\s+/).map(Number))
    .filter((row) => row.length === 3 && row.every(Number.isInteger))
}

async function ownedProcessGroups(pid: number): Promise<Set<number>> {
  const processes = await listProcesses()
  const owned = new Set([pid])
  let previousSize = 0
  while (previousSize !== owned.size) {
    previousSize = owned.size
    for (const [id, parent] of processes) if (owned.has(parent)) owned.add(id)
  }
  // Native run_command starts its own session, outside the ACP process group.
  return new Set(
    processes
      .filter(([id, , group]) => owned.has(id) && owned.has(group))
      .map(([, , group]) => group),
  )
}

function killGroup(group: number) {
  try {
    process.kill(-group, 'SIGKILL')
  } catch {
    /* Already gone (ESRCH) or no longer ours (EPERM); shutdown stays best-effort. */
  }
}

const SHUTDOWN_GRACE_MS = 500
const SIGKILL_TIMEOUT_MS = 2000
const TERMINATION_DEADLINE_MS = 2500
const ACP_REQUEST_TIMEOUT_MS = 30_000
const STDERR_DRAIN_MS = 50

type AcpClient = Client & {
  requestElicitation?: (
    params: Record<string, unknown>,
  ) => Promise<Record<string, unknown>>
}

/** Own one ACP process, bounded control requests, and shutdown of its tracked process groups. */
export function connectAcp(
  launch: ProviderLaunch | undefined,
  cwd: string,
  env: Readonly<Record<string, string>>,
  client: AcpClient,
  onExit: (error: Error) => void,
  options?: { requestTimeoutMs?: number },
) {
  const cmdName = launch?.command ?? 'agy'
  const requestTimeoutMs = options?.requestTimeoutMs ?? ACP_REQUEST_TIMEOUT_MS
  const child = spawnProcess(cmdName, launch?.args ?? ['acp'], {
    cwd,
    env: { ...(launch?.env ?? process.env), ...env },
    stdio: 'pipe',
    detached: process.platform !== 'win32',
  })
  let stopping = false
  let stderr = ''
  child.stderr.on('data', (chunk) => {
    stderr = (stderr + chunk).slice(-4096)
  })
  const stderrClosed = new Promise<void>((resolve) => {
    child.stderr.once('end', resolve)
    child.stderr.once('close', resolve)
    child.stderr.once('error', () => resolve())
  })
  let exitInfo:
    { code: number | null; signal: NodeJS.Signals | null } | undefined
  let resolveExited!: () => void
  const exited = new Promise<void>((resolve) => {
    resolveExited = resolve
  })
  function formatExitError(fallback: string): Error {
    const detail = stderr.trim()
    if (exitInfo) {
      const status =
        exitInfo.code !== null
          ? `code ${exitInfo.code}`
          : `signal ${exitInfo.signal ?? 'unknown'}`
      return new Error(
        detail
          ? `${cmdName} exited (${status}): ${detail}`
          : `${cmdName} exited (${status})`,
      )
    }
    return new Error(detail ? `${fallback}: ${detail}` : fallback)
  }
  function fail(error: Error) {
    if (stopping) return
    // A broken protocol stream need not mean the child process has exited.
    void close().catch(() => undefined)
    onExit(error)
  }
  child.on('error', fail)
  child.once('exit', (code, signal) => {
    exitInfo = { code, signal }
    resolveExited()
    void Promise.race([
      stderrClosed,
      delay(STDERR_DRAIN_MS, undefined, { ref: false }),
    ]).then(() => {
      child.stdout.destroy()
      child.stderr.destroy()
      fail(formatExitError('ACP process exited'))
    })
  })
  const tierSchema = z.object({
    id: z.string().optional(),
    name: z.string().optional(),
  })
  let tier: z.infer<typeof tierSchema> | undefined
  let models: SessionMetadata['models']
  const stream = ndJsonStream(
    Writable.toWeb(child.stdin),
    Readable.toWeb(child.stdout),
  )
  const writer = stream.writable.getWriter()
  const writable = new WritableStream({
    write: (message) => writer.write(message),
  })
  // SDK 1.9 strips legacy models from session responses; retain the validated inventory.
  const readable = stream.readable.pipeThrough(
    new TransformStream({
      async transform(message, controller) {
        // agy's multi-select items (`items: { anyOf }` without `type: 'string'`) fail SDK 1.9's
        // CreateElicitationRequest validation, so answer them here instead of via Client.createElicitation.
        if (
          'method' in message &&
          message.method === 'elicitation/create' &&
          'id' in message
        ) {
          const id = message.id
          void (
            stopping || !client.requestElicitation
              ? Promise.resolve({ action: 'cancel' })
              : client.requestElicitation(record(message.params))
          )
            .catch(() => ({ action: 'cancel' }))
            .then((result) =>
              writer.write({
                jsonrpc: '2.0',
                id,
                result,
              }),
            )
            .catch(() => undefined)
          return
        }
        if (
          'method' in message &&
          message.method === 'session/request_permission' &&
          'id' in message &&
          !RequestPermissionRequestSchema.safeParse(message.params).success
        ) {
          await writer.write({
            jsonrpc: '2.0',
            id: message.id,
            result: { outcome: { outcome: 'cancelled' } },
          })
          return
        }
        if ('result' in message) {
          const result = nativeModelsSchema.safeParse(
            record(message.result).models,
          )
          if (result.success) models = result.data
          const observedTier = tierSchema.safeParse(
            record(record(record(message.result)._meta).antigravity).tier,
          )
          if (observedTier.success) tier = observedTier.data
        }
        controller.enqueue(message)
      },
      async flush() {
        if (stopping) return
        await Promise.race([
          Promise.all([exited, stderrClosed]),
          delay(STDERR_DRAIN_MS, undefined, { ref: false }),
        ])
      },
    }),
  )
  const rpc = new ClientSideConnection(
    () => ({
      sessionUpdate: async (params) => {
        if (!stopping) await client.sessionUpdate(params)
      },
      requestPermission: async (params) =>
        stopping
          ? { outcome: { outcome: 'cancelled' } }
          : client.requestPermission(params),
    }),
    { readable, writable },
  )
  void rpc.closed.then(() => fail(formatExitError('ACP connection closed')))
  let closing: Promise<void> | undefined
  function close(): Promise<void> {
    if (closing) return closing
    stopping = true
    closing = (async () => {
      const groups = new Set<number>()
      // Best effort: without a usable `ps`, still stop agy's own process group.
      const collectGroups = async () => {
        if (process.platform === 'win32' || !child.pid) return
        groups.add(child.pid)
        const owned = await ownedProcessGroups(child.pid).catch(
          () => new Set<number>(),
        )
        for (const group of owned) groups.add(group)
      }
      await collectGroups()
      child.stdin.end()
      const stop = async () => {
        if (process.platform === 'win32') await terminateProcess(child)
        else {
          await collectGroups()
          for (const group of groups) killGroup(group)
        }
      }
      const timer = setTimeout(() => {
        void stop().catch(() => undefined)
      }, SHUTDOWN_GRACE_MS)
      const force = setTimeout(() => child.kill('SIGKILL'), SIGKILL_TIMEOUT_MS)
      let deadline: NodeJS.Timeout | undefined
      try {
        await Promise.race([
          exited,
          new Promise<never>((_, reject) => {
            deadline = setTimeout(
              () => reject(new Error('agy did not exit after termination')),
              TERMINATION_DEADLINE_MS,
            )
          }),
        ])
      } finally {
        clearTimeout(timer)
        clearTimeout(force)
        clearTimeout(deadline)
        child.stdin.destroy()
        child.stdout.destroy()
        child.stderr.destroy()
        // EOF may exit agy before its detached command sessions have stopped.
        if (process.platform !== 'win32')
          for (const group of groups) killGroup(group)
      }
    })()
    return closing
  }
  async function request<T>(
    operation: Promise<T>,
    options?: { timeoutMs?: number; fatalOnTimeout?: boolean },
  ): Promise<T> {
    const timeoutMs = options?.timeoutMs ?? requestTimeoutMs
    const fatalOnTimeout = options?.fatalOnTimeout ?? true
    let timer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            const error = new Error('ACP request timed out')
            if (fatalOnTimeout) fail(error)
            reject(error)
          }, timeoutMs)
        }),
      ])
    } finally {
      clearTimeout(timer)
    }
  }
  return {
    rpc,
    exited,
    close,
    request,
    get closed() {
      return stopping
    },
    get tier() {
      return tier
    },
    get models() {
      return models
    },
  }
}
