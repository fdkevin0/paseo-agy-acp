import { z } from 'zod'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { RequestPermissionRequestSchema } from './permissions.ts'
import { TransformStream, WritableStream } from 'node:stream/web'
import { nativeModelsSchema, record, type SessionMetadata } from './mapping.ts'
import { Readable, Writable } from 'node:stream'
import {
  ClientSideConnection,
  ndJsonStream,
  type Client,
} from '@agentclientprotocol/sdk'
import { spawnProcess, terminateProcess } from '@getpaseo/plugin/server'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'

const execFileAsync = promisify(execFile)

async function ownedProcessGroups(pid: number): Promise<Set<number>> {
  const { stdout } = await execFileAsync(
    'ps',
    ['-A', '-o', 'pid=,ppid=,pgid='],
    {
      encoding: 'utf8',
      timeout: 1000,
    },
  )
  const processes = stdout
    .trim()
    .split('\n')
    .map((line) => line.trim().split(/\s+/).map(Number))
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
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
  }
}

/** Own one ACP process, bounded control requests, and shutdown of its tracked process groups. */
export function connectAcp(
  launch: ProviderLaunch | undefined,
  cwd: string,
  env: Readonly<Record<string, string>>,
  client: Client,
  onExit: (error: Error) => void,
) {
  const child = spawnProcess(
    launch?.command ?? 'agy',
    launch?.args ?? ['acp'],
    {
      cwd,
      env: { ...(launch?.env ?? process.env), ...env },
      stdio: 'pipe',
      detached: process.platform !== 'win32',
    },
  )
  let stopping = false
  let stderr = ''
  child.stderr.on('data', (chunk) => {
    stderr = (stderr + chunk).slice(-4096)
  })
  const exited = new Promise<void>((resolve) =>
    child.once('close', () => resolve()),
  )
  function fail(error: Error) {
    if (stopping) return
    // A broken protocol stream need not mean the child process has exited.
    void close().catch(() => undefined)
    onExit(error)
  }
  child.on('error', fail)
  child.once('exit', (code) =>
    fail(new Error(`agy exited (${code}): ${stderr}`)),
  )
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
  void rpc.closed.then(() => fail(new Error('ACP connection closed')))
  let closing: Promise<void> | undefined
  function close(): Promise<void> {
    if (closing) return closing
    stopping = true
    closing = (async () => {
      const groups = new Set<number>()
      let inspectionError: unknown
      const collectGroups = async () => {
        if (process.platform === 'win32' || !child.pid) return
        groups.add(child.pid)
        for (const group of await ownedProcessGroups(child.pid))
          groups.add(group)
      }
      if (process.platform !== 'win32' && child.pid) {
        try {
          await collectGroups()
        } catch (error) {
          inspectionError = error
        }
      }
      child.stdin.end()
      const stop = async () => {
        if (process.platform === 'win32') await terminateProcess(child)
        else {
          await collectGroups().catch(() => undefined)
          for (const group of groups) killGroup(group)
        }
      }
      let stopError: unknown
      const timer = setTimeout(() => {
        void stop().catch((error) => {
          stopError = error
        })
      }, 500)
      const force = setTimeout(() => child.kill('SIGKILL'), 2000)
      let deadline: NodeJS.Timeout | undefined
      try {
        await Promise.race([
          exited,
          new Promise<never>((_, reject) => {
            deadline = setTimeout(
              () => reject(new Error('agy did not exit after termination')),
              2500,
            )
          }),
        ])
      } finally {
        clearTimeout(timer)
        clearTimeout(force)
        clearTimeout(deadline)
        // EOF may exit agy before its detached command sessions have stopped.
        if (process.platform !== 'win32')
          for (const group of groups) killGroup(group)
      }
      if (inspectionError) throw inspectionError
      if (stopError) throw stopError
    })()
    return closing
  }
  async function request<T>(operation: Promise<T>): Promise<T> {
    let timer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        operation,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            void close().catch(onExit)
            reject(new Error('ACP request timed out'))
          }, 30_000)
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
