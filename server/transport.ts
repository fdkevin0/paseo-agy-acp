import { z } from 'zod'
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

/** Own one stdio ACP process, bounded control requests, and process-tree shutdown. */
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
    { cwd, env: { ...(launch?.env ?? process.env), ...env }, stdio: 'pipe' },
  )
  let stopping = false
  let activeClient = client
  let activeOnExit = onExit
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
    activeOnExit(error)
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
      sessionUpdate: (params) => activeClient.sessionUpdate(params),
      requestPermission: (params) => activeClient.requestPermission(params),
    }),
    { readable, writable },
  )
  void rpc.closed.then(() => fail(new Error('ACP connection closed')))
  let closing: Promise<void> | undefined
  function close(): Promise<void> {
    if (closing) return closing
    stopping = true
    closing = (async () => {
      child.stdin.end()
      const timer = setTimeout(() => terminateProcess(child), 500)
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
      }
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
            void close().catch(activeOnExit)
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
    bind(nextClient: Client, nextOnExit: (error: Error) => void) {
      activeClient = nextClient
      activeOnExit = nextOnExit
    },
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
