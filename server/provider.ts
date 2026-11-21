import {
  negotiateProviderCapabilities,
  requireProviderCapabilities,
  PROVIDER_PROTOCOL_VERSION,
  ProviderInputSchema,
} from '@getpaseo/plugin/server/provider'
import type {
  ProviderCatalog,
  ProviderLaunch,
  ProviderRegistration,
  ProviderConnection,
  ProviderEvent,
  ProviderInput,
} from '@getpaseo/plugin/server/provider'
import {
  dataDirectory,
  providerStatus,
  removeSessionArtifacts,
  resolveCatalogCacheKey,
  resolveSpawnCwd,
} from './preflight.ts'
import { configState } from './mapping.ts'
import {
  formatErrorMessage,
  record,
  sessionIdPattern,
  setBounded,
} from './common.ts'
import { SharedControlRuntime } from './runtime.ts'
import { AgyUsageSource } from './usage.ts'
import { ReportedRequestError, SessionManager } from './session.ts'

const CATALOG_CACHE_LIMIT = 100

const supported = [
  'prompt.message',
  'prompt.command',
  'prompt.image',
  'session.configure',
  'session.list',
  'session.persistence',
  'session.can_stop_runtime',
  'permission',
  'permission.tool_policy',
  'session.subsession',
] as const

/** Register the direct ACP provider and own every runtime for its connection. */
export function createProvider(
  usage = new AgyUsageSource(),
): ProviderRegistration {
  return {
    id: 'agy',
    label: 'Antigravity (ACP)',
    description: 'Native agy ACP with rich tools and history',
    command: ['agy', 'acp'],
    icon: 'icon.svg',
    status: ({ launch }) => providerStatus(launch),
    async getCatalogCacheKey({ launch }) {
      return resolveCatalogCacheKey(launch)
    },
    async connect(connection): Promise<ProviderConnection> {
      if (!connection.versions.includes(PROVIDER_PROTOCOL_VERSION))
        throw new Error(
          `Provider protocol version ${PROVIDER_PROTOCOL_VERSION} required`,
        )
      const capabilities = negotiateProviderCapabilities(
        connection.capabilities,
        supported,
      )
      const listeners = new Set<(event: ProviderEvent) => void>()
      const emit = (event: ProviderEvent) => {
        for (const listener of listeners) listener(event)
      }
      const manager = new SessionManager({
        launch: connection.launch,
        usage,
        capabilities,
        emit,
      })
      const controlRuntime = new SharedControlRuntime({
        launch: connection.launch,
      })
      const pending = new Set<Promise<void>>()
      const pendingCatalogs = new Map<string, Promise<ProviderCatalog>>()
      let closed = false
      async function handle(input: ProviderInput) {
        if (input.type === 'catalog') {
          const cwd = await resolveSpawnCwd(input.cwd, connection.launch, {
            allowMissingCwd: true,
          })
          let catalog = pendingCatalogs.get(cwd)
          if (!catalog) {
            const pendingCatalog = probeCatalog(
              controlRuntime,
              connection.launch,
              cwd,
              usage,
            )
            catalog = pendingCatalog
            setBounded(
              pendingCatalogs,
              cwd,
              pendingCatalog,
              CATALOG_CACHE_LIMIT,
            )
            const clear = () => {
              if (pendingCatalogs.get(cwd) === pendingCatalog)
                pendingCatalogs.delete(cwd)
            }
            void pendingCatalog.then(clear, clear)
          }
          emit({
            type: 'catalog',
            requestId: input.requestId,
            catalog: await catalog,
          })
        } else if (input.type === 'sessions')
          await listSessions(controlRuntime, connection.launch, input, emit)
        else await manager.handle(input)
      }
      return {
        version: PROVIDER_PROTOCOL_VERSION,
        capabilities,
        onEvent(listener) {
          listeners.add(listener)
          return () => {
            listeners.delete(listener)
          }
        },
        async send(input) {
          if (closed) throw new Error('Provider connection is closed')
          input = ProviderInputSchema.parse(input)
          requireProviderCapabilities(capabilities, input)
          if (input.type === 'session.permission') {
            const task = handle(input)
            const tracked = task.then(
              () => undefined,
              () => undefined,
            )
            pending.add(tracked)
            void tracked.finally(() => pending.delete(tracked))
            return task
          }
          const task = handle(input).catch((error) => {
            if (error instanceof ReportedRequestError) return
            const message = formatErrorMessage(error)
            if ('requestId' in input)
              emit({
                type: 'request.failed',
                requestId: input.requestId,
                error: { message },
              })
            else if (input.type === 'session.prompt')
              emit({
                type: 'session.prompt_result',
                sessionId: input.sessionId,
                clientMessageId: input.prompt.clientMessageId,
                result: { type: 'failed', error: { message } },
              })
          })
          pending.add(task)
          void task.finally(() => pending.delete(task))
        },
        async close() {
          closed = true
          await Promise.all([manager.close(), controlRuntime.close()])
          await Promise.all([...pending])
          listeners.clear()
        },
      }
    },
  }
}

async function probeCatalog(
  controlRuntime: SharedControlRuntime,
  launch: ProviderLaunch | undefined,
  cwd: string,
  usage: AgyUsageSource,
): Promise<ProviderCatalog> {
  return controlRuntime.run(
    cwd,
    async (lease) => {
      const { transport } = lease
      let nativeId = ''
      try {
        const response = await transport.request(
          transport.rpc.newSession({ cwd, mcpServers: [] }),
        )
        nativeId = response.sessionId
        const dir = usage.remember(launch, {}, cwd)
        if (transport.tier?.name)
          setBounded(usage.tiers, dir, transport.tier.name, CATALOG_CACHE_LIMIT)
        const config = configState({
          ...response,
          models: transport.models,
        })
        return {
          models: config.models,
          modes: config.modes,
          thinkingOptions: config.thinkingOptions,
          defaultModel: config.model,
          defaultMode: config.mode,
          defaultThinkingOption: config.thinkingOption,
        }
      } finally {
        if (nativeId) {
          let closeOk = false
          try {
            await transport.request(
              transport.rpc.closeSession({ sessionId: nativeId }),
              { timeoutMs: 1500, fatalOnTimeout: false },
            )
            closeOk = !transport.closed
          } catch {
            closeOk = false
          }
          let canCleanup = closeOk
          if (!closeOk || !lease.reusable) {
            canCleanup = await lease.retire().then(
              () => true,
              () => false,
            )
          }
          if (canCleanup && sessionIdPattern.test(nativeId)) {
            await removeSessionArtifacts(
              dataDirectory(launch, {}, cwd),
              nativeId,
            ).catch(() => undefined)
          }
        }
      }
    },
    { serialize: true },
  )
}

async function listSessions(
  controlRuntime: SharedControlRuntime,
  launch: ProviderLaunch | undefined,
  input: Extract<ProviderInput, { type: 'sessions' }>,
  emit: (event: ProviderEvent) => void,
) {
  const spawnCwd = await resolveSpawnCwd(input.cwd, launch, {
    allowMissingCwd: true,
  })
  const sessions = await controlRuntime.run(spawnCwd, async ({ transport }) => {
    const results = []
    let cursor: string | undefined
    const cursors = new Set<string>()
    do {
      const response = await transport.request(
        transport.rpc.listSessions({ cwd: input.cwd, cursor }),
      )
      results.push(
        ...response.sessions
          .filter(
            (s) =>
              !input.query ||
              `${s.title ?? ''} ${s.sessionId}`
                .toLowerCase()
                .includes(input.query.toLowerCase()),
          )
          .map((s) => {
            const raw = record(s)
            const meta = record(s._meta)
            const description = [
              raw.description,
              raw.lastPromptPreview,
              meta.description,
              meta.lastPromptPreview,
            ].find((v): v is string => typeof v === 'string' && v.length > 0)
            return {
              persistence: {
                version: 1,
                data: { sessionId: s.sessionId },
              },
              cwd: s.cwd,
              title: s.title ?? undefined,
              ...(description ? { description } : {}),
              updatedAt: s.updatedAt ?? undefined,
            }
          }),
      )
      cursor = response.nextCursor ?? undefined
      if (cursor && cursors.has(cursor))
        throw new Error('Repeated ACP session list cursor')
      if (cursor) cursors.add(cursor)
    } while (cursor && results.length < (input.limit ?? 100))
    return results.slice(0, input.limit ?? 100)
  })
  emit({
    type: 'sessions',
    requestId: input.requestId,
    sessions,
  })
}
