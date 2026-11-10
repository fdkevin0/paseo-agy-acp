import { randomUUID } from 'node:crypto'
import { homedir } from 'node:os'
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
import { providerStatus, catalogCacheKey } from './preflight.ts'
import { configState } from './mapping.ts'
import { formatErrorMessage, record, setBounded } from './common.ts'
import { AgyUsageSource } from './usage.ts'
import { SessionManager } from './session.ts'

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
      return catalogCacheKey(launch)
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
      const pending = new Set<Promise<void>>()
      const catalogs = new Map<string, Promise<ProviderCatalog>>()
      let closed = false
      async function handle(input: ProviderInput) {
        if (input.type === 'catalog') {
          const cwd = input.cwd ?? homedir()
          let catalog = catalogs.get(cwd)
          if (!catalog) {
            catalog = probeCatalog(manager, connection.launch, cwd, usage)
            setBounded(catalogs, cwd, catalog, CATALOG_CACHE_LIMIT)
            void catalog.catch(() => catalogs.delete(cwd))
          }
          emit({
            type: 'catalog',
            requestId: input.requestId,
            catalog: await catalog,
          })
        } else if (input.type === 'sessions')
          await listSessions(manager, input, emit)
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
          await manager.close()
          await Promise.all([...pending])
          listeners.clear()
        },
      }
    },
  }
}

async function probeCatalog(
  manager: SessionManager,
  launch: ProviderLaunch | undefined,
  cwd: string,
  usage: AgyUsageSource,
): Promise<ProviderCatalog> {
  const session = await manager.start(randomUUID(), cwd)
  try {
    const response = await session.transport.request(
      session.transport.rpc.newSession({ cwd, mcpServers: [] }),
    )
    session.state.nativeId = response.sessionId
    const dir = usage.remember(launch, {}, cwd)
    if (session.transport.tier?.name)
      setBounded(
        usage.tiers,
        dir,
        session.transport.tier.name,
        CATALOG_CACHE_LIMIT,
      )
    const config = configState({
      ...response,
      models: session.transport.models,
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
    await session.close()
  }
}

async function listSessions(
  manager: SessionManager,
  input: Extract<ProviderInput, { type: 'sessions' }>,
  emit: (event: ProviderEvent) => void,
) {
  const session = await manager.start(randomUUID(), input.cwd ?? homedir())
  try {
    const results = []
    let cursor: string | undefined
    const cursors = new Set<string>()
    do {
      const response = await session.transport.request(
        session.transport.rpc.listSessions({ cwd: input.cwd, cursor }),
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
    emit({
      type: 'sessions',
      requestId: input.requestId,
      sessions: results.slice(0, input.limit ?? 100),
    })
  } finally {
    await session.close()
  }
}
