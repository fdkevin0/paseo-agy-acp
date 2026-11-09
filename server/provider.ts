import { randomUUID } from 'node:crypto'
import { homedir } from 'node:os'
import {
  negotiateProviderCapabilities,
  requireProviderCapabilities,
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
import { AgyUsageSource } from './usage.ts'
import { SessionManager } from './session.ts'
const supported = [
  'prompt.message',
  'prompt.command',
  'prompt.image',
  'session.configure',
  'session.list',
  'session.persistence',
  'session.can_stop_runtime',
  'permission',
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
      if (!connection.versions.includes(1))
        throw new Error('Provider protocol version 1 required')
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
            catalogs.set(cwd, catalog)
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
        version: 1,
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
          if (input.type === 'session.permission') return handle(input)
          const task = handle(input).catch((error) => {
            if ('requestId' in input)
              emit({
                type: 'request.failed',
                requestId: input.requestId,
                error: { message: String(error) },
              })
            else if (input.type === 'session.prompt')
              emit({
                type: 'session.prompt_result',
                sessionId: input.sessionId,
                clientMessageId: input.prompt.clientMessageId,
                result: { type: 'failed', error: { message: String(error) } },
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
      usage.tiers.set(dir, session.transport.tier.name)
    session.flush()
    await session.waitForCommands(100)
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
          .map((s) => ({
            persistence: {
              version: 1,
              data: { sessionId: s.sessionId },
            },
            cwd: s.cwd,
            title: s.title ?? undefined,
            updatedAt: s.updatedAt ?? undefined,
          })),
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
  return
}
