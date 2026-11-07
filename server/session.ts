import { z } from 'zod'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import type {
  RequestPermissionRequest,
  RequestPermissionResponse,
  SessionNotification,
  NewSessionResponse,
  ContentBlock,
  McpServer,
} from '@agentclientprotocol/sdk'
import type {
  ProviderLaunch,
  ProviderEvent,
  ProviderInput,
  ProviderConfigChanges,
  ProviderSessionConfig,
  ProviderUsage,
} from '@getpaseo/plugin/server/provider'
import { RequestPermissionRequestSchema } from './permissions.ts'
import { watchBrain } from './brain.ts'
import { dataDirectory } from './preflight.ts'
import { connectAcp } from './transport.ts'
import {
  configState,
  Timeline,
  record,
  toolDetail,
  promptContent,
  contextUsage,
  promptUsage,
  sessionIdPattern,
  type SessionMetadata,
} from './mapping.ts'

type Open = Extract<ProviderInput, { type: 'session.open' }>
type Prompt = Extract<ProviderInput, { type: 'session.prompt' }>
type Session = Awaited<ReturnType<typeof createSession>>
interface SessionOptions {
  launch?: ProviderLaunch
  capabilities: readonly string[]
  emit: (event: ProviderEvent) => void
}

/** Own native runtimes, session identities and their cancellation across a provider connection. */
export class SessionManager {
  private sessions = new Map<string, Session>()
  private opening = new Set<string>()
  private transports = new Set<ReturnType<typeof connectAcp>>()
  private closed = false
  private options: SessionOptions
  constructor(options: SessionOptions) {
    this.options = options
  }

  start(id: string, cwd: string, config?: ProviderSessionConfig) {
    if (this.closed) throw new Error('Provider connection is closed')
    return createSession(id, cwd, config, this.options, this.transports)
  }
  async open(input: Open) {
    const { emit, capabilities } = this.options
    if (this.sessions.has(input.sessionId) || this.opening.has(input.sessionId))
      throw new Error('Session already open')
    if (input.config.systemPrompt)
      throw new Error('Custom systemPrompt is not supported by agy ACP')
    this.opening.add(input.sessionId)
    try {
      const session = await this.start(
        input.sessionId,
        input.config.cwd,
        input.config,
      )
      this.sessions.set(input.sessionId, session)
      try {
        const data = record(input.persistence?.data)
        if (
          input.persistence &&
          (input.persistence.version !== 1 ||
            typeof data.sessionId !== 'string')
        )
          throw new Error('Invalid agy persistence handle')
        const params = {
          cwd: input.config.cwd,
          mcpServers: mcpServers(input.config),
        }
        session.state.replay = input.history === 'replay'
        if (input.persistence) {
          if (!session.initialized.agentCapabilities?.loadSession)
            throw new Error('agy does not support session/load')
          session.state.nativeId = data.sessionId as string
          session.state.metadata = {
            ...(await session.transport.request(
              session.transport.rpc.loadSession({
                ...params,
                sessionId: session.state.nativeId,
              }),
            )),
            sessionId: session.state.nativeId,
          }
        } else {
          session.state.metadata = await session.transport.request(
            session.transport.rpc.newSession(params),
          )
          session.state.nativeId = session.state.metadata.sessionId
        }
        session.state.metadata.models = session.transport.models
        await session.drain()
        if (this.closed) throw new Error('Provider connection is closed')
        emit({
          type: 'session.opened',
          sessionId: input.sessionId,
          requestId: input.requestId,
          cwd: input.config.cwd,
          restoration: 'core',
          capabilities,
          persistence: {
            version: 1,
            data: { sessionId: session.state.nativeId },
          },
        })
        session.flush()
        await configureSession(session, input.config)
        await session.drain()
        session.state.loading = false
        session.state.replay = true
        emit({
          type: 'session.ready',
          sessionId: input.sessionId,
          requestId: input.requestId,
        })
      } catch (error) {
        this.sessions.delete(input.sessionId)
        await session.close()
        throw error
      }
    } finally {
      this.opening.delete(input.sessionId)
    }
  }
  async handle(input: ProviderInput) {
    const { emit } = this.options
    if (input.type === 'session.open') return this.open(input)
    if (!('sessionId' in input))
      throw new Error(`Unsupported operation: ${input.type}`)
    const session = this.sessions.get(input.sessionId)
    if (!session) throw new Error('Session is not open')
    if (input.type === 'session.prompt')
      return promptSession(session, input, emit)
    if (input.type === 'session.permission') {
      const permission = session.permissions.get(input.permissionId)
      if (!permission) throw new Error('Permission is no longer pending')
      const allowed = (kind: string) =>
        kind.startsWith(
          input.response.behavior === 'allow' ? 'allow' : 'reject',
        )
      const option = permission.request.options.find((o) =>
        input.response.selectedActionId
          ? o.optionId === input.response.selectedActionId && allowed(o.kind)
          : allowed(o.kind),
      )
      permission.resolve({
        outcome: option
          ? { outcome: 'selected', optionId: option.optionId }
          : { outcome: 'cancelled' },
      })
      session.permissions.delete(input.permissionId)
      emit({
        type: 'session.permission_resolved',
        sessionId: session.id,
        permissionId: input.permissionId,
      })
      return
    }
    if (input.type === 'session.configure')
      await configureSession(session, input.changes)
    else if (input.type === 'session.interrupt') {
      session.cancelPermissions()
      await session.transport.request(
        session.transport.rpc.cancel({ sessionId: session.state.nativeId }),
      )
    } else if (input.type === 'session.can_stop_runtime') {
      // ACP cannot prove native background tasks are idle.
      emit({
        type: input.type,
        requestId: input.requestId,
        sessionId: session.id,
        canStop: false,
      })
      return
    } else if (input.type === 'session.close') {
      await session.close()
      this.sessions.delete(session.id)
      emit({ type: 'session.closed', sessionId: session.id })
    } else throw new Error(`Unsupported operation: ${input.type}`)
    emit({ type: 'request.completed', requestId: input.requestId })
  }
  async close() {
    this.closed = true
    await Promise.all([...this.transports].map((t) => t.close()))
    await Promise.all([...this.sessions.values()].map((s) => s.close()))
    this.sessions.clear()
  }
}

async function createSession(
  id: string,
  cwd: string,
  config: ProviderSessionConfig | undefined,
  options: SessionOptions,
  transports: Set<ReturnType<typeof connectAcp>>,
) {
  const { launch, capabilities, emit } = options
  const permissions = new Map<
    string,
    {
      request: RequestPermissionRequest
      resolve: (r: RequestPermissionResponse) => void
    }
  >()
  const timeline = new Timeline(id)
  const state = {
    nativeId: '',
    opening: true,
    loading: true,
    replay: true,
    turn: '',
    failed: false,
    metadata: { sessionId: '' } as NewSessionResponse & SessionMetadata,
    prompts: new Set<string>(),
  }
  const buffered: SessionNotification[] = []
  const children = new Map<string, () => void>()
  let notificationLane = Promise.resolve()
  let usage: ProviderUsage = {}
  async function drain() {
    // ACP dispatches notifications asynchronously relative to response resolution.
    await new Promise((resolve) => setTimeout(resolve, 0))
    await notificationLane
  }
  function publishUsage(next: ProviderUsage) {
    usage = { ...usage, ...next }
    emit({
      type: 'session.usage',
      sessionId: id,
      turnId: state.turn || undefined,
      usage,
    })
  }
  function terminalize(
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
  ) {
    for (const item of timeline.terminalize(status, error))
      emit({ type: 'timeline.item', sessionId: id, item })
  }
  function update(notification: SessionNotification) {
    if (state.opening) {
      buffered.push(notification)
      return
    }
    const u = notification.update
    if (u.sessionUpdate === 'config_option_update') {
      state.metadata.configOptions = u.configOptions
      publishConfig()
    } else if (u.sessionUpdate === 'current_mode_update') {
      if (state.metadata.modes)
        state.metadata.modes.currentModeId = u.currentModeId
      publishConfig()
    } else if (u.sessionUpdate === 'available_commands_update')
      emit({
        type: 'session.commands',
        sessionId: id,
        commands: u.availableCommands.map((c) => ({
          name: c.name,
          description: c.description,
          argumentHint: c.input?.hint,
        })),
      })
    else if (u.sessionUpdate === 'usage_update') {
      const next = contextUsage(u)
      if (next && (!state.loading || state.replay)) publishUsage(next)
    } else {
      if (state.loading && !state.replay) return
      if (u.sessionUpdate === 'user_message_chunk' && !state.loading) {
        timeline.boundary()
        return
      }
      const item = timeline.update(u)
      if (item) {
        if (
          capabilities.includes('session.subsession') &&
          item.type === 'tool_call' &&
          item.detail.type === 'sub_agent'
        ) {
          const nativeId = item.detail.childSessionId
          if (nativeId && sessionIdPattern.test(nativeId)) {
            const childId = `${id}:child:${nativeId}`
            if (!children.has(childId)) {
              emit({
                type: 'session.opened',
                sessionId: childId,
                parentSessionId: id,
                toolCallId: item.callId,
                cwd,
                capabilities: [],
                restoration: 'parent',
                title: item.detail.subAgentType ?? 'Antigravity subagent',
              })
              children.set(
                childId,
                watchBrain(
                  join(dataDirectory(launch, config?.env, cwd), 'brain'),
                  nativeId,
                  childId,
                  emit,
                ),
              )
              emit({ type: 'session.ready', sessionId: childId })
            }
            item.detail.childSessionId = childId
          }
        }
        if (state.replay) emit({ type: 'timeline.item', sessionId: id, item })
      }
    }
  }
  function publishConfig() {
    emit({
      type: 'session.config',
      sessionId: id,
      config: configState(state.metadata),
    })
  }
  function cancelPermissions() {
    for (const [permissionId, p] of permissions) {
      p.resolve({ outcome: { outcome: 'cancelled' } })
      emit({
        type: 'session.permission_resolved',
        sessionId: id,
        permissionId,
      })
    }
    permissions.clear()
  }
  const transport = connectAcp(
    launch,
    cwd,
    config?.env ?? {},
    {
      sessionUpdate: (notification) => {
        notificationLane = notificationLane.then(() => update(notification))
        return notificationLane
      },
      requestPermission: (request) => {
        const parsed = RequestPermissionRequestSchema.safeParse(request)
        if (!parsed.success)
          return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
        const input = z
          .record(z.string(), z.json())
          .safeParse(record(request.toolCall?.rawInput))
        if (!input.success)
          return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
        return new Promise((resolve) => {
          const permissionId = randomUUID()
          permissions.set(permissionId, { request, resolve })
          emit({
            type: 'session.permission',
            sessionId: id,
            request: {
              id: permissionId,
              name: request.toolCall.title ?? 'Tool',
              kind: 'tool',
              title: request.toolCall.title ?? undefined,
              input: input.data,
              detail: toolDetail(request.toolCall),
              actions: request.options.map((o) => ({
                id: o.optionId,
                label: o.name,
                behavior: o.kind.startsWith('allow') ? 'allow' : 'deny',
              })),
            },
          })
        })
      },
    },
    (error) => {
      if (state.failed) return
      state.failed = true
      cancelPermissions()
      terminalize('failed', error.message)
      if (!state.opening)
        emit({
          type: 'session.runtime_failed',
          sessionId: id,
          error: { message: error.message },
        })
    },
  )
  transports.add(transport)
  try {
    const initialized = await transport.request(
      transport.rpc.initialize({
        protocolVersion: 1,
        clientInfo: { name: 'paseo-agy-acp', version: '0.1.0' },
        clientCapabilities: {
          fs: { readTextFile: false, writeTextFile: false },
          terminal: false,
        },
      }),
    )
    return {
      id,
      cwd,
      config,
      state,
      initialized,
      transport,
      timeline,
      permissions,
      cancelPermissions,
      publishConfig,
      publishUsage,
      terminalize,
      drain,
      flush() {
        state.opening = false
        for (const item of buffered.splice(0)) update(item)
      },
      async close() {
        cancelPermissions()
        for (const [childId, stop] of children) {
          stop()
          emit({ type: 'session.closed', sessionId: childId })
        }
        children.clear()
        terminalize('canceled')
        await transport.close()
        transports.delete(transport)
      },
    }
  } catch (error) {
    await transport.close()
    transports.delete(transport)
    throw error
  }
}

async function configureSession(
  session: Session,
  changes: ProviderConfigChanges,
) {
  if (session.state.turn)
    throw new Error('Wait for the current turn before changing configuration')
  if (changes.settings && Object.keys(changes.settings).length)
    throw new Error('Custom settings are not supported')
  for (const [key, value] of [
    ['model', changes.model],
    ['mode', changes.mode],
    ['thought_level', changes.thinkingOption],
  ] as const) {
    if (value == null) continue
    const current = configState(session.state.metadata)
    const selected =
      key === 'model'
        ? current.model
        : key === 'mode'
          ? current.mode
          : current.thinkingOption
    if (value === selected) continue
    const option = session.state.metadata.configOptions?.find(
      (o) => o.id === key || o.category === key,
    )
    if (option) {
      if (option.currentValue === value) continue
      const result = await session.transport.request(
        session.transport.rpc.setSessionConfigOption({
          sessionId: session.state.nativeId,
          configId: option.id,
          value,
        }),
      )
      session.state.metadata.configOptions = result.configOptions
    } else if (key === 'mode') {
      await session.transport.request(
        session.transport.rpc.setSessionMode({
          sessionId: session.state.nativeId,
          modeId: value,
        }),
      )
      if (session.state.metadata.modes)
        session.state.metadata.modes.currentModeId = value
    } else throw new Error(`agy does not advertise ${key}`)
  }
  session.publishConfig()
}

function mcpServers(config: ProviderSessionConfig): McpServer[] {
  return Object.entries(config.mcpServers).map(([name, server]) =>
    server.type === 'stdio'
      ? {
          name,
          command: server.command,
          args: server.args ?? [],
          env: Object.entries(server.env ?? {}).map(([name, value]) => ({
            name,
            value,
          })),
        }
      : {
          name,
          type: server.type,
          url: server.url,
          headers: Object.entries(server.headers ?? {}).map(
            ([name, value]) => ({ name, value }),
          ),
        },
  )
}

async function promptSession(
  session: Session,
  input: Prompt,
  emit: SessionOptions['emit'],
) {
  const p = input.prompt
  if (session.state.prompts.has(p.clientMessageId)) return
  if (p.clearPendingPermissions) session.cancelPermissions()
  if (session.state.opening || session.state.loading)
    throw new Error('Session is not ready')
  if (session.state.turn) throw new Error('Session already has an active turn')
  if (session.state.failed)
    throw new Error('ACP runtime has exited; reopen the session')
  if (p.delivery === 'steer' || p.outputSchema)
    throw new Error('Steering and output schemas are not supported')
  const blocks: ContentBlock[] =
    p.input.type === 'command'
      ? [
          {
            type: 'text',
            text: `/${p.input.name} ${p.input.arguments}`.trim(),
          },
        ]
      : p.input.content.map(promptContent)
  const turnId = randomUUID()
  session.state.turn = turnId
  session.state.prompts.add(p.clientMessageId)
  session.timeline.boundary()
  emit({
    type: 'timeline.item',
    sessionId: session.id,
    item: {
      type: 'user_message',
      id: `${session.id}:user:${p.clientMessageId}`,
      text: blocks
        .map((c) => (c.type === 'text' ? c.text : '[image]'))
        .join('\n'),
      clientMessageId: p.clientMessageId,
    },
  })
  emit({
    type: 'session.prompt_result',
    sessionId: session.id,
    clientMessageId: p.clientMessageId,
    result: { type: 'turn', turnId },
  })
  emit({
    type: 'session.turn',
    sessionId: session.id,
    turnId,
    state: 'started',
  })
  try {
    const result = await session.transport.rpc.prompt({
      sessionId: session.state.nativeId,
      prompt: blocks,
    })
    await session.drain()
    if (result.usage) session.publishUsage(promptUsage(result.usage))
    const status = result.stopReason === 'cancelled' ? 'canceled' : 'completed'
    session.terminalize(status)
    emit({
      type: 'session.turn',
      sessionId: session.id,
      turnId,
      state: status,
    })
  } catch (error) {
    await session.drain()
    session.terminalize('failed', String(error))
    emit({
      type: 'session.turn',
      sessionId: session.id,
      turnId,
      state: 'failed',
      error: { message: String(error) },
    })
  } finally {
    session.state.turn = ''
    session.timeline.boundary()
    session.cancelPermissions()
  }
}
