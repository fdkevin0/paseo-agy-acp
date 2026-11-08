import type { AgyUsageSource } from './usage.ts'
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
  ProviderTimelineItem,
  ProviderUsage,
} from '@getpaseo/plugin/server/provider'
import { RequestPermissionRequestSchema } from './permissions.ts'
import {
  BrainTranscriptProjector,
  hasActiveBackgroundWork,
  readInboxState,
  readSubagents,
  readTranscriptRows,
  readLastStepIndex,
  settleBrainTurn,
  watchBrain,
  type BrainWatchHandle,
} from './brain.ts'
import { dataDirectory, removeSessionArtifacts } from './preflight.ts'
import { connectAcp } from './transport.ts'
import {
  configState,
  FULL_ACCESS_MODE_ID,
  Timeline,
  record,
  toolDetail,
  promptContent,
  contextUsage,
  promptUsage,
  sessionIdPattern,
  type SessionMetadata,
  type StepUsage,
} from './mapping.ts'

type Open = Extract<ProviderInput, { type: 'session.open' }>
type Prompt = Extract<ProviderInput, { type: 'session.prompt' }>
type Session = Awaited<ReturnType<typeof createSession>>
interface SessionOptions {
  usage: AgyUsageSource
  launch?: ProviderLaunch
  capabilities: readonly string[]
  emit: (event: ProviderEvent) => void
}

const SKIP_PERMISSIONS_FLAG = '--dangerously-skip-permissions'

function effectiveLaunch(
  launch: ProviderLaunch | undefined,
  skipPermissions?: boolean,
): ProviderLaunch | undefined {
  if (!skipPermissions) return launch
  const args = launch?.args ?? ['acp']
  return args.includes(SKIP_PERMISSIONS_FLAG)
    ? launch
    : {
        command: launch?.command ?? 'agy',
        args: [...args, SKIP_PERMISSIONS_FLAG],
        env: launch?.env ?? {},
      }
}

function sessionParams(cwd: string, config?: ProviderSessionConfig) {
  return {
    cwd,
    mcpServers: config ? mcpServers(config) : [],
    ...(config?.systemPrompt?.trim()
      ? {
          _meta: {
            antigravity: {
              systemPrompt: { append: config.systemPrompt },
            },
          },
        }
      : {}),
  }
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

  async start(id: string, cwd: string, config?: ProviderSessionConfig) {
    if (this.closed) throw new Error('Provider connection is closed')
    return createSession(id, cwd, config, this.options, this.transports)
  }
  async open(input: Open) {
    const { emit, capabilities } = this.options
    if (this.sessions.has(input.sessionId) || this.opening.has(input.sessionId))
      throw new Error('Session already open')
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
        const params = sessionParams(input.config.cwd, input.config)
        session.state.persisted = Boolean(input.persistence)
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
        const dir = this.options.usage.remember(
          this.options.launch,
          input.config.env,
          input.config.cwd,
        )
        if (session.transport.tier?.name)
          this.options.usage.tiers.set(dir, session.transport.tier.name)
        await session.drain()
        if (this.closed) throw new Error('Provider connection is closed')
        session.state.opened = true
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
        if (input.persistence) await session.replayTools()
        await configureSession(session, input.config)
        await session.drain()
        session.state.loading = false
        session.state.replay = true
        await session.waitForCommands()
        if (this.closed || session.state.closing || session.state.failed)
          throw new Error('Session closed before ready')
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
      await session.interrupt()
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
    await Promise.all([...this.sessions.values()].map((s) => s.close()))
    await Promise.all([...this.transports].map((t) => t.close()))
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
  const brainRoot = join(dataDirectory(launch, config?.env, cwd), 'brain')
  const state = {
    nativeId: '',
    opened: false,
    persisted: false,
    closing: false,
    skipPermissions: config?.mode === FULL_ACCESS_MODE_ID,
    turnHadContextUsage: false,
    turnHadModelOutput: false,
    turnToolCallIds: [] as string[],
    opening: true,
    loading: true,
    replay: true,
    turn: '',
    turnAbort: null as AbortController | null,
    interrupting: null as Promise<void> | null,
    failed: false,
    metadata: { sessionId: '' } as NewSessionResponse & SessionMetadata,
    prompts: new Set<string>(),
  }
  const buffered: SessionNotification[] = []
  const children = new Map<string, BrainWatchHandle>()
  let notificationLane = Promise.resolve()
  let usage: ProviderUsage = {}
  let commandsReceived = false
  let emittedCommands: string | undefined
  let commandWaiter: (() => void) | null = null
  let closing: Promise<void> | undefined
  function resolveCommands() {
    commandWaiter?.()
    commandWaiter = null
  }
  async function waitForCommands(timeoutMs = 1000) {
    if (commandsReceived || state.failed || state.closing) return
    let timer: NodeJS.Timeout | undefined
    try {
      await new Promise<void>((resolve) => {
        commandWaiter = resolve
        timer = setTimeout(resolve, timeoutMs)
        timer.unref()
      })
      await drain()
    } finally {
      clearTimeout(timer)
      commandWaiter = null
    }
  }
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
  async function syncKilledChildren(
    subagents?: Awaited<ReturnType<typeof readSubagents>>,
  ) {
    const list =
      subagents ??
      (await readSubagents(brainRoot, state.nativeId).catch(() => []))
    for (const sub of list)
      if (sub.state === 'SUBAGENT_STATE_KILLED') {
        const childId = `${id}:child:${sub.conversationId}`
        children.get(childId)?.cancel()
        children.delete(childId)
      }
  }
  function emitItem(item: ProviderTimelineItem, timestamp?: string) {
    timeline.track(item)
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
            watchBrain(brainRoot, nativeId, childId, emit, 1000, () =>
              Boolean(state.turn),
            ),
          )
          emit({ type: 'session.ready', sessionId: childId })
        }
        item.detail.childSessionId = childId
      }
    }
    if (
      item.type === 'tool_call' &&
      item.status === 'completed' &&
      /\bmanage_subagents\b/.test(item.name)
    ) {
      void syncKilledChildren()
    }
    if (state.replay)
      emit({
        type: 'timeline.item',
        sessionId: id,
        item,
        ...(timestamp ? { timestamp } : {}),
      })
  }
  function update(notification: SessionNotification) {
    if (state.closing || state.failed) return
    if (state.nativeId && notification.sessionId !== state.nativeId) return
    if (state.opening) {
      buffered.push(notification)
      return
    }
    if (notification.sessionId !== state.nativeId) return
    const u = notification.update
    if (u.sessionUpdate === 'config_option_update') {
      state.metadata.configOptions = u.configOptions
      if (configState({ ...state.metadata, fullAccess: false }).mode === 'plan')
        state.metadata.fullAccess = false
      publishConfig()
    } else if (u.sessionUpdate === 'available_commands_update') {
      const next = u.availableCommands.map((c) => ({
        name: c.name,
        description: c.description,
        argumentHint: c.input?.hint,
      }))
      const serialized = JSON.stringify(next)
      if (state.opened && serialized !== emittedCommands) {
        emittedCommands = serialized
        emit({
          type: 'session.commands',
          sessionId: id,
          commands: next,
        })
      }
      commandsReceived = true
      resolveCommands()
    } else if (u.sessionUpdate === 'usage_update') {
      const next = contextUsage(u)
      if (next && (!state.loading || state.replay)) {
        if (state.turn) state.turnHadContextUsage = true
        publishUsage(next)
      }
    } else {
      if (state.loading && !state.replay) return
      if (u.sessionUpdate === 'user_message_chunk' && !state.loading) {
        timeline.boundary()
        return
      }
      if (
        u.sessionUpdate === 'tool_call' &&
        !state.turnToolCallIds.includes(u.toolCallId)
      )
        state.turnToolCallIds.push(u.toolCallId)
      if (
        state.turn &&
        (u.sessionUpdate === 'agent_message_chunk' ||
          u.sessionUpdate === 'agent_thought_chunk' ||
          u.sessionUpdate === 'tool_call')
      )
        state.turnHadModelOutput = true
      const item = timeline.update(u)
      if (item) emitItem(item)
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
  const client = {
    sessionUpdate: (notification: SessionNotification) => {
      notificationLane = notificationLane.then(() => update(notification))
      return notificationLane
    },
    requestPermission: (request: RequestPermissionRequest) => {
      const parsed = RequestPermissionRequestSchema.safeParse(request)
      if (
        !parsed.success ||
        !state.nativeId ||
        request.sessionId !== state.nativeId ||
        state.closing ||
        state.failed
      )
        return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
      const input = z
        .record(z.string(), z.json())
        .safeParse(record(request.toolCall?.rawInput))
      if (!input.success)
        return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
      if (state.metadata.fullAccess) {
        const allow =
          request.options.find((o) => o.kind === 'allow_once') ??
          request.options.find((o) => o.kind.startsWith('allow'))
        return Promise.resolve({
          outcome: allow
            ? { outcome: 'selected' as const, optionId: allow.optionId }
            : { outcome: 'cancelled' as const },
        })
      }
      return new Promise<RequestPermissionResponse>((resolve) => {
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
  }
  const onExit = (error: Error) => {
    if (state.failed || state.closing) return
    state.failed = true
    state.turnAbort?.abort(error)
    resolveCommands()
    cancelPermissions()
    terminalize('failed', error.message)
    if (!state.opening && state.opened)
      emit({
        type: 'session.runtime_failed',
        sessionId: id,
        error: { message: error.message },
      })
  }
  let currentTransport = connectAcp(
    effectiveLaunch(launch, state.skipPermissions),
    cwd,
    config?.env ?? {},
    client,
    onExit,
  )
  const initialTransport = currentTransport
  transports.add(initialTransport)
  void initialTransport.exited
    .then(() => initialTransport.close())
    .then(
      () => transports.delete(initialTransport),
      () => undefined,
    )
  try {
    let currentInitialized = await currentTransport.request(
      currentTransport.rpc.initialize({
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
      brainRoot,
      state,
      get initialized() {
        return currentInitialized
      },
      get transport() {
        return currentTransport
      },
      timeline,
      permissions,
      cancelPermissions,
      publishConfig,
      publishUsage,
      terminalize,
      emitItem,
      hasChild(nativeId: string) {
        return children.has(`${id}:child:${nativeId}`)
      },
      syncKilledChildren,
      drain,
      waitForCommands,
      async replayTools() {
        if (!state.replay) return
        const [inbox, subagents, rows] = await Promise.all([
          readInboxState(brainRoot, state.nativeId),
          readSubagents(brainRoot, state.nativeId),
          readTranscriptRows(brainRoot, state.nativeId),
        ])
        const projector = new BrainTranscriptProjector(id)
        projector.syncMetadata(inbox, subagents)
        projector.bindToolCallIds(state.turnToolCallIds)
        for (const row of rows)
          for (const item of projector.project(row)) {
            if (item.type !== 'tool_call') continue
            await Promise.all(
              [...children.values()].map((child) => child.ready),
            )
            emitItem(item)
          }
        // A child opened by the last replayed tool must read its history before a historical kill cancels it.
        await Promise.all([...children.values()].map((child) => child.ready))
        await syncKilledChildren(subagents)
      },
      flush() {
        state.opening = false
        for (const item of buffered.splice(0)) update(item)
      },
      async interrupt(): Promise<void> {
        if (state.interrupting) return state.interrupting
        state.turnAbort?.abort()
        cancelPermissions()
        const hadRunningWatchedChildren = [...children.values()].some((child) =>
          child.isRunning(),
        )
        for (const [childId, child] of children)
          if (child.isRunning()) {
            child.cancel()
            children.delete(childId)
          }
        // Native session/cancel leaves background tasks alive; stop their owner when background work is active.
        state.interrupting = (async () => {
          const needsRelaunch =
            hadRunningWatchedChildren ||
            (await hasActiveBackgroundWork(brainRoot, state.nativeId).catch(
              () => true,
            ))
          if (needsRelaunch) {
            for (const child of children.values()) child.cancel()
            children.clear()
          }
          try {
            await currentTransport.request(
              currentTransport.rpc.cancel({ sessionId: state.nativeId }),
            )
          } catch (error) {
            // A concurrent close already stops the runtime; its closed stream is not an interrupt failure.
            if (!state.closing) throw error
          } finally {
            if (needsRelaunch) await this.relaunch(state.skipPermissions, true)
          }
        })()
        try {
          await state.interrupting
        } finally {
          state.interrupting = null
        }
      },
      async relaunch(skipPermissions: boolean, force = false): Promise<void> {
        if (
          !force &&
          (state.skipPermissions === skipPermissions ||
            !currentInitialized.agentCapabilities?.loadSession)
        )
          return
        const prev = configState(state.metadata)
        const nativeMode = configState({
          ...state.metadata,
          fullAccess: false,
        }).mode
        state.loading = true
        state.replay = false
        try {
          await currentTransport.close()
          if (state.closing) return
          if (!currentInitialized.agentCapabilities?.loadSession)
            throw new Error(
              'agy cannot reload after interruption; reopen the session',
            )
          const next = connectAcp(
            effectiveLaunch(launch, skipPermissions),
            cwd,
            config?.env ?? {},
            client,
            onExit,
          )
          currentTransport = next
          transports.add(next)
          void next.exited
            .then(() => next.close())
            .then(
              () => transports.delete(next),
              () => undefined,
            )
          currentInitialized = await next.request(
            next.rpc.initialize({
              protocolVersion: 1,
              clientInfo: { name: 'paseo-agy-acp', version: '0.1.0' },
              clientCapabilities: {
                fs: { readTextFile: false, writeTextFile: false },
                terminal: false,
              },
            }),
          )
          if (state.closing) return
          state.skipPermissions = skipPermissions
          state.metadata = {
            ...(await next.request(
              next.rpc.loadSession({
                ...sessionParams(cwd, config),
                sessionId: state.nativeId,
              }),
            )),
            sessionId: state.nativeId,
            models: next.models,
            fullAccess: skipPermissions,
          }
          await drain()
          if (state.closing) return
          for (const [k, v] of [
            ['model', prev.model],
            ['thought_level', prev.thinkingOption],
            ['mode', force ? nativeMode : undefined],
          ] as const) {
            await applyNativeConfigOption(
              next,
              state.nativeId,
              state.metadata,
              k,
              v,
            )
          }
        } catch (error) {
          onExit(error instanceof Error ? error : new Error(String(error)))
          throw error
        } finally {
          state.loading = false
          state.replay = true
        }
      },
      close(): Promise<void> {
        if (closing) return closing
        state.closing = true
        state.turnAbort?.abort()
        resolveCommands()
        closing = (async () => {
          cancelPermissions()
          for (const [childId, stop] of children) {
            stop()
            emit({ type: 'session.closed', sessionId: childId })
          }
          children.clear()
          terminalize('canceled')
          const disposable =
            !state.persisted &&
            state.prompts.size === 0 &&
            sessionIdPattern.test(state.nativeId)
          if (disposable)
            await currentTransport
              .request(
                currentTransport.rpc.closeSession({
                  sessionId: state.nativeId,
                }),
              )
              .catch(() => undefined)
          const dir = dataDirectory(launch, config?.env, cwd)
          const stopped = await currentTransport.close().then(
            () => true,
            () => false,
          )
          if (disposable && stopped)
            await removeSessionArtifacts(dir, state.nativeId).catch(
              () => undefined,
            )
        })()
        return closing
      },
    }
  } catch (error) {
    await currentTransport.close()
    throw error
  }
}

async function applyNativeConfigOption(
  transport: ReturnType<typeof connectAcp>,
  nativeId: string,
  metadata: NewSessionResponse & SessionMetadata,
  key: 'model' | 'mode' | 'thought_level',
  value: string | undefined,
  strict = false,
) {
  if (!value) return
  const option = metadata.configOptions?.find(
    (o) => o.id === key || o.category === key,
  )
  if (!option) {
    if (strict) throw new Error(`agy does not advertise ${key}`)
    return
  }
  if (option.currentValue === value) return
  const result = await transport.request(
    transport.rpc.setSessionConfigOption({
      sessionId: nativeId,
      configId: option.id,
      value,
    }),
  )
  metadata.configOptions = result.configOptions
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
    if (key === 'mode' && value === FULL_ACCESS_MODE_ID) {
      await session.relaunch(true)
      session.state.metadata.fullAccess = true
      const nativeModes = configState({
        ...session.state.metadata,
        fullAccess: false,
      }).modes.map((m) => m.id)
      const target = ['accept-edits', 'default'].find((m) =>
        nativeModes.includes(m),
      )
      await applyNativeConfigOption(
        session.transport,
        session.state.nativeId,
        session.state.metadata,
        key,
        target,
      )
      continue
    }
    if (key === 'mode') {
      await session.relaunch(false)
      session.state.metadata.fullAccess = false
    }
    await applyNativeConfigOption(
      session.transport,
      session.state.nativeId,
      session.state.metadata,
      key,
      value,
      true,
    )
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
  if (session.state.closing) throw new Error('Session is closing')
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
  const turnAbort = new AbortController()
  session.state.turn = turnId
  session.state.turnAbort = turnAbort
  session.state.turnHadContextUsage = false
  session.state.turnHadModelOutput = false
  session.state.turnToolCallIds = []
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
    const minStepIndex = await readLastStepIndex(
      session.brainRoot,
      session.state.nativeId,
    ).catch(() => -1)
    turnAbort.signal.throwIfAborted()
    const result = await session.transport.rpc.prompt({
      sessionId: session.state.nativeId,
      prompt: blocks,
    })
    await session.drain()
    let status: 'completed' | 'canceled' | 'failed' =
      result.stopReason === 'cancelled' ? 'canceled' : 'completed'
    let turnError: string | undefined
    let lastStep: StepUsage | undefined
    if (status === 'completed') {
      const settled = await settleBrainTurn(
        session.brainRoot,
        session.state.nativeId,
        session.id,
        session.emitItem,
        {
          signal: turnAbort.signal,
          minStepIndex,
          acpToolCallIds: session.state.turnToolCallIds,
          shouldEmitPreWakeupTool: (item) =>
            session.timeline.isRunning(item.id) ||
            (item.detail.type === 'sub_agent' &&
              item.detail.childSessionId !== undefined &&
              !session.hasChild(item.detail.childSessionId)),
          // A model turn always writes USER_INPUT; give agy a moment to flush it. Native commands may write none.
          transcriptGraceMs: session.state.turnHadModelOutput ? 2000 : 0,
          isWaitingOnUser: () => session.permissions.size > 0,
        },
      )
      await session.syncKilledChildren()
      status = settled.status
      turnError = settled.error
      lastStep = settled.lastStepUsage
    }
    if (result.usage)
      session.publishUsage(
        promptUsage(result.usage, {
          model: configState(session.state.metadata).model,
          preserveContext: session.state.turnHadContextUsage,
          lastStep,
        }),
      )
    await session.state.interrupting
    session.terminalize(status, turnError)
    emit({
      type: 'session.turn',
      sessionId: session.id,
      turnId,
      state: status,
      ...(status === 'failed' && turnError
        ? { error: { message: turnError } }
        : {}),
    })
  } catch (error) {
    await session.drain()
    const restartError = await session.state.interrupting?.then(
      () => undefined,
      (error: unknown) => error,
    )
    error = restartError ?? error
    const status =
      !restartError &&
      turnAbort.signal.aborted &&
      turnAbort.signal.reason instanceof Error &&
      turnAbort.signal.reason.name === 'AbortError'
        ? 'canceled'
        : 'failed'
    session.terminalize(status, String(error))
    emit({
      type: 'session.turn',
      sessionId: session.id,
      turnId,
      state: status,
      ...(status === 'failed' ? { error: { message: String(error) } } : {}),
    })
  } finally {
    session.state.turnAbort = null
    session.state.turn = ''
    session.timeline.boundary()
    session.cancelPermissions()
  }
}
