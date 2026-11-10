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
import {
  RequestPermissionRequestSchema,
  buildElicitationResponse,
  formatQuestionAnswersText,
  isPreapprovedMcpTool,
  normalizeElicitationQuestions,
  permissionKind,
  pickPermissionOption,
  sameNormalizedQuestions,
  toQuestionPermissionInput,
  type NormalizedQuestion,
} from './permissions.ts'
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
  type SubagentRecord,
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
type PermissionResponseInput = Extract<
  ProviderInput,
  { type: 'session.permission' }
>['response']
type PendingQuestionPermission = {
  type: 'question'
  questions: NormalizedQuestion[]
  promise: Promise<PermissionResponseInput | null>
  resolve: (r: PermissionResponseInput | null) => void
  fromElicitation: boolean
  fromBrain: boolean
}
type PendingPermission =
  | {
      type: 'acp'
      request: RequestPermissionRequest
      resolve: (r: RequestPermissionResponse) => void
    }
  | PendingQuestionPermission
type Session = Awaited<ReturnType<typeof createSession>>
interface SessionOptions {
  usage: AgyUsageSource
  launch?: ProviderLaunch
  capabilities: readonly string[]
  emit: (event: ProviderEvent) => void
  stallTimeoutMs?: number
}

const SKIP_PERMISSIONS_FLAG = '--dangerously-skip-permissions'

const isSpawn = (name: string) => name.includes('invoke_subagent')
const isMessage = (name: string) => name.includes('send_message')

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
          const canResume =
            input.history === 'skip' &&
            Boolean(
              session.initialized.agentCapabilities?.sessionCapabilities
                ?.resume,
            )
          if (!canResume && !session.initialized.agentCapabilities?.loadSession)
            throw new Error('agy does not support session/load')
          session.state.nativeId = data.sessionId as string
          session.state.metadata = {
            ...(await session.transport.request(
              canResume
                ? session.transport.rpc.resumeSession({
                    ...params,
                    sessionId: session.state.nativeId,
                  })
                : session.transport.rpc.loadSession({
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
          ...(input.config.title ? { title: input.config.title } : {}),
          ...(input.config.persist !== false
            ? {
                persistence: {
                  version: 1,
                  data: { sessionId: session.state.nativeId },
                },
              }
            : {}),
        })
        await session.refreshSubagents()
        await session.flush()
        if (input.persistence) await session.replayTools()
        session.state.turnToolCallIds = []
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
      session.permissions.delete(input.permissionId)
      if (permission.type === 'acp') {
        const option = pickPermissionOption(
          permission.request.options,
          input.response.behavior,
          input.response.selectedActionId,
        )
        permission.resolve({
          outcome: option
            ? { outcome: 'selected', optionId: option.optionId }
            : { outcome: 'cancelled' },
        })
      } else {
        permission.resolve(input.response)
      }
      emit({
        type: 'session.permission_resolved',
        sessionId: session.id,
        permissionId: input.permissionId,
      })
      if (input.response.behavior === 'deny' && input.response.interrupt)
        await session.interrupt()
      return
    }
    if (input.type === 'session.configure')
      await configureSession(session, input.changes)
    else if (input.type === 'session.interrupt') {
      await session.interrupt()
    } else if (input.type === 'session.can_stop_runtime') {
      emit({
        type: input.type,
        requestId: input.requestId,
        sessionId: session.id,
        canStop: await session.canStop(),
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
  const permissions = new Map<string, PendingPermission>()
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
    inSettle: false,
    handledElicitations: [] as Array<{
      questions: NormalizedQuestion[]
      answerText: string | null
      wokeUp: boolean
    }>,
    suppressContinuationAcp: false,
    preStagedQuestion: undefined as
      | {
          questions: NormalizedQuestion[]
          response: PermissionResponseInput
        }
      | undefined,
    turnToolCallIds: [] as string[],
    lastKnownStepIndex: -1,
    wakeup: {
      pendingReplay: false,
      suppressEcho: false,
      carriedUsage: undefined as StepUsage | undefined,
    },
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
  const childParents = new Map<string, string>()
  const killedChildren = new Set<string>()
  const resuming = new Set<string>()
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
  const childKey = (nativeId: string) => `${id}:child:${nativeId}`
  async function syncKilledChildren(
    subagents?: readonly SubagentRecord[],
    parentNativeId = state.nativeId,
  ) {
    rememberSubagents(
      subagents ??
        (await readSubagents(brainRoot, parentNativeId).catch(() => [])),
      parentNativeId,
    )
    // Include descendants discovered by their own watchers, not just this parent's descriptors.
    await Promise.all(
      [...killedChildren].map((childId) => children.get(childId)?.cancel()),
    )
  }
  function rememberSubagents(
    subagents: readonly SubagentRecord[],
    parentNativeId = state.nativeId,
  ) {
    for (const sub of subagents) {
      if (
        sub.conversationId !== state.nativeId &&
        sub.conversationId !== parentNativeId &&
        !childParents.has(sub.conversationId)
      )
        childParents.set(sub.conversationId, parentNativeId)
      if (sub.state === 'SUBAGENT_STATE_KILLED') {
        const childId = childKey(sub.conversationId)
        killedChildren.add(childId)
        if (!state.loading) void children.get(childId)?.cancel()
      }
    }
  }
  async function refreshSubagents() {
    // Optional ownership metadata must not poison the serialized ACP notification lane.
    const subagents = await readSubagents(brainRoot, state.nativeId).catch(
      () => [],
    )
    if (state.loading) rememberSubagents(subagents)
    else await syncKilledChildren(subagents)
  }
  function watchChild(
    nativeId: string,
    childId: string,
    resume?: ReturnType<BrainWatchHandle['resumePoint']>,
  ) {
    const child = watchBrain(
      brainRoot,
      nativeId,
      childId,
      (event) => {
        if (event.type === 'timeline.item')
          linkSubsession(childId, nativeId, event.item)
        emit(event)
      },
      1000,
      () => Boolean(state.turn),
      resume,
      (subagents) => rememberSubagents(subagents, nativeId),
    )
    children.set(childId, child)
    void child.ready.then(() => {
      if (killedChildren.has(childId)) return child.cancel()
    })
  }
  /** A child stopped by interruption (not killed) can be woken by a message. */
  function isResumable(childId: string) {
    return (
      !killedChildren.has(childId) &&
      children.get(childId)?.isStopping() === true
    )
  }
  /** Resume watching the same child session once its previous watcher has drained. */
  function resumeChild(
    nativeId: string,
    childId: string,
    existing: BrainWatchHandle,
  ) {
    if (resuming.has(childId)) return
    resuming.add(childId)
    void existing.cancel().then(() => {
      resuming.delete(childId)
      if (
        state.closing ||
        state.failed ||
        killedChildren.has(childId) ||
        children.get(childId) !== existing
      )
        return
      watchChild(nativeId, childId, existing.resumePoint())
    })
  }
  function canLinkChild(
    nativeId: string,
    parentNativeId: string,
    name: string,
  ) {
    const childId = childKey(nativeId)
    const spawn = isSpawn(name)
    // Killed children are only linked for history, their own spawn call, or an already open session.
    const killedAllowed =
      state.loading ||
      spawn ||
      !killedChildren.has(childId) ||
      children.has(childId)
    // Only a recorded parent, or the spawning call of an unseen child, owns it.
    const owned =
      childParents.get(nativeId) === parentNativeId ||
      (spawn && !childParents.has(nativeId))
    return (
      !state.closing &&
      !state.failed &&
      capabilities.includes('session.subsession') &&
      sessionIdPattern.test(nativeId) &&
      nativeId !== state.nativeId &&
      killedAllowed &&
      owned
    )
  }
  /** Open or resume direct child sub-sessions for root or nested subagents. */
  function linkSubsession(
    parentSessionId: string,
    parentNativeId: string,
    item: ProviderTimelineItem,
  ) {
    if (item.type === 'tool_call' && item.detail.type === 'sub_agent') {
      const nativeId = item.detail.childSessionId
      delete item.detail.childSessionId
      if (!nativeId || !canLinkChild(nativeId, parentNativeId, item.name))
        return
      childParents.set(nativeId, parentNativeId)
      const childId = childKey(nativeId)
      const existing = children.get(childId)
      if (
        existing &&
        isResumable(childId) &&
        !state.loading &&
        isMessage(item.name)
      ) {
        resumeChild(nativeId, childId, existing)
      } else if (!existing) {
        emit({
          type: 'session.opened',
          sessionId: childId,
          parentSessionId,
          toolCallId: item.callId,
          cwd,
          capabilities: ['session.subsession'],
          restoration: 'parent',
          title: item.detail.subAgentType ?? 'Antigravity subagent',
        })
        watchChild(nativeId, childId)
        emit({ type: 'session.ready', sessionId: childId })
      }
      item.detail.childSessionId = childId
    }
    if (
      item.type === 'tool_call' &&
      item.status === 'completed' &&
      item.name.includes('manage_subagents')
    ) {
      void syncKilledChildren(undefined, parentNativeId)
    }
  }
  /** Brain-projected items are not yet known to the ACP timeline. */
  function emitItem(item: ProviderTimelineItem, timestamp?: string) {
    timeline.track(item)
    publishItem(item, timestamp)
  }
  /** Link child sessions and publish an item the timeline already tracks. */
  function publishItem(item: ProviderTimelineItem, timestamp?: string) {
    linkSubsession(id, state.nativeId, item)
    if (state.replay)
      emit({
        type: 'timeline.item',
        sessionId: id,
        item,
        ...(timestamp ? { timestamp } : {}),
      })
  }
  async function update(notification: SessionNotification) {
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
      if (state.wakeup.suppressEcho || state.suppressContinuationAcp) return
      const next = contextUsage(u)
      if (next && (!state.loading || state.replay)) {
        if (state.turn) state.turnHadContextUsage = true
        publishUsage(next)
      }
    } else {
      if (state.loading && !state.replay) return
      if (state.suppressContinuationAcp) {
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
        return
      }
      if (u.sessionUpdate === 'user_message_chunk' && !state.loading) {
        state.wakeup.suppressEcho = false
        timeline.boundary()
        return
      }
      if (state.wakeup.suppressEcho) return
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
      // Timeline.update already tracks the ACP item.
      const item = timeline.update(u)
      if (item) {
        if (
          item.type === 'tool_call' &&
          item.detail.type === 'sub_agent' &&
          isMessage(item.name)
        ) {
          // Terminalization may run while ownership is being read.
          const nativeId = item.detail.childSessionId
          delete item.detail.childSessionId
          await refreshSubagents()
          // Do not restore a native ID that a later terminalization could publish.
          if (state.closing || state.failed) return
          if (nativeId !== undefined) item.detail.childSessionId = nativeId
        }
        publishItem(item)
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
      if (p.type === 'acp') p.resolve({ outcome: { outcome: 'cancelled' } })
      else p.resolve(null)
      emit({
        type: 'session.permission_resolved',
        sessionId: id,
        permissionId,
      })
    }
    permissions.clear()
  }
  function askUserQuestion(
    questions: NormalizedQuestion[],
    title?: string,
    source: 'elicitation' | 'brain' = 'elicitation',
  ): {
    promise: Promise<PermissionResponseInput | null>
    entry: PendingQuestionPermission
  } | null {
    if (
      !questions.length ||
      !state.nativeId ||
      !state.turn ||
      state.closing ||
      state.failed
    )
      return null
    for (const p of permissions.values()) {
      if (
        p.type === 'question' &&
        (source === 'elicitation' ? !p.fromElicitation : !p.fromBrain) &&
        sameNormalizedQuestions(p.questions, questions)
      ) {
        if (source === 'elicitation') p.fromElicitation = true
        else p.fromBrain = true
        return { promise: p.promise, entry: p }
      }
    }
    const input = toQuestionPermissionInput(questions)
    const detail = toolDetail({
      title: 'ask_question',
      rawInput: input,
    })
    let resolve!: (r: PermissionResponseInput | null) => void
    const promise = new Promise<PermissionResponseInput | null>((res) => {
      resolve = res
    })
    const entry: PendingQuestionPermission = {
      type: 'question',
      questions,
      promise,
      resolve,
      fromElicitation: source === 'elicitation',
      fromBrain: source === 'brain',
    }
    const permissionId = randomUUID()
    permissions.set(permissionId, entry)
    emit({
      type: 'session.permission',
      sessionId: id,
      request: {
        id: permissionId,
        name: 'ask_question',
        kind: 'question',
        title: title ?? questions[0]?.question ?? 'Question',
        input,
        detail,
        actions: [
          { id: 'allow', label: 'Submit', behavior: 'allow' },
          { id: 'deny', label: 'Cancel', behavior: 'deny' },
        ],
      },
    })
    return { promise, entry }
  }
  const client = {
    sessionUpdate: (notification: SessionNotification) => {
      notificationLane = notificationLane.then(() => update(notification))
      return notificationLane
    },
    requestElicitation: async (params: Record<string, unknown>) => {
      if (
        !state.nativeId ||
        (typeof params.sessionId === 'string' &&
          params.sessionId !== state.nativeId) ||
        !state.turn ||
        state.closing ||
        state.failed
      )
        return { action: 'cancel' }
      const questions = normalizeElicitationQuestions(params)
      if (!questions.length) return { action: 'cancel' }
      if (state.preStagedQuestion) {
        const staged = state.preStagedQuestion
        state.preStagedQuestion = undefined
        if (sameNormalizedQuestions(staged.questions, questions)) {
          return buildElicitationResponse(
            questions,
            staged.response,
            staged.questions,
          )
        }
      }
      const pending = askUserQuestion(
        questions,
        typeof params.message === 'string' && params.message.trim()
          ? params.message.trim()
          : undefined,
        'elicitation',
      )
      if (!pending) return { action: 'cancel' }
      const response = await pending.promise
      if (!response) return { action: 'cancel' }
      if (!pending.entry.fromBrain) {
        state.handledElicitations.push({
          questions,
          answerText:
            formatQuestionAnswersText(pending.entry.questions, response) ||
            null,
          wokeUp: state.inSettle,
        })
      }
      return buildElicitationResponse(
        questions,
        response,
        pending.entry.questions,
      )
    },
    requestPermission: (request: RequestPermissionRequest) => {
      const parsed = RequestPermissionRequestSchema.safeParse(request)
      if (
        !parsed.success ||
        !state.nativeId ||
        request.sessionId !== state.nativeId ||
        !state.turn ||
        state.closing ||
        state.failed
      )
        return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
      const input = z
        .record(z.string(), z.json())
        .safeParse(record(request.toolCall?.rawInput))
      if (!input.success)
        return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
      const detail = toolDetail(request.toolCall)
      const kind = permissionKind(request, detail)
      const preapproved = isPreapprovedMcpTool(
        request.toolCall.title,
        input.data,
        config?.toolPolicy,
      )
      if (kind !== 'question' && (state.metadata.fullAccess || preapproved)) {
        const allow = pickPermissionOption(request.options, 'allow')
        return Promise.resolve({
          outcome: allow
            ? { outcome: 'selected' as const, optionId: allow.optionId }
            : { outcome: 'cancelled' as const },
        })
      }
      return new Promise<RequestPermissionResponse>((resolve) => {
        const permissionId = randomUUID()
        permissions.set(permissionId, { type: 'acp', request, resolve })
        emit({
          type: 'session.permission',
          sessionId: id,
          request: {
            id: permissionId,
            name: request.toolCall.title ?? 'Tool',
            kind,
            title: request.toolCall.title ?? undefined,
            input: input.data,
            detail,
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
  /** Start an owned ACP runtime; it leaves the connection's transports once it exits and stops. */
  function startRuntime(skipPermissions: boolean) {
    const transport = connectAcp(
      effectiveLaunch(launch, skipPermissions),
      cwd,
      config?.env ?? {},
      client,
      onExit,
    )
    transports.add(transport)
    void transport.exited
      .then(() => transport.close())
      .then(
        () => transports.delete(transport),
        () => undefined,
      )
    return transport
  }
  function initializeRuntime(transport: ReturnType<typeof connectAcp>) {
    return transport.request(
      transport.rpc.initialize({
        protocolVersion: 1,
        clientInfo: { name: 'paseo-agy-acp', version: '0.1.0' },
        clientCapabilities: {
          fs: { readTextFile: false, writeTextFile: false },
          terminal: false,
          elicitation: { form: {} },
        },
      }),
    )
  }
  let currentTransport = startRuntime(state.skipPermissions)
  try {
    let currentInitialized = await initializeRuntime(currentTransport)
    return {
      id,
      cwd,
      config,
      brainRoot,
      stallTimeoutMs: options.stallTimeoutMs,
      state,
      get initialized() {
        return currentInitialized
      },
      get transport() {
        return currentTransport
      },
      timeline,
      permissions,
      askUserQuestion,
      cancelPermissions,
      publishConfig,
      publishUsage,
      terminalize,
      emitItem,
      resetWakeup() {
        state.wakeup = {
          pendingReplay: false,
          suppressEcho: false,
          carriedUsage: undefined,
        }
      },
      hasChild(nativeId: string) {
        return children.has(childKey(nativeId))
      },
      isChildResumable(nativeId: string) {
        return isResumable(childKey(nativeId))
      },
      syncKilledChildren,
      rememberSubagents,
      refreshSubagents,
      canLinkChild,
      drain,
      waitForCommands,
      async replayTools() {
        if (!state.replay) return
        const [inbox, subagents, rows] = await Promise.all([
          readInboxState(brainRoot, state.nativeId),
          readSubagents(brainRoot, state.nativeId),
          readTranscriptRows(brainRoot, state.nativeId),
        ])
        rememberSubagents(subagents)
        const projector = new BrainTranscriptProjector(id)
        projector.syncMetadata(inbox, subagents)
        projector.bindToolCallIds(state.turnToolCallIds)
        for (const row of rows) {
          const stepIndex = Number(row.step_index)
          if (
            Number.isInteger(stepIndex) &&
            stepIndex > state.lastKnownStepIndex
          )
            state.lastKnownStepIndex = stepIndex
          for (const item of projector.project(row)) {
            if (item.type !== 'tool_call' || item.status === 'running') continue
            await Promise.all(
              [...children.values()].map((child) => child.ready),
            )
            emitItem(item)
          }
        }
        // A child opened by the last replayed tool must read its history before a historical kill cancels it.
        let knownChildren = 0
        while (knownChildren !== children.size) {
          knownChildren = children.size
          await Promise.all([...children.values()].map((child) => child.ready))
        }
        await syncKilledChildren(subagents)
      },
      async canStop(): Promise<boolean> {
        if (
          state.closing ||
          state.opening ||
          state.loading ||
          state.turn ||
          state.interrupting ||
          permissions.size > 0 ||
          [...children.values()].some((child) => child.isRunning())
        )
          return false
        return !(await hasActiveBackgroundWork(brainRoot, state.nativeId, {
          includeDaemons: true,
        }).catch(() => true))
      },
      async flush() {
        state.opening = false
        for (const item of buffered.splice(0))
          notificationLane = notificationLane.then(() => update(item))
        await notificationLane
      },
      async interrupt(): Promise<void> {
        if (state.interrupting) return state.interrupting
        state.turnAbort?.abort()
        cancelPermissions()
        // Children stay registered when canceled so they are neither reopened nor left unclosed.
        const runningChildren = [...children.values()].filter((child) =>
          child.isRunning(),
        )
        const canceling = runningChildren.map((child) => child.cancel())
        // Native session/cancel leaves background tasks alive; stop their owner when background work is active.
        state.interrupting = (async () => {
          await Promise.all(canceling)
          const needsRelaunch =
            runningChildren.length > 0 ||
            (await hasActiveBackgroundWork(brainRoot, state.nativeId).catch(
              () => true,
            ))
          if (needsRelaunch)
            await Promise.all([...children.values()].map((c) => c.cancel()))
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
          const next = startRuntime(skipPermissions)
          currentTransport = next
          currentInitialized = await initializeRuntime(next)
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
          this.resetWakeup()
          state.turnToolCallIds = []
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
            (config?.persist === false || state.prompts.size === 0) &&
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
  const expectsUserEcho =
    p.input.type === 'message' &&
    blocks.some((c) => c.type !== 'text' || c.text.trim().length > 0)
  session.state.turn = turnId
  session.state.turnAbort = turnAbort
  session.state.turnHadContextUsage = false
  session.state.turnHadModelOutput = false
  session.state.inSettle = false
  session.state.handledElicitations = []
  session.state.suppressContinuationAcp = false
  session.state.preStagedQuestion = undefined
  session.state.turnToolCallIds = []
  session.state.wakeup.suppressEcho =
    session.state.wakeup.pendingReplay && expectsUserEcho
  session.state.wakeup.pendingReplay = false
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
    const readStep = await readLastStepIndex(
      session.brainRoot,
      session.state.nativeId,
    ).catch(() => -1)
    const minStepIndex = Math.max(session.state.lastKnownStepIndex, readStep)
    session.state.lastKnownStepIndex = minStepIndex
    turnAbort.signal.throwIfAborted()
    const result = await session.transport.rpc.prompt({
      sessionId: session.state.nativeId,
      prompt: blocks,
    })
    await session.drain()
    const suppressedForeground = session.state.wakeup.suppressEcho
    session.state.wakeup.suppressEcho = false
    let status: 'completed' | 'canceled' | 'failed' =
      result.stopReason === 'cancelled' ? 'canceled' : 'completed'
    let turnError: string | undefined
    let lastStep: StepUsage | undefined
    let turnWokeUp = false
    let turnWakeupUsage: StepUsage | undefined
    if (status === 'completed') {
      session.state.inSettle = true
      const settled = await settleBrainTurn(
        session.brainRoot,
        session.state.nativeId,
        session.id,
        session.emitItem,
        {
          signal: turnAbort.signal,
          onSubagents: session.rememberSubagents,
          minStepIndex,
          acpToolCallIds: session.state.turnToolCallIds,
          emitForeground: suppressedForeground,
          shouldEmitPreWakeupTool: (item) =>
            session.timeline.isRunning(item.id) ||
            (item.detail.type === 'sub_agent' &&
              item.detail.childSessionId !== undefined &&
              session.canLinkChild(
                item.detail.childSessionId,
                session.state.nativeId,
                item.name,
              ) &&
              // Unknown children open, interrupted ones resume.
              (!session.hasChild(item.detail.childSessionId) ||
                session.isChildResumable(item.detail.childSessionId))),
          onStep: (stepIndex) => {
            if (stepIndex > session.state.lastKnownStepIndex)
              session.state.lastKnownStepIndex = stepIndex
          },
          // A model turn always writes USER_INPUT; give agy a moment to flush it. Native commands may write none.
          transcriptGraceMs:
            session.state.turnHadModelOutput || suppressedForeground ? 2000 : 0,
          stallTimeoutMs: session.stallTimeoutMs,
          isWaitingOnUser: () => session.permissions.size > 0,
          onQuestion: async (pendingQuestion) => {
            const handledIndex = session.state.handledElicitations.findIndex(
              (entry) =>
                entry.wokeUp === pendingQuestion.wokeUp &&
                sameNormalizedQuestions(
                  entry.questions,
                  pendingQuestion.questions,
                ),
            )
            if (handledIndex >= 0) {
              const [handled] = session.state.handledElicitations.splice(
                handledIndex,
                1,
              )
              return {
                status: pendingQuestion.wokeUp ? 'answered' : 'already_handled',
                answerText: handled.answerText ?? undefined,
              }
            }
            const pending = session.askUserQuestion(
              pendingQuestion.questions,
              pendingQuestion.questions[0]?.question,
              'brain',
            )
            if (!pending) return { status: 'canceled' }
            const response = await pending.promise
            if (!response || turnAbort.signal.aborted) {
              return { status: 'canceled' }
            }
            if (response.behavior === 'deny') {
              await session.interrupt()
              return { status: 'canceled' }
            }
            const answerText =
              formatQuestionAnswersText(pending.entry.questions, response) ||
              'Proceed'
            if (pending.entry.fromElicitation) {
              return { status: 'answered', answerText }
            }
            session.state.preStagedQuestion = {
              questions: pendingQuestion.questions,
              response,
            }
            session.state.suppressContinuationAcp = true
            try {
              const contResult = await session.transport.rpc.prompt({
                sessionId: session.state.nativeId,
                prompt: [{ type: 'text', text: answerText }],
              })
              await session.drain()
              if (contResult.stopReason === 'cancelled') {
                return { status: 'canceled' }
              }
            } finally {
              session.state.suppressContinuationAcp = false
              session.state.preStagedQuestion = undefined
            }
            return { status: 'answered', answerText }
          },
        },
      )
      await session.syncKilledChildren()
      status = settled.status
      turnError = settled.error
      lastStep = settled.lastStepUsage
      turnWokeUp = Boolean(settled.wokeUp)
      turnWakeupUsage = settled.wakeupUsage
      if (turnWokeUp) session.state.wakeup.pendingReplay = true
      if (
        settled.stalled &&
        !turnAbort.signal.aborted &&
        !session.state.closing &&
        !session.state.failed
      ) {
        await session.interrupt().catch(() => undefined)
      }
    }
    if (result.usage || turnWakeupUsage) {
      const carriedWakeup = result.usage
        ? session.state.wakeup.carriedUsage
        : undefined
      session.state.wakeup.carriedUsage = turnWakeupUsage
      session.publishUsage(
        promptUsage(result.usage, {
          model: configState(session.state.metadata).model,
          preserveContext: session.state.turnHadContextUsage && !turnWokeUp,
          lastStep,
          carriedWakeup,
          wakeup: turnWakeupUsage,
        }),
      )
    } else if (turnWokeUp) {
      session.state.wakeup.carriedUsage = undefined
    }
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
    session.state.wakeup.suppressEcho = false
    session.state.inSettle = false
    session.state.handledElicitations = []
    session.state.suppressContinuationAcp = false
    session.state.preStagedQuestion = undefined
    session.state.turnAbort = null
    session.state.turn = ''
    session.timeline.boundary()
    session.cancelPermissions()
  }
}
