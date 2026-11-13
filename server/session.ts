import { BrainTurnObservation } from './brain-observer.ts'
import type { AgyUsageSource } from './usage.ts'
import { z } from 'zod'
import { randomUUID } from 'node:crypto'
import { stat } from 'node:fs/promises'
import { join } from 'node:path'
import {
  PROTOCOL_VERSION,
  type RequestPermissionRequest,
  type RequestPermissionResponse,
  type SessionNotification,
  type ToolCall,
  type NewSessionResponse,
  type ContentBlock,
  type McpServer,
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
  isPreapprovedMcpTool,
  permissionKind,
  pickPermissionOption,
} from './permissions.ts'
import {
  buildElicitationResponse,
  formatQuestionAnswersText,
  normalizeElicitationQuestions,
  sameNormalizedQuestions,
  toQuestionPermissionInput,
  type NormalizedQuestion,
} from './questions.ts'
import {
  hasActiveBackgroundWork,
  readInboxState,
  readSubagents,
  readTranscriptRows,
  readLastStepIndex,
  settleBrainTurn,
  type ForegroundAcpSegment,
} from './brain.ts'
import { readLatestStepUsage } from './brain-io.ts'
import { projectReplayHistory, type HistoryEntry } from './history.ts'
import { SubagentRegistry, isSubagentMessage } from './subagents.ts'
import { dataDirectory, removeSessionArtifacts } from './preflight.ts'
import { connectAcp } from './transport.ts'
import {
  PACKAGE_VERSION,
  definedProcessEnv,
  formatErrorMessage,
  record,
  sessionIdPattern,
  setBounded,
} from './common.ts'
import {
  configState,
  addStepUsage,
  FULL_ACCESS_MODE_ID,
  Timeline,
  toolDetail,
  promptContent,
  contextUsage,
  promptUsage,
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
  retryTimeoutMs?: number
}

const SKIP_PERMISSIONS_FLAG = '--dangerously-skip-permissions'
const TIER_CACHE_LIMIT = 100

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
        env: launch?.env ?? definedProcessEnv(),
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
  private opening = new Map<string, Promise<Session>>()
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
    const starting = this.start(input.sessionId, input.config.cwd, input.config)
    this.opening.set(input.sessionId, starting)
    try {
      const session = await starting
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
          setBounded(
            this.options.usage.tiers,
            dir,
            session.transport.tier.name,
            TIER_CACHE_LIMIT,
          )
        await session.drain()
        if (this.closed || session.state.closing || session.state.failed)
          throw new Error('Session closed before open')
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
        if (input.persistence) {
          if (session.state.replay) {
            await session.replayTools()
          } else {
            session.state.lastKnownStepIndex = await readLastStepIndex(
              session.brainRoot,
              session.state.nativeId,
            ).catch(() => -1)
          }
        }
        session.state.turnToolCallIds = []
        session.state.turnForegroundAcp = []
        await configureSession(session, input.config, { strictThinking: false })
        await session.drain()
        if (input.persistence) await session.restoreUsage()
        session.state.loading = false
        session.state.replay = true
        session.flushDeferredUsage()
        await session.waitForCommands()
        if (this.closed || session.state.closing || session.state.failed)
          throw new Error('Session closed before ready')
        session.state.ready = true
        emit({
          type: 'session.ready',
          sessionId: input.sessionId,
          requestId: input.requestId,
        })
      } catch (error) {
        const wasOpened = session.state.opened
        const closedExternally = session.state.closedEmitted
        this.sessions.delete(input.sessionId)
        await session.close()
        if (wasOpened && !closedExternally && !session.state.closedEmitted) {
          session.state.closedEmitted = true
          emit({ type: 'session.closed', sessionId: input.sessionId })
        }
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
    if (input.type === 'session.close') {
      const session =
        this.sessions.get(input.sessionId) ??
        (await this.opening.get(input.sessionId)?.catch(() => undefined))
      if (!session) throw new Error('Session is not open')
      const wasOpened = session.state.opened
      await session.close()
      this.sessions.delete(session.id)
      if (wasOpened && !session.state.closedEmitted) {
        session.state.closedEmitted = true
        emit({ type: 'session.closed', sessionId: session.id })
      }
      emit({ type: 'request.completed', requestId: input.requestId })
      return
    }
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
        if (!option || option.kind.startsWith('reject')) {
          session.state.deniedToolCallIds.add(
            permission.request.toolCall.toolCallId,
          )
        }
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
    } else throw new Error(`Unsupported operation: ${input.type}`)
    emit({ type: 'request.completed', requestId: input.requestId })
  }

  async close() {
    this.closed = true
    const pendingOpens = await Promise.all(
      [...this.opening.values()].map((p) => p.catch(() => undefined)),
    )
    for (const s of pendingOpens) {
      if (s) this.sessions.set(s.id, s)
    }
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
  const cwdStat = await stat(cwd).catch(() => null)
  if (!cwdStat?.isDirectory()) {
    throw new Error(`Working directory does not exist: ${cwd}`)
  }
  const { launch, capabilities, emit } = options
  const permissions = new Map<string, PendingPermission>()
  const timeline = new Timeline(id)
  const brainRoot = join(dataDirectory(launch, config?.env, cwd), 'brain')
  const state = {
    nativeId: '',
    opened: false,
    persisted: false,
    closing: false,
    silenced: false,
    closedEmitted: false,
    skipPermissions: config?.mode === FULL_ACCESS_MODE_ID,
    turnHadContextUsage: false,
    turnHadModelOutput: false,
    inSettle: false,
    handledElicitations: [] as Array<{
      questions: NormalizedQuestion[]
      answerText: string | null
      wokeUp: boolean
      declined?: boolean
    }>,
    suppressContinuationAcp: false,
    continuationEchoSeen: false,
    preStagedQuestion: undefined as
      | {
          questions: NormalizedQuestion[]
          response: PermissionResponseInput
        }
      | undefined,
    turnToolCallIds: [] as string[],
    turnForegroundAcp: [] as ForegroundAcpSegment[],
    deniedToolCallIds: new Set<string>(),
    lastKnownStepIndex: -1,
    wakeup: {
      pendingReplay: false,
      suppressEcho: false,
      carriedUsage: undefined as StepUsage | undefined,
    },
    opening: true,
    loading: true,
    ready: false,
    replay: true,
    turn: '',
    turnAbort: null as AbortController | null,
    observation: null as BrainTurnObservation | null,
    turnFinished: false,
    interrupting: null as Promise<void> | null,
    failed: false,
    metadata: { sessionId: '' } as NewSessionResponse & SessionMetadata,
    prompts: new Set<string>(),
  }
  const buffered: SessionNotification[] = []
  let collectingHistory = true
  const acpHistory: HistoryEntry[] = []
  const acpToolResults = new Map<
    string,
    { rawOutput: boolean; content: boolean }
  >()
  const acpToolSnapshots = new Map<string, Readonly<ToolCall>>()
  function acpResultProvided(callId: string): boolean {
    const result = acpToolResults.get(callId)
    return result?.rawOutput === true || result?.content === true
  }
  const subagents = new SubagentRegistry({
    sessionId: id,
    cwd,
    brainRoot,
    capabilities,
    emit,
    getNativeId: () => state.nativeId,
    isParentTurnActive: () => Boolean(state.turn),
    isLoading: () => state.loading,
    isUnavailable: () => state.closing || state.failed,
    retryTimeoutMs: options.retryTimeoutMs,
  })
  let notificationLane = Promise.resolve()
  let relaunching = Promise.resolve()
  const nativeWindowSizes = new Map<string | undefined, number>()
  let usage: ProviderUsage = {}
  let deferredUsage = false
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
    if (!state.opened || state.silenced) return
    usage = { ...usage, ...next }
    deferredUsage = false
    emit({
      type: 'session.usage',
      sessionId: id,
      turnId: state.turn || undefined,
      usage,
    })
  }
  function flushDeferredUsage() {
    if (!deferredUsage || !state.opened || state.silenced) return
    deferredUsage = false
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
    if (state.silenced) return
    for (const item of timeline.terminalize(
      status,
      error,
      state.deniedToolCallIds,
    )) {
      emit({ type: 'timeline.item', sessionId: id, item })
    }
  }
  function finishTurnOnce(
    turnId: string,
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
  ) {
    if (state.turn !== turnId || state.turnFinished || state.silenced) return
    state.turnFinished = true
    terminalize(status, error)
    emit({
      type: 'session.turn',
      sessionId: id,
      turnId,
      state: status,
      ...(status === 'failed' && error ? { error: { message: error } } : {}),
    })
  }
  /** Brain-projected items are not yet known to the ACP timeline. */
  function emitItem(item: ProviderTimelineItem, timestamp?: string) {
    if (state.silenced) return
    timeline.track(item)
    publishItem(item, timestamp)
  }
  /** Link child sessions and publish an item the timeline already tracks. */
  function publishItem(item: ProviderTimelineItem, timestamp?: string) {
    if (state.silenced) return
    subagents.linkSubsession(id, state.nativeId, item)
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
      if (state.opened && !state.silenced && serialized !== emittedCommands) {
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
      if (!next) return
      setBounded(
        nativeWindowSizes,
        configState(state.metadata).model,
        u.size,
        100,
      )
      if (!state.loading || state.replay) {
        if (state.turn) state.turnHadContextUsage = true
        publishUsage(next)
      } else if (!state.ready) {
        usage = { ...usage, ...next }
        deferredUsage = true
      }
    } else {
      if (state.loading && !state.replay) return
      if (state.suppressContinuationAcp) {
        if (u.sessionUpdate === 'user_message_chunk')
          state.continuationEchoSeen = true
        if (
          u.sessionUpdate === 'tool_call' &&
          state.continuationEchoSeen &&
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
        state.turnToolCallIds = []
        state.turnForegroundAcp = []
        timeline.boundary()
        return
      }
      if (state.wakeup.suppressEcho) return
      const isNewToolCall =
        u.sessionUpdate === 'tool_call' &&
        !state.turnToolCallIds.includes(u.toolCallId)
      if (isNewToolCall) state.turnToolCallIds.push(u.toolCallId)
      if (!state.loading && state.turn && !state.inSettle) {
        if (
          (u.sessionUpdate === 'agent_message_chunk' ||
            u.sessionUpdate === 'agent_thought_chunk') &&
          u.content.type === 'text' &&
          u.content.text.length > 0
        ) {
          const segType =
            u.sessionUpdate === 'agent_message_chunk'
              ? 'assistant_message'
              : 'reasoning'
          const last =
            state.turnForegroundAcp[state.turnForegroundAcp.length - 1]
          if (last && last.type === segType) last.text += u.content.text
          else
            state.turnForegroundAcp.push({
              type: segType,
              text: u.content.text,
            })
        } else if (isNewToolCall) {
          state.turnForegroundAcp.push({
            type: 'tool_call',
            callId: u.toolCallId,
          })
        }
      }
      if (
        state.turn &&
        (u.sessionUpdate === 'agent_message_chunk' ||
          u.sessionUpdate === 'agent_thought_chunk' ||
          u.sessionUpdate === 'tool_call')
      )
        state.turnHadModelOutput = true
      // Timeline.update already tracks the ACP item.
      const item = timeline.update(u, state.deniedToolCallIds)
      if (item) {
        if (
          item.type === 'tool_call' &&
          item.detail.type === 'sub_agent' &&
          isSubagentMessage(item.name)
        ) {
          // Terminalization may run while ownership is being read.
          const nativeId = item.detail.childSessionId
          delete item.detail.childSessionId
          await subagents.refreshSubagents()
          // Do not restore a native ID that a later terminalization could publish.
          if (state.closing || state.failed) return
          if (nativeId !== undefined) item.detail.childSessionId = nativeId
        }
        if (
          state.loading &&
          state.persisted &&
          state.replay &&
          collectingHistory
        ) {
          if (
            item.type === 'tool_call' &&
            (u.sessionUpdate === 'tool_call' ||
              u.sessionUpdate === 'tool_call_update')
          ) {
            const result =
              u.sessionUpdate === 'tool_call'
                ? { rawOutput: false, content: false }
                : (acpToolResults.get(item.callId) ?? {
                    rawOutput: false,
                    content: false,
                  })
            if (u.rawOutput != null) result.rawOutput = true
            if (u.content != null)
              result.content = item.status !== 'running' || u.content.length > 0
            acpToolResults.set(item.callId, result)
            const snapshot = timeline.toolCallSnapshot(item.callId)
            if (snapshot) acpToolSnapshots.set(item.callId, snapshot)
            acpHistory.push({
              item,
              resultProvided: acpResultProvided(item.callId),
            })
          } else acpHistory.push({ item })
        } else publishItem(item)
      }
    }
  }
  function publishConfig() {
    if (!state.opened || state.closing || state.failed || state.silenced) return
    emit({
      type: 'session.config',
      sessionId: id,
      config: configState(state.metadata),
    })
  }
  function cancelPermissions() {
    for (const [permissionId, p] of permissions) {
      if (p.type === 'acp') {
        state.deniedToolCallIds.add(p.request.toolCall.toolCallId)
        p.resolve({ outcome: { outcome: 'cancelled' } })
      } else {
        p.resolve(null)
      }
      if (state.opened && !state.silenced) {
        emit({
          type: 'session.permission_resolved',
          sessionId: id,
          permissionId,
        })
      }
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
          declined: response.behavior === 'deny',
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
        request.toolCall,
        config?.toolPolicy,
      )
      if (kind !== 'question' && (state.metadata.fullAccess || preapproved)) {
        const allow = pickPermissionOption(request.options, 'allow')
        if (!allow) state.deniedToolCallIds.add(request.toolCall.toolCallId)
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
  const retiring = new WeakSet<ReturnType<typeof connectAcp>>()
  const onExit = (error: Error) => {
    if (state.failed || state.closing) return
    state.failed = true
    state.turnAbort?.abort(error)
    resolveCommands()
    cancelPermissions()
    terminalize('failed', error.message)
    if (state.ready && !state.silenced)
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
      (error) => {
        if (transport !== currentTransport || retiring.has(transport)) return
        onExit(error)
      },
    )
    transports.add(transport)
    void transport.stopped.then(() => transports.delete(transport))
    return transport
  }
  function initializeRuntime(transport: ReturnType<typeof connectAcp>) {
    return transport.request(
      transport.rpc.initialize({
        protocolVersion: PROTOCOL_VERSION,
        clientInfo: { name: 'paseo-agy-acp', version: PACKAGE_VERSION },
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
    async function doRelaunch(
      skipPermissions: boolean,
      force = false,
    ): Promise<void> {
      if (state.closing) return
      if (state.failed)
        throw new Error('ACP runtime has exited; reopen the session')
      if (
        !force &&
        (state.skipPermissions === skipPermissions ||
          !currentInitialized.agentCapabilities?.loadSession)
      )
        return
      const prev = configState(state.metadata)
      const prevModels = state.metadata.models
      const nativeMode = configState({
        ...state.metadata,
        fullAccess: false,
      }).mode
      state.observation?.stop()
      state.loading = true
      state.replay = false
      retiring.add(currentTransport)
      try {
        await currentTransport.close()
        if (state.closing) return
        if (state.failed)
          throw new Error('ACP runtime has exited; reopen the session')
        if (!currentInitialized.agentCapabilities?.loadSession)
          throw new Error(
            'agy cannot reload after interruption; reopen the session',
          )
        const next = startRuntime(skipPermissions)
        currentTransport = next
        currentInitialized = await initializeRuntime(next)
        if (state.closing) return
        if (state.failed || next.closed)
          throw new Error('ACP runtime exited during relaunch')
        state.skipPermissions = skipPermissions
        state.metadata = {
          ...(await next.request(
            next.rpc.loadSession({
              ...sessionParams(cwd, config),
              sessionId: state.nativeId,
            }),
          )),
          sessionId: state.nativeId,
          models: next.models ?? prevModels,
          fullAccess: skipPermissions,
        }
        await drain()
        if (state.closing) return
        if (state.failed || next.closed)
          throw new Error('ACP runtime exited during relaunch')
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
        await drain()
        if (state.closing) return
        if (state.failed || next.closed)
          throw new Error('ACP runtime exited during relaunch')
      } catch (error) {
        if (state.closing) return
        onExit(error instanceof Error ? error : new Error(String(error)))
        void currentTransport.close().catch(() => undefined)
        throw error
      } finally {
        state.wakeup = {
          pendingReplay: false,
          suppressEcho: false,
          carriedUsage: undefined,
        }
        state.turnToolCallIds = []
        state.turnForegroundAcp = []
        state.loading = false
        state.replay = true
      }
    }
    return {
      id,
      cwd,
      config,
      brainRoot,
      stallTimeoutMs: options.stallTimeoutMs,
      retryTimeoutMs: options.retryTimeoutMs,
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
      flushDeferredUsage,
      terminalize,
      finishTurnOnce,
      emitItem,
      resetWakeup() {
        state.wakeup = {
          pendingReplay: false,
          suppressEcho: false,
          carriedUsage: undefined,
        }
      },
      hasChild: (nativeId: string) => subagents.hasChild(nativeId),
      isChildResumable: (nativeId: string) =>
        subagents.isChildResumable(nativeId),
      syncKilledChildren: subagents.syncKilledChildren,
      rememberSubagents: subagents.rememberSubagents,
      refreshSubagents: subagents.refreshSubagents,
      canLinkChild: subagents.canLinkChild,
      drain,
      waitForCommands,
      get contextWindowMaxTokens() {
        return nativeWindowSizes.get(configState(state.metadata).model)
      },
      publishStepContext(lastStep: StepUsage) {
        const { contextWindowUsedTokens, contextWindowMaxTokens } = promptUsage(
          null,
          {
            model: configState(state.metadata).model,
            contextWindowMaxTokens: nativeWindowSizes.get(
              configState(state.metadata).model,
            ),
            lastStep,
          },
        )
        if (contextWindowUsedTokens !== undefined)
          publishUsage({ contextWindowUsedTokens, contextWindowMaxTokens })
      },
      async restoreUsage() {
        if (usage?.contextWindowUsedTokens !== undefined) return
        const snapshot = await readLatestStepUsage(
          brainRoot,
          state.nativeId,
        ).catch(() => undefined)
        if (snapshot?.lastStepUsage)
          this.publishStepContext(snapshot.lastStepUsage)
      },
      async replayTools() {
        if (!state.replay) return
        const [inbox, subagentRecords, rows] = await Promise.all([
          readInboxState(brainRoot, state.nativeId),
          readSubagents(brainRoot, state.nativeId),
          readTranscriptRows(brainRoot, state.nativeId),
        ])
        subagents.rememberSubagents(subagentRecords)
        // Drain ACP session/load notifications first so turnToolCallIds and acpToolSnapshots are complete;
        // projectReplayHistory() below is purely synchronous and never triggers ACP notifications.
        await drain()
        collectingHistory = false
        const { entries, lastKnownStepIndex } = projectReplayHistory({
          sessionId: id,
          rows,
          inbox,
          subagents: subagentRecords,
          acpHistory: acpHistory.splice(0),
          acpToolCallIds: state.turnToolCallIds,
          acpToolSnapshots,
          acpResultProvided,
        })
        if (lastKnownStepIndex > state.lastKnownStepIndex)
          state.lastKnownStepIndex = lastKnownStepIndex
        for (const { item, timestamp } of entries) {
          await subagents.waitCurrentReady()
          emitItem(item, timestamp)
        }
        acpToolResults.clear()
        acpToolSnapshots.clear()
        // A child opened by the last replayed tool must read its history before a historical kill cancels it.
        await subagents.waitNestedReady()
        await subagents.syncKilledChildren(subagentRecords)
      },
      async canStop(): Promise<boolean> {
        if (
          state.closing ||
          state.opening ||
          state.loading ||
          state.turn ||
          state.interrupting ||
          permissions.size > 0 ||
          subagents.hasRunningChildren()
        )
          return false
        return !(await hasActiveBackgroundWork(brainRoot, state.nativeId, {
          includeDaemons: true,
          retryTimeoutMs: options.retryTimeoutMs,
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
        if (state.closing) return
        if (state.failed)
          throw new Error('ACP runtime has exited; reopen the session')
        state.turnAbort?.abort()
        cancelPermissions()
        // Children stay registered when canceled so they are neither reopened nor left unclosed.
        const canceling = subagents.cancelRunning()
        // Native session/cancel leaves background tasks alive; stop their owner when background work is active.
        state.interrupting = (async () => {
          await Promise.all(canceling)
          let needsRelaunch =
            canceling.length > 0 ||
            (await hasActiveBackgroundWork(brainRoot, state.nativeId, {
              retryTimeoutMs: options.retryTimeoutMs,
            }).catch(() => true))
          if (state.closing) return
          if (state.failed)
            throw new Error('ACP runtime has exited; reopen the session')
          try {
            if (needsRelaunch) {
              retiring.add(currentTransport)
              await Promise.all(subagents.cancelAll())
            }
            await currentTransport.request(
              currentTransport.rpc.cancel({ sessionId: state.nativeId }),
              { timeoutMs: 5000, fatalOnTimeout: false },
            )
          } catch (error) {
            // A concurrent close or forced relaunch replaces this runtime; if loadSession is supported, recover via relaunch.
            if (
              !needsRelaunch &&
              !state.closing &&
              !state.failed &&
              currentInitialized.agentCapabilities?.loadSession
            ) {
              needsRelaunch = true
              retiring.add(currentTransport)
            } else if (!state.closing && (!needsRelaunch || state.failed)) {
              throw error
            }
          } finally {
            if (needsRelaunch && !state.closing && !state.failed)
              await this.relaunch(state.skipPermissions, true)
          }
        })()
        try {
          await state.interrupting
        } finally {
          state.interrupting = null
        }
      },
      relaunch(skipPermissions: boolean, force = false): Promise<void> {
        const run = relaunching.then(
          () => doRelaunch(skipPermissions, force),
          () => doRelaunch(skipPermissions, force),
        )
        relaunching = run.then(
          () => undefined,
          () => undefined,
        )
        return run
      },
      close(): Promise<void> {
        if (closing) return closing
        state.closing = true
        const closingObservation = state.observation
        closingObservation?.stop()
        const closingTurnId = state.turn
        state.turnAbort?.abort()
        resolveCommands()
        closing = (async () => {
          cancelPermissions()
          subagents.closeAll()
          if (closingTurnId) finishTurnOnce(closingTurnId, 'canceled')
          else terminalize('canceled')
          state.silenced = true
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
                { timeoutMs: 1500, fatalOnTimeout: false },
              )
              .catch(() => undefined)
          const dir = dataDirectory(launch, config?.env, cwd)
          await closingObservation?.dispose()
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

type NativeConfigKey = 'model' | 'thought_level' | 'mode'

function selectedConfigValue(
  current: ReturnType<typeof configState>,
  key: NativeConfigKey,
): string | undefined {
  return key === 'model'
    ? current.model
    : key === 'mode'
      ? current.mode
      : current.thinkingOption
}

function isAdvertisedConfigOption(
  metadata: SessionMetadata,
  current: ReturnType<typeof configState>,
  key: NativeConfigKey,
): boolean {
  return (
    Boolean(
      metadata.configOptions?.some((o) => o.id === key || o.category === key),
    ) &&
    (key !== 'thought_level' || current.thinkingOptions.length > 0)
  )
}

async function applyNativeConfigOption(
  transport: ReturnType<typeof connectAcp>,
  nativeId: string,
  metadata: NewSessionResponse & SessionMetadata,
  key: NativeConfigKey,
  value: string | undefined,
  strict = false,
): Promise<boolean> {
  if (!value) return false
  const option = metadata.configOptions?.find(
    (o) => o.id === key || o.category === key,
  )
  if (!option) {
    if (strict) throw new Error(`agy does not advertise ${key}`)
    return false
  }
  if (option.currentValue === value) return false
  const result = await transport.request(
    transport.rpc.setSessionConfigOption({
      sessionId: nativeId,
      configId: option.id,
      value,
    }),
  )
  metadata.configOptions = result.configOptions
  return true
}

async function configureSession(
  session: Session,
  changes: ProviderConfigChanges,
  options?: { strictThinking?: boolean },
) {
  if (session.state.closing || session.state.failed)
    throw new Error('ACP runtime has exited; reopen the session')
  if (session.state.turn)
    throw new Error('Wait for the current turn before changing configuration')
  if (changes.settings && Object.keys(changes.settings).length)
    throw new Error('Custom settings are not supported')
  const strictThinking = options?.strictThinking ?? true
  // Switching `model` dynamically adds or removes `thought_level` in `metadata.configOptions`
  // via the native RPC, so `thought_level` must be re-evaluated after `model` is applied.
  const allowMissingThinking = !strictThinking || changes.model != null
  const entries = [
    ['model', changes.model],
    ['thought_level', changes.thinkingOption],
    ['mode', changes.mode],
  ] as const
  const initial = configState(session.state.metadata)
  for (const [key, value] of entries) {
    if (value == null) continue
    if (key === 'mode' && value === FULL_ACCESS_MODE_ID) continue
    if (key === 'thought_level' && allowMissingThinking) continue
    if (value === selectedConfigValue(initial, key)) continue
    if (!isAdvertisedConfigOption(session.state.metadata, initial, key))
      throw new Error(`agy does not advertise ${key}`)
  }
  let mutated = false
  try {
    for (const [key, value] of entries) {
      if (value == null) continue
      const current = configState(session.state.metadata)
      if (value === selectedConfigValue(current, key)) continue
      if (!isAdvertisedConfigOption(session.state.metadata, current, key)) {
        if (key === 'thought_level' && allowMissingThinking) continue
        throw new Error(`agy does not advertise ${key}`)
      }
      if (
        key === 'thought_level' &&
        !strictThinking &&
        !current.thinkingOptions.some((o) => o.id === value)
      )
        continue
      if (key === 'mode' && value === FULL_ACCESS_MODE_ID) {
        await session.relaunch(true)
        session.state.metadata.fullAccess = true
        mutated = true
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
        mutated = true
      }
      const applied = await applyNativeConfigOption(
        session.transport,
        session.state.nativeId,
        session.state.metadata,
        key,
        value,
        true,
      )
      mutated ||= applied
    }
  } catch (error) {
    if (mutated) session.publishConfig()
    throw error
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
  session.state.turnFinished = false
  session.state.turnHadContextUsage = false
  session.state.turnHadModelOutput = false
  session.state.inSettle = false
  session.state.handledElicitations = []
  session.state.suppressContinuationAcp = false
  session.state.preStagedQuestion = undefined
  session.state.turnToolCallIds = []
  session.state.turnForegroundAcp = []
  session.state.deniedToolCallIds.clear()
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
  const nativeId = session.state.nativeId
  let minStepIndex: number | undefined
  let observation: BrainTurnObservation | undefined
  let observationTransport = session.transport
  let continuationActive = false
  const isCurrent = () =>
    session.state.turn === turnId &&
    session.state.turnAbort === turnAbort &&
    !session.state.closing &&
    !session.state.silenced &&
    session.state.nativeId === nativeId &&
    session.transport === observationTransport
  const stopObservation = () => observation?.stop()
  turnAbort.signal.addEventListener('abort', stopObservation, { once: true })
  const waitPrompt = async (
    operation: ReturnType<typeof session.transport.rpc.prompt>,
  ) => {
    const transport = session.transport
    const outcome = operation.then(
      (value) => ({ ok: true as const, value }),
      (error) => ({ ok: false as const, error }),
    )
    let aborted!: () => void
    const cancellation = new Promise<null>((resolve) => {
      aborted = () => resolve(null)
    })
    turnAbort.signal.addEventListener('abort', aborted, { once: true })
    try {
      if (turnAbort.signal.aborted) aborted()
      let result = await Promise.race([outcome, cancellation])
      if (!result) {
        if (session.state.closing) throw turnAbort.signal.reason
        await session.state.interrupting
        if (session.state.closing) throw turnAbort.signal.reason
        // Cancel is a notification. Bound convergence with the existing control-request policy, not a flush sleep.
        try {
          result = await transport.request(outcome, { fatalOnTimeout: false })
        } catch (error) {
          if (!session.state.closing && session.transport === transport)
            await session.relaunch(session.state.skipPermissions, true)
          throw error
        }
      }
      if (!result.ok) throw result.error
      return result.value
    } finally {
      turnAbort.signal.removeEventListener('abort', aborted)
    }
  }
  const finalDrain = async () => {
    observation?.stop()
    if (session.state.closing || session.state.silenced) return
    await session.state.interrupting
    await session.drain()
    if (
      session.state.turn !== turnId ||
      session.state.turnAbort !== turnAbort ||
      session.state.closing ||
      session.state.silenced ||
      session.state.nativeId !== nativeId
    )
      return
    if (session.transport !== observationTransport && !turnAbort.signal.aborted)
      return
    observationTransport = session.transport
    await observation?.drain()
  }
  let finalSnapshot: Promise<StepUsage | undefined> | undefined
  const reconcileFinalSnapshot = (fallback?: StepUsage) =>
    (finalSnapshot ??= (async () => {
      await finalDrain()
      if (!isCurrent() || minStepIndex === undefined) return
      const observed = observation?.usage.latest ?? fallback
      // A complete EOF can be newer than streamed rows; it is a measurement, not snapshot commit evidence.
      const snapshot = await readLatestStepUsage(
        session.brainRoot,
        nativeId,
        Math.max(minStepIndex, (observation?.usage.latestStepIndex ?? -1) - 1),
      ).catch(() => undefined)
      if (!isCurrent()) return
      if (snapshot)
        session.state.lastKnownStepIndex = Math.max(
          session.state.lastKnownStepIndex,
          snapshot.lastStepIndex,
        )
      return snapshot?.lastStepUsage ?? observed
    })())
  try {
    const readStep = await readLastStepIndex(
      session.brainRoot,
      session.state.nativeId,
    ).catch(() => -1)
    minStepIndex = Math.max(session.state.lastKnownStepIndex, readStep)
    session.state.lastKnownStepIndex = minStepIndex
    turnAbort.signal.throwIfAborted()
    observation = new BrainTurnObservation(
      session.brainRoot,
      nativeId,
      session.id,
      minStepIndex,
      {
        isCurrent,
        onUsage: (usage) => {
          if (
            session.state.turnHadContextUsage &&
            !observation?.wokeUp &&
            observation?.phase !== 'continuation'
          )
            return false
          session.publishStepContext(usage)
        },
        onItem: session.emitItem,
        onBoundary: (index, reconciled) => {
          if (!isCurrent() || index < 0) return
          session.state.lastKnownStepIndex = reconciled
            ? index
            : Math.max(session.state.lastKnownStepIndex, index)
        },
      },
    )
    session.state.observation = observation
    observation.start()
    const result = await waitPrompt(
      session.transport.rpc.prompt({
        sessionId: session.state.nativeId,
        prompt: blocks,
      }),
    )
    await session.drain()
    if (!isCurrent() && !turnAbort.signal.aborted)
      throw new Error('ACP runtime replaced during turn')
    const suppressedForeground = session.state.wakeup.suppressEcho
    session.state.wakeup.suppressEcho = false
    let status: 'completed' | 'canceled' | 'failed' =
      result.stopReason === 'cancelled' ? 'canceled' : 'completed'
    let turnError: string | undefined
    let lastStep: StepUsage | undefined
    let turnWokeUp = false
    let turnWakeupUsage: StepUsage | undefined
    let nextCarriedWakeup: StepUsage | undefined
    if (status === 'completed') {
      session.state.inSettle = true
      const settled = await settleBrainTurn(
        session.brainRoot,
        session.state.nativeId,
        session.id,
        session.emitItem,
        {
          signal: turnAbort.signal,
          observation,
          isRpcActive: () => continuationActive,
          onSubagents: session.rememberSubagents,
          minStepIndex,
          acpToolCallIds: session.state.turnToolCallIds,
          foregroundAcp: suppressedForeground
            ? []
            : [...session.state.turnForegroundAcp],
          deniedToolCallIds: session.state.deniedToolCallIds,
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
          retryTimeoutMs: session.retryTimeoutMs,
          isWaitingOnUser: () => session.permissions.size > 0,
          onQuestionSettled: (questions, wokeUp) => {
            const idx = session.state.handledElicitations.findIndex(
              (entry) =>
                entry.wokeUp === wokeUp &&
                sameNormalizedQuestions(entry.questions, questions),
            )
            if (idx >= 0) session.state.handledElicitations.splice(idx, 1)
          },
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
              if (pendingQuestion.wokeUp && handled.declined) {
                await session.interrupt()
                return { status: 'canceled' }
              }
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
            if (observation?.error) throw observation.error
            if (!isCurrent())
              throw new Error('ACP runtime replaced during question')
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
            pendingQuestion.beginContinuation?.(answerText)
            session.state.suppressContinuationAcp = true
            session.state.continuationEchoSeen = false
            continuationActive = true
            if (observation) {
              observation.phase = 'continuation'
              observation.wokeUp = true
            }
            try {
              const contResult = await waitPrompt(
                session.transport.rpc.prompt({
                  sessionId: session.state.nativeId,
                  prompt: [{ type: 'text', text: answerText }],
                }),
              )
              await session.drain()
              if (contResult.stopReason === 'cancelled') {
                return { status: 'canceled' }
              }
            } finally {
              continuationActive = false
              if (observation?.acceptsRows()) observation.phase = 'settling'
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
      nextCarriedWakeup = settled.carriedWakeupUsage
      if (settled.pendingReplay) session.state.wakeup.pendingReplay = true
      if (
        settled.stalled &&
        !turnAbort.signal.aborted &&
        !session.state.closing &&
        !session.state.failed
      ) {
        await session.interrupt().catch(() => undefined)
      }
    }
    lastStep = await reconcileFinalSnapshot(lastStep)
    turnWokeUp ||= observation?.wokeUp === true
    if (turnAbort.signal.aborted) {
      if (
        turnAbort.signal.reason instanceof Error &&
        turnAbort.signal.reason.name === 'AbortError'
      ) {
        // Stall failures cancel the runtime for cleanup without changing their terminal status.
        if (status !== 'failed') status = 'canceled'
      } else turnAbort.signal.throwIfAborted()
    }
    const preserveContext = session.state.turnHadContextUsage && !turnWokeUp
    if (result.usage || turnWakeupUsage) {
      const carriedWakeup = result.usage
        ? session.state.wakeup.carriedUsage
        : undefined
      session.state.wakeup.carriedUsage = result.usage
        ? nextCarriedWakeup
        : nextCarriedWakeup
          ? addStepUsage(session.state.wakeup.carriedUsage, nextCarriedWakeup)
          : session.state.wakeup.carriedUsage
      session.publishUsage(
        promptUsage(result.usage, {
          model: configState(session.state.metadata).model,
          contextWindowMaxTokens: session.contextWindowMaxTokens,
          preserveContext,
          lastStep,
          carriedWakeup,
          wakeup: turnWakeupUsage,
        }),
      )
    } else if (lastStep && !preserveContext) {
      session.publishStepContext(lastStep)
    }
    session.finishTurnOnce(turnId, status, turnError)
  } catch (error) {
    const restartError = session.state.closing
      ? undefined
      : await session.state.interrupting?.then(
          () => undefined,
          (err: unknown) => err,
        )
    const lastStep = await reconcileFinalSnapshot().catch(
      () => observation?.usage.latest,
    )
    if (
      lastStep &&
      (!session.state.turnHadContextUsage || observation?.wokeUp) &&
      isCurrent()
    )
      session.publishStepContext(lastStep)
    const turnError = restartError ?? error
    const status =
      !restartError &&
      turnAbort.signal.aborted &&
      turnAbort.signal.reason instanceof Error &&
      turnAbort.signal.reason.name === 'AbortError'
        ? 'canceled'
        : 'failed'
    const message = formatErrorMessage(turnError)
    session.finishTurnOnce(turnId, status, message)
  } finally {
    turnAbort.signal.removeEventListener('abort', stopObservation)
    await observation?.dispose()
    if (
      session.state.turn === turnId &&
      session.state.turnAbort === turnAbort
    ) {
      session.state.observation = null
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
}
