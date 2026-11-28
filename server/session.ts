import { BrainTurnObservation } from './brain-observer.ts'
import type { AgyUsageSource } from './usage.ts'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import {
  type RequestPermissionRequest,
  type RequestPermissionResponse,
  type SessionNotification,
  type ToolCall,
  type NewSessionResponse,
  type ContentBlock,
  type McpServer,
  type Usage,
} from '@agentclientprotocol/sdk/unstable'
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
  extractSecurityWarning,
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
  BackgroundWorkProbe,
  captureRuntimeEpoch,
  computeRuntimeEpoch,
  hasActiveBackgroundWork,
  settleBrainTurn,
  type ForegroundAcpSegment,
  type RuntimeEpoch,
} from './brain.ts'
import {
  readInboxState,
  readLastStepIndex,
  readLatestStepUsage,
  readSubagents,
  readTranscriptRows,
  type TranscriptReader,
} from './brain-io.ts'
import {
  captureConversationStepBaseline,
  readConversationCompactionWindow,
  readConversationStepError,
} from './conversation-db.ts'
import { toolDisplayName } from './mcp.ts'
import { projectReplayHistory, type HistoryEntry } from './history.ts'
import { SubagentRegistry } from './subagents.ts'
import {
  binaryFingerprint,
  dataDirectory,
  removeSessionArtifacts,
  resolveSpawnCwd,
} from './preflight.ts'
import {
  SKIP_PERMISSIONS_FLAG,
  TransportTracker,
  abortError,
  closeDisposableSession,
  createAttachableTransport,
  hasSkipPermissionsFlag,
  initializeAcp,
  isLaunchCwdIndependent,
  raceAbort,
  standbyPoolKey,
  type AttachableRuntime,
  type ControlTransport,
  type InitializedAcp,
  type StandbyRuntimePool,
} from './runtime.ts'
import { connectAcp, type AcpClient } from './transport.ts'
import {
  definedProcessEnv,
  formatErrorMessage,
  record,
  sessionIdPattern,
  setBounded,
} from './common.ts'
import {
  configState,
  currentModelId,
  currentNativeMode,
  FULL_ACCESS_MODE_ID,
  hasPositivePromptUsage,
  stripToolSummary,
  Timeline,
  toolDetail,
  promptContent,
  contextUsage,
  promptUsage,
  type SessionMetadata,
  type StepUsage,
} from './mapping.ts'

const childToolCallIdPattern =
  /^([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}):/i

/** Whether a toolCallId is prefixed with a child/workflow conversation UUID distinct from root. */
function isChildToolCallId(
  toolCallId: string | undefined,
  rootNativeId: string,
): boolean {
  if (!toolCallId || !rootNativeId) return false
  const match = childToolCallIdPattern.exec(toolCallId)
  return match !== null && match[1].toLowerCase() !== rootNativeId.toLowerCase()
}

type SessionPhase =
  'buffering' | 'loading' | 'ready' | 'reloading' | 'closing' | 'silenced'

interface HandledElicitation {
  questions: NormalizedQuestion[]
  callId?: string
  answerText: string | null
  wokeUp: boolean
  declined: boolean
}

interface ActiveTurnState {
  readonly id: string
  readonly abort: AbortController
  running: boolean
  finished: boolean
  inSettle: boolean
  hadContextUsage: boolean
  hadModelOutput: boolean
  sawPostRpcAcp: boolean
  estimatedTokens: number | undefined
  handledElicitations: HandledElicitation[]
  wakeupQuestionDeclined: boolean
  readonly deniedToolCallIds: Set<string>
  readonly toolCallIds: Set<string>
  foregroundAcp: ForegroundAcpSegment[]
  activePrompt: {
    transport: ControlTransport
    settled: Promise<void>
  } | null
  observation: BrainTurnObservation | null
}

type Open = Extract<ProviderInput, { type: 'session.open' }>
type Prompt = Extract<ProviderInput, { type: 'session.prompt' }>
type PermissionResponseInput = Extract<
  ProviderInput,
  { type: 'session.permission' }
>['response']
type PendingPermission =
  | {
      type: 'acp'
      request: RequestPermissionRequest
      resolve: (r: RequestPermissionResponse) => void
    }
  | { type: 'question'; resolve: (r: PermissionResponseInput | null) => void }
type Session = Awaited<ReturnType<typeof createSession>>
interface SessionOptions {
  usage: AgyUsageSource
  launch?: ProviderLaunch
  capabilities: readonly string[]
  emit: (event: ProviderEvent) => void
  stallTimeoutMs?: number
  retryTimeoutMs?: number
  cancelConvergenceTimeoutMs?: number
  backgroundProbeTimeoutMs?: number
  observationPollIntervalMs?: number
  commandWaitTimeoutMs?: number
  connectAcp?: typeof connectAcp
  standby?: StandbyRuntimePool
  createObservation?: (
    ...args: ConstructorParameters<typeof BrainTurnObservation>
  ) => BrainTurnObservation
  createTranscriptReader?: () => TranscriptReader
  readSubagents?: typeof readSubagents
  hasActiveBackgroundWork?: typeof hasActiveBackgroundWork
}

const PROMPT_DEDUPE_LIMIT = 1000

function sessionRuntimeExitedError(failureError?: Error): Error {
  return new Error(
    failureError
      ? `ACP runtime has exited; reopen the session (${formatErrorMessage(failureError)})`
      : 'ACP runtime has exited; reopen the session',
  )
}

function isUserCancelAbort(signal: AbortSignal | undefined): boolean {
  return Boolean(
    signal?.aborted &&
    signal.reason instanceof Error &&
    signal.reason.name === 'AbortError',
  )
}

async function raceTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  onTimeout: () => T,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((resolve) => {
        timer = setTimeout(() => resolve(onTimeout()), timeoutMs)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

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

/** Error whose `request.failed` event was already emitted before cleanup closed the session. */
export class ReportedRequestError extends Error {
  override readonly cause: unknown
  constructor(cause: unknown) {
    super(formatErrorMessage(cause))
    this.name = 'ReportedRequestError'
    this.cause = cause
  }
}

/** Own native runtimes, session identities and their cancellation across a provider connection. */
export class SessionManager {
  private sessions = new Map<string, Session>()
  private opening = new Map<
    string,
    { promise: Promise<Session>; abort: AbortController }
  >()
  private transports = new TransportTracker()
  private closed = false
  private closeController = new AbortController()
  private options: SessionOptions
  constructor(options: SessionOptions) {
    this.options = options
  }

  async start(
    id: string,
    cwd: string,
    config?: ProviderSessionConfig,
    startOptions?: { allowMissingCwd?: boolean },
    signal?: AbortSignal,
  ) {
    if (this.closed) throw new Error('Provider connection is closed')
    return createSession(
      id,
      cwd,
      config,
      this.options,
      this.transports,
      startOptions,
      signal
        ? AbortSignal.any([this.closeController.signal, signal])
        : this.closeController.signal,
    )
  }

  private async open(input: Open) {
    const { emit, capabilities } = this.options
    if (this.sessions.has(input.sessionId) || this.opening.has(input.sessionId))
      throw new Error('Session already open')
    const openAbort = new AbortController()
    const starting = this.start(
      input.sessionId,
      input.config.cwd,
      input.config,
      {
        allowMissingCwd: Boolean(input.persistence),
      },
      openAbort.signal,
    )
    this.opening.set(input.sessionId, { promise: starting, abort: openAbort })
    try {
      const session = await starting
      if (this.closed || openAbort.signal.aborted || session.isClosing()) {
        await session.close().catch(() => undefined)
        throw new Error(
          this.closed
            ? 'Provider connection is closed'
            : 'Session closed before open',
        )
      }
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
        session.state.replayHistory = input.history === 'replay'
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
              canResume ? undefined : { idleTimeoutMs: 30_000 },
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
          session.spawnCwd,
        )
        if (session.transport.tier?.name)
          this.options.usage.rememberTier(dir, session.transport.tier.name)
        await session.drain()
        if (this.closed || session.isUnavailable())
          throw (
            session.state.failureError ??
            new Error('Session closed before open')
          )
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
        await session.subagents.refreshSubagents()
        let persistedEstimatedTokens: number | undefined
        if (input.persistence) {
          const persistedModel = currentModelId(session.state.metadata)
          persistedEstimatedTokens = (
            await session.refreshCompactionWindow({
              allowExisting: true,
              model: persistedModel,
            })
          )?.estimatedTokens
        }
        await session.flush()
        if (input.persistence) {
          if (session.state.replayHistory) {
            await session.replayTools()
          } else {
            await session.updateRuntimeEpoch()
            session.state.lastKnownStepIndex = Math.max(
              session.state.lastKnownStepIndex,
              session.state.runtimeEpoch?.afterStepIndex ?? -1,
            )
          }
        }
        await configureSession(
          session,
          input.config,
          input.persistence ? 'restore' : 'open',
        )
        await session.drain()
        if (input.persistence)
          await session.restoreUsage(persistedEstimatedTokens)
        if (!session.state.commandsReceived && !session.isUnavailable()) {
          await session.waitForCommands()
        }
        if (this.closed || session.isUnavailable())
          throw (
            session.state.failureError ??
            new Error('Session closed before ready')
          )
        session.state.phase = 'ready'
        session.flushDeferredUsage()
        emit({
          type: 'session.ready',
          sessionId: input.sessionId,
          requestId: input.requestId,
        })
        if (input.persistence && !this.closed) {
          session.replenishStandby()
        }
      } catch (error) {
        const wasOpened = session.state.opened
        const closedExternally = session.state.closedEmitted
        this.sessions.delete(input.sessionId)
        await session.close()
        if (wasOpened && !closedExternally && !session.state.closedEmitted) {
          // Reject the open request before session.closed so Paseo surfaces the real error
          // instead of replacing it with a generic "Provider session <id> closed".
          emit({
            type: 'request.failed',
            requestId: input.requestId,
            error: { message: formatErrorMessage(error) },
          })
          session.state.closedEmitted = true
          emit({ type: 'session.closed', sessionId: input.sessionId })
          throw new ReportedRequestError(error)
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
      const existing = this.sessions.get(input.sessionId)
      const pending = existing ? undefined : this.opening.get(input.sessionId)
      if (!existing && !pending) throw new Error('Session is not open')
      pending?.abort.abort(new Error('Session closed before open'))
      const session =
        existing ?? (await pending!.promise.catch(() => undefined))
      if (session) {
        const wasOpened = session.state.opened
        await session.close()
        this.sessions.delete(session.id)
        if (wasOpened && !session.state.closedEmitted) {
          session.state.closedEmitted = true
          emit({ type: 'session.closed', sessionId: session.id })
        }
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
        if (
          (!option || option.kind.startsWith('reject')) &&
          !isChildToolCallId(
            permission.request.toolCall.toolCallId,
            session.state.nativeId,
          )
        ) {
          session.state.activeTurn?.deniedToolCallIds.add(
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
    if (input.type === 'session.configure') {
      session.assertAlive()
      if (!session.isReady()) throw new Error('Session is not ready')
      if (session.state.activeTurn)
        throw new Error(
          'Wait for the current turn before changing configuration',
        )
      await session.runControl(async () => {
        if (session.state.activeTurn?.running)
          throw new Error(
            'Wait for the current turn before changing configuration',
          )
        await configureSession(session, input.changes)
      })
    } else if (input.type === 'session.interrupt') {
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
    this.closeController.abort(new Error('Provider connection is closed'))
    const pendingOpens = await Promise.all(
      [...this.opening.values()].map(({ promise }) =>
        promise.catch(() => undefined),
      ),
    )
    for (const s of pendingOpens) {
      if (s) this.sessions.set(s.id, s)
    }
    await Promise.all([...this.sessions.values()].map((s) => s.close()))
    await this.transports.closeAll()
    this.sessions.clear()
  }
}

async function createSession(
  id: string,
  cwd: string,
  config: ProviderSessionConfig | undefined,
  options: SessionOptions,
  transports: TransportTracker,
  startOptions?: { allowMissingCwd?: boolean },
  closeSignal?: AbortSignal,
) {
  const assertManagerOpen = () => {
    if (closeSignal?.aborted) throw abortError(closeSignal)
  }
  assertManagerOpen()
  const { launch, capabilities, emit } = options
  const spawnCwd = await resolveSpawnCwd(cwd, launch, {
    env: config?.env,
    allowMissingCwd: startOptions?.allowMissingCwd,
  })
  assertManagerOpen()
  const permissions = new Map<string, PendingPermission>()
  const mcpServerNames = Object.keys(config?.mcpServers ?? {})
  const timeline = new Timeline(id, mcpServerNames)
  const appData = dataDirectory(launch, config?.env, spawnCwd)
  const brainRoot = join(appData, 'brain')
  const state = {
    nativeId: '',
    phase: 'buffering' as SessionPhase,
    opened: false,
    persisted: false,
    replayHistory: true,
    closedEmitted: false,
    commandsReceived: false,
    skipPermissions: config?.mode === FULL_ACCESS_MODE_ID,
    lastKnownStepIndex: -1,
    activeTurn: null as ActiveTurnState | null,
    interrupting: null as Promise<void> | null,
    runtimeEpoch: undefined as RuntimeEpoch | undefined,
    failureError: undefined as Error | undefined,
    metadata: { sessionId: '' } as NewSessionResponse & SessionMetadata,
    prompts: new Map<string, true>(),
    hasPrompted: false,
  }
  const isClosing = () =>
    state.phase === 'closing' || state.phase === 'silenced'
  const isUnavailable = () => isClosing() || state.failureError !== undefined
  const isLoading = () =>
    state.phase === 'buffering' ||
    state.phase === 'loading' ||
    state.phase === 'reloading'
  const isReady = () => state.phase === 'ready' || state.phase === 'reloading'
  const isReloading = () => state.phase === 'reloading'
  const isSuppressingReplay = () =>
    state.phase === 'reloading' ||
    ((state.phase === 'buffering' || state.phase === 'loading') &&
      !state.replayHistory)
  const buffered: SessionNotification[] = []
  let collectingHistory = true
  const acpHistory: HistoryEntry[] = []
  const replayToolCallIds = new Set<string>()
  const acpToolResults = new Map<
    string,
    { rawOutput: boolean; content: boolean }
  >()
  const acpToolSnapshots = new Map<string, Readonly<ToolCall>>()
  const isCollectingReplay = () =>
    state.phase === 'loading' &&
    state.persisted &&
    state.replayHistory &&
    collectingHistory
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
    isParentTurnActive: () => state.activeTurn?.running === true,
    isLoading,
    isSuppressingReplay,
    isUnavailable,
    retryTimeoutMs: options.retryTimeoutMs,
    mcpServerNames,
    readSubagents: options.readSubagents,
    createTranscriptReader: options.createTranscriptReader,
  })
  let notificationLane = Promise.resolve()
  function enqueueNotification(fn: () => Promise<void>): Promise<void> {
    notificationLane = notificationLane.then(fn, fn).catch(() => undefined)
    return notificationLane
  }
  let controlLane = Promise.resolve()
  let controlActive = 0
  function runControl<T>(
    fn: (defer: (bg: Promise<void>) => void) => Promise<T>,
  ): Promise<T> {
    controlActive++
    let trailing: Promise<void> | undefined
    const fg = controlLane.then(
      () =>
        fn((bg) => {
          trailing = bg
        }),
      () =>
        fn((bg) => {
          trailing = bg
        }),
    )
    const finish = () => {
      controlActive--
    }
    const settled = fg.then(
      () => (trailing ? trailing.then(finish, finish) : finish()),
      () => (trailing ? trailing.then(finish, finish) : finish()),
    )
    controlLane = settled
    return fg.finally(() => (trailing ? undefined : settled))
  }
  const runtimeExitedError = () => sessionRuntimeExitedError(state.failureError)
  const assertAlive = () => {
    if (isClosing()) throw new Error('Session is closing')
    if (state.failureError) throw runtimeExitedError()
  }
  const nativeWindowSizes = new Map<string | undefined, number>()
  const compactionWindowSizes = new Map<string | undefined, number>()
  let sqlGenIdx = -1
  function activeModelWindow(): number | undefined {
    const model = currentModelId(state.metadata)
    return compactionWindowSizes.get(model) ?? nativeWindowSizes.get(model)
  }
  function withActiveWindow(next: ProviderUsage): ProviderUsage {
    if (next.contextWindowUsedTokens === undefined) return next
    const maxTokens = activeModelWindow()
    if (maxTokens !== undefined)
      return { ...next, contextWindowMaxTokens: maxTokens }
    if (next.contextWindowMaxTokens === undefined) return next
    const cleaned = { ...next }
    delete cleaned.contextWindowMaxTokens
    return cleaned
  }
  async function refreshCompactionWindow(options?: {
    allowExisting?: boolean
    model?: string
  }): Promise<{ maxTokens?: number; estimatedTokens?: number } | undefined> {
    const snapshot = await readConversationCompactionWindow(
      brainRoot,
      state.nativeId,
      options?.allowExisting ? -1 : sqlGenIdx,
    ).catch((error) => {
      if (error instanceof Error && error.name === 'BrainEscapeError')
        throw error
      return undefined
    })
    if (!snapshot) return undefined
    sqlGenIdx = Math.max(sqlGenIdx, snapshot.genIdx)
    if (snapshot.estimatedTokens !== undefined && state.activeTurn) {
      state.activeTurn.estimatedTokens = snapshot.estimatedTokens
    }
    if (snapshot.maxTokens !== undefined) {
      const targetModel =
        options && 'model' in options
          ? options.model
          : currentModelId(state.metadata)
      setBounded(compactionWindowSizes, targetModel, snapshot.maxTokens, 100)
    }
    return {
      maxTokens: snapshot.maxTokens,
      estimatedTokens: snapshot.estimatedTokens,
    }
  }
  function syncModelContextWindow() {
    const prevMax = usage.contextWindowMaxTokens
    usage = withActiveWindow(usage)
    if (
      state.opened &&
      !isUnavailable() &&
      usage.contextWindowUsedTokens !== undefined &&
      usage.contextWindowMaxTokens !== prevMax
    ) {
      if (isReady()) publishUsage({})
      else deferredUsage = true
    }
  }
  let usage: ProviderUsage = {}
  let deferredUsage = false
  let emittedCommands: string | undefined
  let commandWaiter: (() => void) | null = null
  let closing: Promise<void> | undefined
  function resolveCommands() {
    commandWaiter?.()
    commandWaiter = null
  }
  async function waitForCommands(
    timeoutMs = options.commandWaitTimeoutMs ?? 1000,
  ) {
    if (state.commandsReceived || isUnavailable()) return
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
    if (!state.opened || state.phase === 'silenced') return
    usage = withActiveWindow({ ...usage, ...next })
    deferredUsage = false
    emit({
      type: 'session.usage',
      sessionId: id,
      turnId: state.activeTurn?.id,
      usage,
    })
  }
  function flushDeferredUsage() {
    if (!deferredUsage || !state.opened || state.phase === 'silenced') return
    deferredUsage = false
    emit({
      type: 'session.usage',
      sessionId: id,
      turnId: state.activeTurn?.id,
      usage,
    })
  }
  function terminalize(
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
  ) {
    if (state.phase === 'silenced') return
    for (const item of timeline.terminalize(
      status,
      error,
      state.activeTurn?.deniedToolCallIds,
    )) {
      emit({ type: 'timeline.item', sessionId: id, item })
    }
  }
  function finishTurnOnce(
    turnId: string,
    status: 'completed' | 'canceled' | 'failed',
    error?: string,
    beforeEmit?: () => void,
  ) {
    const turn = state.activeTurn
    if (
      !turn ||
      turn.id !== turnId ||
      turn.finished ||
      state.phase === 'silenced'
    )
      return
    turn.finished = true
    terminalize(status, error)
    beforeEmit?.()
    emit({
      type: 'session.turn',
      sessionId: id,
      turnId,
      state: status,
      ...(status === 'failed' && error ? { error: { message: error } } : {}),
    })
  }
  function emitItem(
    item: ProviderTimelineItem,
    timestamp?: string,
    toolResult?: { rawOutput: unknown },
  ) {
    if (state.phase === 'silenced') return
    if (item.type === 'tool_call' && toolResult)
      item = timeline.recoverToolResult(item, toolResult.rawOutput)
    timeline.track(item)
    subagents.linkSubsession(id, state.nativeId, item)
    emit({
      type: 'timeline.item',
      sessionId: id,
      item,
      ...(timestamp ? { timestamp } : {}),
    })
  }
  /** Link child sessions and publish an item the timeline already tracks. */
  function publishItem(item: ProviderTimelineItem, timestamp?: string) {
    if (state.phase === 'silenced' || isSuppressingReplay()) return
    subagents.linkSubsession(id, state.nativeId, item)
    emit({
      type: 'timeline.item',
      sessionId: id,
      item,
      ...(timestamp ? { timestamp } : {}),
    })
  }
  async function update(
    notification: SessionNotification,
    foreground = !(state.activeTurn?.inSettle ?? false),
  ) {
    if (isUnavailable()) return
    if (state.nativeId && notification.sessionId !== state.nativeId) return
    if (state.phase === 'buffering') {
      buffered.push(notification)
      return
    }
    if (notification.sessionId !== state.nativeId) return
    const u = notification.update
    if (u.sessionUpdate === 'config_option_update') {
      const prevModel = currentModelId(state.metadata)
      state.metadata.configOptions = u.configOptions
      if (isReloading()) return
      if (currentNativeMode(state.metadata) === 'plan')
        state.metadata.fullAccess = false
      const nextModel = currentModelId(state.metadata)
      if (!isLoading() && nextModel !== prevModel) {
        await refreshCompactionWindow({ model: prevModel }).catch(
          () => undefined,
        )
        if (isUnavailable()) return
        syncModelContextWindow()
      }
      publishConfig()
    } else if (u.sessionUpdate === 'available_commands_update') {
      const next = u.availableCommands.map((c) => ({
        name: c.name,
        description: c.description,
        argumentHint: c.input?.hint,
      }))
      const serialized = JSON.stringify(next)
      if (
        state.opened &&
        state.phase !== 'silenced' &&
        serialized !== emittedCommands
      ) {
        emittedCommands = serialized
        emit({
          type: 'session.commands',
          sessionId: id,
          commands: next,
        })
      }
      state.commandsReceived = true
      resolveCommands()
    } else if (u.sessionUpdate === 'usage_update') {
      const next = contextUsage(u)
      if (!next) return
      const turn = state.activeTurn
      if (!isLoading() && turn && !foreground) turn.sawPostRpcAcp = true
      const model = currentModelId(state.metadata)
      setBounded(nativeWindowSizes, model, u.size, 100)
      // agy writes server-issued max_context_tokens to SQLite before/as steps emit usage_update;
      // readConversationCompactionWindow is O(1) stat-cached on .db/.db-wal, so refreshing on every
      // live usage_update picks up mid-turn/TUI AGY_CLI_AUTO_COMPACT_OVERRIDE or Orchestrator switches.
      if (!isLoading() && state.nativeId) {
        await refreshCompactionWindow({ model }).catch(() => undefined)
        if (isUnavailable()) return
      }
      if (!isSuppressingReplay()) {
        if (turn) turn.hadContextUsage = true
        publishUsage(next)
      } else if (!isReady()) {
        usage = withActiveWindow({ ...usage, ...next })
        deferredUsage = true
      }
    } else {
      if (isSuppressingReplay()) return
      const turn = state.activeTurn
      if (u.sessionUpdate === 'user_message_chunk' && !isLoading()) {
        if (turn) {
          turn.toolCallIds.clear()
          turn.foregroundAcp = []
        }
        timeline.boundary()
        return
      }
      // 1.5.16+ forwards child/workflow tool updates on the root session prefixed
      // with `<childConvUUID>:<origCallId>`; SubagentRegistry Brain watchers render children.
      if (
        (u.sessionUpdate === 'tool_call' ||
          u.sessionUpdate === 'tool_call_update') &&
        isChildToolCallId(u.toolCallId, state.nativeId)
      )
        return
      if (
        !isLoading() &&
        turn &&
        !foreground &&
        (u.sessionUpdate === 'agent_message_chunk' ||
          u.sessionUpdate === 'agent_thought_chunk' ||
          u.sessionUpdate === 'tool_call' ||
          u.sessionUpdate === 'tool_call_update')
      )
        turn.sawPostRpcAcp = true
      const isNewToolCall =
        u.sessionUpdate === 'tool_call' &&
        (turn
          ? !turn.toolCallIds.has(u.toolCallId)
          : isCollectingReplay() && !replayToolCallIds.has(u.toolCallId))
      if (isNewToolCall) {
        if (turn) turn.toolCallIds.add(u.toolCallId)
        else replayToolCallIds.add(u.toolCallId)
      }
      if (!isLoading() && turn && foreground) {
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
          const last = turn.foregroundAcp[turn.foregroundAcp.length - 1]
          if (last && last.type === segType) last.text += u.content.text
          else
            turn.foregroundAcp.push({
              type: segType,
              text: u.content.text,
            })
        } else if (isNewToolCall) {
          turn.foregroundAcp.push({
            type: 'tool_call',
            callId: u.toolCallId,
          })
        }
      }
      if (
        turn &&
        (u.sessionUpdate === 'agent_message_chunk' ||
          u.sessionUpdate === 'agent_thought_chunk' ||
          u.sessionUpdate === 'tool_call')
      )
        turn.hadModelOutput = true
      // Timeline.update already tracks the ACP item.
      const cancelFailed =
        !state.failureError && isUserCancelAbort(turn?.abort.signal)
      const item = timeline.update(u, turn?.deniedToolCallIds, cancelFailed)
      if (item) {
        if (!(await subagents.prepareMessageItem(item))) return
        if (isCollectingReplay()) {
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
    if (!state.opened || isUnavailable()) return
    emit({
      type: 'session.config',
      sessionId: id,
      config: configState(state.metadata),
    })
  }
  function cancelPermissions() {
    const entries = [...permissions]
    permissions.clear()
    for (const [permissionId, p] of entries) {
      if (p.type === 'acp') {
        if (!isChildToolCallId(p.request.toolCall.toolCallId, state.nativeId)) {
          state.activeTurn?.deniedToolCallIds.add(p.request.toolCall.toolCallId)
        }
        p.resolve({ outcome: { outcome: 'cancelled' } })
      } else {
        p.resolve(null)
      }
      if (state.opened && state.phase !== 'silenced') {
        emit({
          type: 'session.permission_resolved',
          sessionId: id,
          permissionId,
        })
      }
    }
  }
  const client = {
    sessionUpdate: (notification: SessionNotification) => {
      const foreground = !(state.activeTurn?.inSettle ?? false)
      return enqueueNotification(() => update(notification, foreground))
    },
    requestElicitation: async (params: Record<string, unknown>) => {
      const turn = state.activeTurn
      if (
        !state.nativeId ||
        (typeof params.sessionId === 'string' &&
          params.sessionId !== state.nativeId) ||
        !turn ||
        isSuppressingReplay() ||
        isUnavailable()
      )
        return { action: 'cancel' }
      const questions = normalizeElicitationQuestions(params)
      if (!questions.length) return { action: 'cancel' }
      const input = toQuestionPermissionInput(questions)
      const permissionId = randomUUID()
      const response = await new Promise<PermissionResponseInput | null>(
        (resolve) => {
          permissions.set(permissionId, { type: 'question', resolve })
          emit({
            type: 'session.permission',
            sessionId: id,
            request: {
              id: permissionId,
              name: 'ask_question',
              kind: 'question',
              title:
                (typeof params.message === 'string' && params.message.trim()) ||
                questions[0].question,
              input,
              detail: toolDetail({ title: 'ask_question', rawInput: input }),
              actions: [
                { id: 'allow', label: 'Submit', behavior: 'allow' },
                { id: 'deny', label: 'Cancel', behavior: 'deny' },
              ],
            },
          })
        },
      )
      if (!response) return { action: 'cancel' }
      const callId =
        typeof params.toolCallId === 'string' ? params.toolCallId : undefined
      if (!isChildToolCallId(callId, state.nativeId)) {
        if (turn.inSettle && response.behavior === 'deny')
          turn.wakeupQuestionDeclined = true
        turn.handledElicitations.push({
          questions,
          callId,
          answerText: formatQuestionAnswersText(questions, response) || null,
          wokeUp: turn.inSettle,
          declined: response.behavior === 'deny',
        })
      }
      return buildElicitationResponse(questions, response)
    },
    requestPermission: (request: RequestPermissionRequest) => {
      const parsed = RequestPermissionRequestSchema.safeParse(request)
      const turn = state.activeTurn
      if (
        !parsed.success ||
        !state.nativeId ||
        request.sessionId !== state.nativeId ||
        !turn ||
        isSuppressingReplay() ||
        isUnavailable()
      )
        return Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
      const rawInput = parsed.data.toolCall.rawInput
      const validatedInput =
        rawInput !== null &&
        typeof rawInput === 'object' &&
        !Array.isArray(rawInput)
          ? rawInput
          : {}
      const detail = toolDetail(request.toolCall)
      const kind = permissionKind(request, detail)
      const preapproved = isPreapprovedMcpTool(
        request.toolCall,
        config?.toolPolicy,
        mcpServerNames,
      )
      if (kind !== 'question' && (state.metadata.fullAccess || preapproved)) {
        const allow = pickPermissionOption(request.options, 'allow')
        if (
          !allow &&
          !isChildToolCallId(request.toolCall.toolCallId, state.nativeId)
        )
          turn.deniedToolCallIds.add(request.toolCall.toolCallId)
        return Promise.resolve({
          outcome: allow
            ? { outcome: 'selected' as const, optionId: allow.optionId }
            : { outcome: 'cancelled' as const },
        })
      }
      const displayInput = stripToolSummary(
        validatedInput,
      ) as typeof validatedInput
      const securityWarning = extractSecurityWarning(request)
      return new Promise<RequestPermissionResponse>((resolve) => {
        const permissionId = randomUUID()
        permissions.set(permissionId, { type: 'acp', request, resolve })
        emit({
          type: 'session.permission',
          sessionId: id,
          request: {
            id: permissionId,
            name: toolDisplayName(
              { ...request.toolCall, name: record(request.toolCall).name },
              mcpServerNames,
            ),
            kind,
            title: request.toolCall.title ?? undefined,
            ...(securityWarning
              ? { description: securityWarning.description }
              : {}),
            input: displayInput,
            detail,
            actions: request.options.map((o) => {
              const behavior = o.kind.startsWith('allow')
                ? ('allow' as const)
                : ('deny' as const)
              return {
                id: o.optionId,
                label: o.name,
                behavior,
                ...(behavior === 'allow' &&
                securityWarning?.warnedOptionIds.has(o.optionId)
                  ? { variant: 'danger' as const }
                  : {}),
              }
            }),
          },
        })
      })
    },
  }
  const retiring = new WeakSet<ReturnType<typeof connectAcp>>()
  const onExit = (error: Error) => {
    if (isUnavailable()) return
    const message = formatErrorMessage(error)
    const formatted = message === error.message ? error : new Error(message)
    state.failureError = formatted
    state.activeTurn?.abort.abort(formatted)
    resolveCommands()
    cancelPermissions()
    terminalize('failed', message)
    if (isReady())
      emit({
        type: 'session.runtime_failed',
        sessionId: id,
        error: { message },
      })
  }
  const sessionEnv = config?.env ?? {}
  function createSessionCallbacks(
    getTransport: () => ControlTransport | undefined,
  ): {
    sessionClient: AcpClient
    sessionOnExit: (error: Error) => void
  } {
    const isStale = () => {
      const transport = getTransport()
      return (
        transport !== undefined &&
        (transport !== currentTransport || retiring.has(transport))
      )
    }
    return {
      sessionClient: {
        sessionUpdate: (notification) =>
          isStale() ? Promise.resolve() : client.sessionUpdate(notification),
        requestPermission: (request) =>
          isStale()
            ? Promise.resolve({ outcome: { outcome: 'cancelled' as const } })
            : client.requestPermission(request),
        requestElicitation: (params) =>
          isStale()
            ? Promise.resolve({ action: 'cancel' })
            : client.requestElicitation(params),
      },
      sessionOnExit: (error) => {
        if (isStale()) return
        onExit(error)
      },
    }
  }
  const resolveLaunchFingerprint = (
    runLaunch: ProviderLaunch | undefined,
  ): Promise<string | undefined> =>
    options.standby
      ? options.standby.resolveBinaryFingerprint(runLaunch, sessionEnv)
      : binaryFingerprint(runLaunch, { env: sessionEnv }).catch(() => undefined)
  /** Start an owned ACP runtime; it leaves the connection's transports once it exits and stops. */
  function startColdRuntime(skipPermissions: boolean, fingerprint?: string) {
    const runLaunch = effectiveLaunch(launch, skipPermissions)
    let boundTransport: ControlTransport | undefined
    const { sessionClient, sessionOnExit } = createSessionCallbacks(
      () => boundTransport,
    )
    const { transport, bind } = createAttachableTransport(
      options.connectAcp ?? connectAcp,
      runLaunch,
      spawnCwd,
      sessionEnv,
      sessionClient,
      sessionOnExit,
    )
    boundTransport = transport
    transports.track(transport)
    return {
      transport,
      toAttachable: (initialized: InitializedAcp): AttachableRuntime => ({
        transport,
        initialized,
        key: standbyPoolKey(runLaunch, sessionEnv, fingerprint),
        reusable: isLaunchCwdIndependent(spawnCwd, runLaunch, sessionEnv),
        attach: (nextClient, nextOnExit) => bind(nextClient, nextOnExit),
      }),
    }
  }
  async function acquireRuntime(
    skipPermissions: boolean,
    checkAlive: () => boolean,
  ): Promise<
    | {
        transport: ControlTransport
        initialized: InitializedAcp
        fromStandby: true
      }
    | {
        transport: ControlTransport
        toAttachable: (initialized: InitializedAcp) => AttachableRuntime
        fromStandby: false
      }
    | undefined
  > {
    const targetLaunch = effectiveLaunch(launch, skipPermissions)
    const warm = await options.standby?.take(targetLaunch, spawnCwd, sessionEnv)
    let alive = false
    try {
      alive = checkAlive()
    } catch (error) {
      if (warm) await warm.transport.close().catch(() => undefined)
      throw error
    }
    if (!alive) {
      if (warm) await warm.transport.close().catch(() => undefined)
      return undefined
    }
    if (warm && !warm.transport.closed) {
      currentTransport = warm.transport
      currentAttachable = warm
      const { sessionClient, sessionOnExit } = createSessionCallbacks(
        () => warm.transport,
      )
      warm.attach(sessionClient, sessionOnExit)
      transports.track(warm.transport)
      return {
        transport: warm.transport,
        initialized: warm.initialized,
        fromStandby: true,
      }
    }
    if (warm) await warm.transport.close().catch(() => undefined)
    const fingerprint = await resolveLaunchFingerprint(targetLaunch)
    if (!checkAlive()) return undefined
    const started = startColdRuntime(skipPermissions, fingerprint)
    currentTransport = started.transport
    currentAttachable = undefined
    return {
      transport: started.transport,
      toAttachable: started.toAttachable,
      fromStandby: false,
    }
  }
  let currentTransport!: ControlTransport
  let currentAttachable: AttachableRuntime | undefined
  let currentInitialized!: InitializedAcp
  const initialAcquired = (await acquireRuntime(state.skipPermissions, () => {
    assertManagerOpen()
    return true
  }))!
  if (initialAcquired.fromStandby) {
    currentInitialized = initialAcquired.initialized
  } else {
    const onCloseAbort = () => {
      void currentTransport.close().catch(() => undefined)
    }
    closeSignal?.addEventListener('abort', onCloseAbort, { once: true })
    try {
      currentInitialized = await initializeAcp(currentTransport, closeSignal)
      assertManagerOpen()
      currentAttachable = initialAcquired.toAttachable(currentInitialized)
    } catch (error) {
      await currentTransport.close().catch(() => undefined)
      throw error
    } finally {
      closeSignal?.removeEventListener('abort', onCloseAbort)
    }
  }
  function replenishStandby(skipPermissions = state.skipPermissions) {
    if (isUnavailable() || skipPermissions || hasSkipPermissionsFlag(launch))
      return
    options.standby?.warm(launch, spawnCwd, sessionEnv)
  }
  function canReloadSession() {
    return Boolean(
      currentInitialized.agentCapabilities?.sessionCapabilities?.resume ||
      currentInitialized.agentCapabilities?.loadSession,
    )
  }
  function isReloadAlive(next: ControlTransport): boolean {
    if (isClosing()) {
      void next.close().catch(() => undefined)
      return false
    }
    if (retiring.has(next))
      throw new Error('ACP runtime superseded during relaunch')
    if (state.failureError || next.closed)
      throw (
        state.failureError ?? new Error('ACP runtime exited during relaunch')
      )
    return true
  }
  function isRetireAlive(obsolete: ControlTransport): boolean {
    if (isClosing()) return false
    if (currentTransport !== obsolete)
      throw new Error('ACP runtime superseded during relaunch')
    if (state.failureError) throw runtimeExitedError()
    return true
  }
  async function updateRuntimeEpoch(): Promise<void> {
    state.runtimeEpoch = await captureRuntimeEpoch(brainRoot, state.nativeId, {
      minAfterStep: Math.max(
        state.lastKnownStepIndex,
        state.runtimeEpoch?.afterStepIndex ?? -1,
      ),
      previous: state.runtimeEpoch,
      readSubagents: options.readSubagents,
    }).catch(() => state.runtimeEpoch)
  }
  async function reloadIntoRuntime(
    next: ControlTransport,
    nextInitialized: InitializedAcp | undefined,
    toAttachable:
      ((initialized: InitializedAcp) => AttachableRuntime) | undefined,
    skipPermissions: boolean,
    restoreMode: boolean,
    prevPhase: SessionPhase,
    prev: ReturnType<typeof configState>,
    prevModels: SessionMetadata['models'],
    nativeMode: string | undefined,
  ): Promise<void> {
    try {
      currentInitialized =
        nextInitialized ?? (await initializeAcp(next, closeSignal))
      if (toAttachable && currentTransport === next) {
        currentAttachable = toAttachable(currentInitialized)
      }
      if (!isReloadAlive(next)) return
      if (!canReloadSession())
        throw new Error(
          'agy cannot reload after interruption; reopen the session',
        )
      state.skipPermissions = skipPermissions
      const canResume = Boolean(
        currentInitialized.agentCapabilities?.sessionCapabilities?.resume,
      )
      const params = {
        ...sessionParams(cwd, config),
        sessionId: state.nativeId,
      }
      state.metadata = {
        ...(await next.request(
          canResume
            ? next.rpc.resumeSession(params)
            : next.rpc.loadSession(params),
          canResume ? undefined : { idleTimeoutMs: 30_000 },
        )),
        sessionId: state.nativeId,
        models: next.models ?? prevModels,
        fullAccess: skipPermissions,
      }
      await drain()
      if (!isReloadAlive(next)) return
      for (const [k, v] of [
        ['model', prev.model],
        ['thought_level', prev.thinkingOption],
        ['mode', restoreMode ? nativeMode : undefined],
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
      if (!isReloadAlive(next)) return
      await updateRuntimeEpoch()
    } catch (error) {
      void next.close().catch(() => undefined)
      if (isClosing() || retiring.has(next)) return
      onExit(error instanceof Error ? error : new Error(String(error)))
      throw error
    } finally {
      if (state.phase === 'reloading') state.phase = prevPhase
      if (restoreMode && !isUnavailable()) publishConfig()
    }
  }
  async function replaceRuntime(opts: {
    skipPermissions: boolean
    sendCancel: boolean
    deferReload?: (bg: Promise<void>) => void
  }): Promise<void> {
    if (isClosing()) return
    if (state.failureError) throw runtimeExitedError()
    const { skipPermissions, sendCancel, deferReload } = opts
    const prev = configState(state.metadata)
    const prevModels = state.metadata.models
    const nativeMode = currentNativeMode(state.metadata)
    if (retiring.has(currentTransport))
      throw new Error('ACP runtime superseded during relaunch')
    const obsolete = currentTransport
    retiring.add(obsolete)
    state.activeTurn?.observation?.stop()
    const prevPhase = state.phase
    state.phase = 'reloading'
    let handedToReload = false
    try {
      await raceTimeout(subagents.cancelAll(), 150, () =>
        subagents.forceStopAll(),
      )
      if (sendCancel) {
        await obsolete
          .request(obsolete.rpc.cancel({ sessionId: state.nativeId }), {
            timeoutMs: 100,
            fatalOnTimeout: false,
          })
          .catch(() => undefined)
      }
      try {
        await obsolete.close()
      } catch (error) {
        if (isClosing()) return
        if (currentTransport === obsolete) {
          onExit(error instanceof Error ? error : new Error(String(error)))
        }
        throw error
      }
      if (!isRetireAlive(obsolete)) return
      if (!canReloadSession()) {
        const error = new Error(
          'agy cannot reload after interruption; reopen the session',
        )
        onExit(error)
        throw error
      }
      await updateRuntimeEpoch()
      if (!isRetireAlive(obsolete)) return
      const acquired = await acquireRuntime(skipPermissions, () =>
        isRetireAlive(obsolete),
      )
      if (!acquired) return
      if (acquired.fromStandby) replenishStandby(skipPermissions)
      handedToReload = true
      const reload = reloadIntoRuntime(
        acquired.transport,
        acquired.fromStandby ? acquired.initialized : undefined,
        acquired.fromStandby ? undefined : acquired.toAttachable,
        skipPermissions,
        Boolean(deferReload),
        prevPhase,
        prev,
        prevModels,
        nativeMode,
      )
      if (deferReload) deferReload(reload)
      else await reload
    } catch (error) {
      if (!handedToReload && !isClosing() && currentTransport === obsolete) {
        onExit(error instanceof Error ? error : new Error(String(error)))
      }
      throw error
    } finally {
      if (
        !handedToReload &&
        currentTransport === obsolete &&
        state.phase === 'reloading'
      ) {
        state.phase = prevPhase
      }
    }
  }
  async function relaunchForPermissions(
    skipPermissions: boolean,
  ): Promise<void> {
    if (isClosing()) return
    if (state.failureError) throw runtimeExitedError()
    if (state.skipPermissions === skipPermissions || !canReloadSession()) return
    await replaceRuntime({
      skipPermissions,
      sendCancel: false,
    })
  }
  const backgroundProbe = new BackgroundWorkProbe()
  function hasBackgroundWork(probeOptions?: {
    includeDaemons?: boolean
  }): Promise<boolean> {
    const check = options.hasActiveBackgroundWork
    if (check) {
      return check(brainRoot, state.nativeId, {
        includeDaemons: probeOptions?.includeDaemons,
        retryTimeoutMs: options.retryTimeoutMs,
        runtimeEpoch: state.runtimeEpoch,
      })
    }
    return backgroundProbe.hasActiveWork(brainRoot, state.nativeId, {
      includeDaemons: probeOptions?.includeDaemons,
      retryTimeoutMs: options.retryTimeoutMs,
      runtimeEpoch: state.runtimeEpoch,
    })
  }
  function syncBackgroundProbe(): Promise<void> {
    if (options.hasActiveBackgroundWork || isClosing() || !state.nativeId)
      return Promise.resolve()
    return backgroundProbe
      .sync(brainRoot, state.nativeId, state.runtimeEpoch)
      .catch(() => undefined)
  }
  return {
    id,
    cwd,
    spawnCwd,
    config,
    mcpServerNames,
    appData,
    brainRoot,
    stallTimeoutMs: options.stallTimeoutMs,
    retryTimeoutMs: options.retryTimeoutMs,
    cancelConvergenceTimeoutMs: options.cancelConvergenceTimeoutMs,
    observationPollIntervalMs: options.observationPollIntervalMs,
    createObservation: options.createObservation,
    createTranscriptReader: options.createTranscriptReader,
    state,
    subagents,
    isClosing,
    isUnavailable,
    isReady,
    assertAlive,
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
    flushDeferredUsage,
    finishTurnOnce,
    emitItem,
    drain,
    waitForCommands,
    refreshCompactionWindow,
    syncModelContextWindow,
    publishStepContext(lastStep: StepUsage) {
      const { contextWindowUsedTokens } = promptUsage(null, { lastStep })
      if (contextWindowUsedTokens !== undefined)
        publishUsage({ contextWindowUsedTokens })
    },
    async restoreUsage(estimatedTokens?: number) {
      if (usage?.contextWindowUsedTokens !== undefined) return
      const snapshot = await readLatestStepUsage(
        brainRoot,
        state.nativeId,
      ).catch(() => undefined)
      if (snapshot?.lastStepUsage)
        this.publishStepContext(snapshot.lastStepUsage)
      else if (estimatedTokens !== undefined)
        publishUsage({ contextWindowUsedTokens: estimatedTokens })
    },
    async replayTools() {
      const [inbox, subagentRecords, rows, tailStep] = await Promise.all([
        readInboxState(brainRoot, state.nativeId),
        (options.readSubagents ?? readSubagents)(brainRoot, state.nativeId),
        readTranscriptRows(brainRoot, state.nativeId),
        readLastStepIndex(brainRoot, state.nativeId).catch(() => -1),
      ])
      state.runtimeEpoch = computeRuntimeEpoch(
        rows,
        inbox,
        subagentRecords,
        Math.max(state.lastKnownStepIndex, tailStep),
        state.runtimeEpoch,
      )
      subagents.rememberSubagents(subagentRecords)
      // Drain ACP session/load notifications first so replayToolCallIds and acpToolSnapshots are complete;
      // projectReplayHistory() below is purely synchronous and never triggers ACP notifications.
      await drain()
      collectingHistory = false
      const { entries, lastKnownStepIndex } = projectReplayHistory({
        sessionId: id,
        rows,
        inbox,
        subagents: subagentRecords,
        acpHistory: acpHistory.splice(0),
        acpToolCallIds: [...replayToolCallIds],
        acpToolSnapshots,
        acpResultProvided,
        mcpServerNames,
      })
      if (lastKnownStepIndex > state.lastKnownStepIndex)
        state.lastKnownStepIndex = lastKnownStepIndex
      for (const { item, timestamp } of entries) {
        await subagents.waitCurrentReady()
        emitItem(item, timestamp)
      }
      replayToolCallIds.clear()
      acpToolResults.clear()
      acpToolSnapshots.clear()
      // A child opened by the last replayed tool must read its history before a historical kill cancels it.
      await subagents.waitNestedReady()
      await subagents.syncKilledChildren(subagentRecords)
      await Promise.all(subagents.cancelRunning())
    },
    async canStop(): Promise<boolean> {
      if (
        isClosing() ||
        isLoading() ||
        controlActive > 0 ||
        state.activeTurn ||
        state.interrupting
      )
        return false
      if (state.failureError) return true
      if (permissions.size > 0 || subagents.hasRunningChildren()) return false
      return !(await hasBackgroundWork({ includeDaemons: true }).catch(
        () => true,
      ))
    },
    hasBackgroundWork,
    syncBackgroundProbe,
    updateRuntimeEpoch,
    async waitForRuntimeReady(signal: AbortSignal): Promise<void> {
      while (state.interrupting || controlActive > 0) {
        signal.throwIfAborted()
        await raceAbort(
          Promise.all([
            state.interrupting?.catch(() => undefined),
            controlLane,
          ]),
          signal,
        )
      }
      signal.throwIfAborted()
      assertAlive()
    },
    runControl,
    async flush() {
      if (state.phase === 'buffering') state.phase = 'loading'
      for (const item of buffered.splice(0))
        enqueueNotification(() => update(item))
      await notificationLane
    },
    async interrupt(): Promise<void> {
      if (isClosing()) return
      if (state.failureError) throw runtimeExitedError()
      if (!isReady()) return
      state.activeTurn?.abort.abort()
      cancelPermissions()
      if (state.interrupting) return state.interrupting
      if (controlActive > 0) return
      // Children stay registered when canceled so they are neither reopened nor left unclosed.
      const canceling = subagents.cancelRunning()
      // agy 1.5.16+ session/cancel (ForceStopTree) uses a 5s CancelGrace (exceeding Paseo's 2s
      // interrupt budget), can leave detached OS process groups on timeout, and leaves subagent
      // metadata as SUBAGENT_STATE_ALIVE instead of writing SUBAGENT_STATE_KILLED; retire and
      // reload the runtime (with a fresh RuntimeEpoch) when children or background work are active.
      const task = runControl(async (defer) => {
        const needsRelaunch = await raceTimeout(
          Promise.all([
            Promise.all(canceling),
            hasBackgroundWork().catch(() => true),
          ]).then(([, activeBg]) => canceling.length > 0 || activeBg),
          options.backgroundProbeTimeoutMs ?? 200,
          () => true,
        )
        if (isClosing()) return
        if (state.failureError) throw runtimeExitedError()
        if (needsRelaunch) {
          await replaceRuntime({
            skipPermissions: Boolean(state.metadata.fullAccess),
            sendCancel: true,
            deferReload: defer,
          })
          return
        }
        const active =
          state.activeTurn?.activePrompt?.transport === currentTransport
            ? state.activeTurn.activePrompt
            : null
        try {
          await currentTransport.request(
            (async () => {
              await currentTransport.rpc.cancel({
                sessionId: state.nativeId,
              })
              if (active) await active.settled
            })(),
            {
              timeoutMs: options.cancelConvergenceTimeoutMs ?? 350,
              fatalOnTimeout: false,
            },
          )
        } catch {
          if (isClosing()) return
          if (state.failureError) throw runtimeExitedError()
          await replaceRuntime({
            skipPermissions: Boolean(state.metadata.fullAccess),
            sendCancel: false,
            deferReload: defer,
          })
        }
      })
      const settled = task.finally(() => {
        if (state.interrupting === settled) state.interrupting = null
      })
      state.interrupting = settled
      await settled
    },
    relaunchForPermissions,
    replenishStandby,
    close(): Promise<void> {
      if (closing) return closing
      const wasReady = state.phase === 'ready'
      const closingTurn = state.activeTurn
      state.phase = 'closing'
      closingTurn?.observation?.stop()
      closingTurn?.abort.abort()
      resolveCommands()
      closing = (async () => {
        cancelPermissions()
        subagents.closeAll()
        if (closingTurn) finishTurnOnce(closingTurn.id, 'canceled')
        else terminalize('canceled')
        state.phase = 'silenced'
        const disposable =
          !state.persisted &&
          (config?.persist === false || !state.hasPrompted) &&
          sessionIdPattern.test(state.nativeId)
        const closeOk = disposable
          ? await closeDisposableSession(currentTransport, state.nativeId)
          : false
        await closingTurn?.observation?.dispose()
        const recyclable =
          disposable &&
          wasReady &&
          closeOk &&
          !state.failureError &&
          !state.skipPermissions &&
          !hasSkipPermissionsFlag(launch) &&
          controlActive === 0 &&
          !state.interrupting &&
          !retiring.has(currentTransport) &&
          !state.hasPrompted &&
          mcpServerNames.length === 0 &&
          currentAttachable !== undefined &&
          currentAttachable.transport === currentTransport &&
          currentAttachable.reusable &&
          options.standby !== undefined
        let recycled = false
        if (recyclable) {
          recycled = await options.standby!.recycle(
            currentAttachable!,
            effectiveLaunch(launch, state.skipPermissions),
            spawnCwd,
            sessionEnv,
          )
          if (recycled) transports.untrack(currentTransport)
        }
        const cleaned = recycled
          ? true
          : await currentTransport.close().then(
              () => true,
              () => false,
            )
        if (disposable && cleaned)
          await removeSessionArtifacts(appData, state.nativeId).catch(
            () => undefined,
          )
      })()
      return closing
    },
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

type ConfigurePolicy = 'strict' | 'open' | 'restore'

function isKnownConfigChoice(
  current: ReturnType<typeof configState>,
  key: NativeConfigKey,
  value: string,
): boolean {
  const choices =
    key === 'model'
      ? current.models
      : key === 'mode'
        ? current.modes
        : current.thinkingOptions
  return choices.some((o) => o.id === value)
}

async function configureSession(
  session: Session,
  changes: Pick<
    ProviderConfigChanges,
    'model' | 'mode' | 'thinkingOption' | 'settings'
  >,
  policy: ConfigurePolicy = 'strict',
) {
  session.assertAlive()
  if (changes.settings && Object.keys(changes.settings).length)
    throw new Error('Custom settings are not supported')
  const isBestEffort = (key: NativeConfigKey) =>
    policy === 'restore' || (policy === 'open' && key === 'thought_level')
  const entries = [
    ['model', changes.model],
    ['thought_level', changes.thinkingOption],
    ['mode', changes.mode],
  ] as const
  const initial = configState(session.state.metadata)
  for (const [key, value] of entries) {
    if (value == null || isBestEffort(key)) continue
    if (key === 'mode' && value === FULL_ACCESS_MODE_ID) continue
    // Switching `model` dynamically adds or removes `thought_level`, so re-evaluate it after `model`.
    if (key === 'thought_level' && changes.model != null) continue
    if (value === selectedConfigValue(initial, key)) continue
    if (!isAdvertisedConfigOption(session.state.metadata, initial, key))
      throw new Error(`agy does not advertise ${key}`)
  }
  const willSwitchModel =
    changes.model != null &&
    changes.model !== initial.model &&
    (!isBestEffort('model') ||
      (isAdvertisedConfigOption(session.state.metadata, initial, 'model') &&
        isKnownConfigChoice(initial, 'model', changes.model)))
  if (willSwitchModel) {
    await session.refreshCompactionWindow({ model: initial.model })
  }
  let mutated = false
  let succeeded = false
  try {
    for (const [key, value] of entries) {
      if (value == null) continue
      const current = configState(session.state.metadata)
      if (value === selectedConfigValue(current, key)) continue
      const isFullAccess = key === 'mode' && value === FULL_ACCESS_MODE_ID
      if (!isFullAccess) {
        if (!isAdvertisedConfigOption(session.state.metadata, current, key)) {
          if (
            isBestEffort(key) ||
            (key === 'thought_level' && changes.model != null)
          )
            continue
          throw new Error(`agy does not advertise ${key}`)
        }
        if (isBestEffort(key) && !isKnownConfigChoice(current, key, value))
          continue
      }
      if (key === 'mode') {
        await session.relaunchForPermissions(isFullAccess)
        session.assertAlive()
        session.state.metadata.fullAccess = isFullAccess
        mutated = true
        if (isFullAccess) {
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
      }
      const applied = await applyNativeConfigOption(
        session.transport,
        session.state.nativeId,
        session.state.metadata,
        key,
        value,
        !isBestEffort(key),
      )
      mutated ||= applied
    }
    succeeded = true
  } finally {
    if (currentModelId(session.state.metadata) !== initial.model) {
      await session.drain()
      session.syncModelContextWindow()
    }
    if (succeeded || mutated) session.publishConfig()
  }
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
  if (session.isClosing()) throw new Error('Session is closing')
  if (!session.isReady()) throw new Error('Session is not ready')
  if (session.state.activeTurn)
    throw new Error('Session already has an active turn')
  session.assertAlive()
  if (p.delivery === 'steer' || p.outputSchema)
    throw new Error('Steering and output schemas are not supported')
  if (session.spawnCwd !== session.cwd)
    throw new Error(`Working directory does not exist: ${session.cwd}`)
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
  const turn: ActiveTurnState = {
    id: turnId,
    abort: turnAbort,
    running: false,
    finished: false,
    inSettle: false,
    hadContextUsage: false,
    hadModelOutput: false,
    sawPostRpcAcp: false,
    estimatedTokens: undefined,
    handledElicitations: [],
    wakeupQuestionDeclined: false,
    deniedToolCallIds: new Set(),
    toolCallIds: new Set(),
    foregroundAcp: [],
    activePrompt: null,
    observation: null,
  }
  session.state.activeTurn = turn
  let started = false
  const isFirstNewPrompt =
    !session.state.persisted && !session.state.hasPrompted
  session.state.hasPrompted = true
  setBounded(
    session.state.prompts,
    p.clientMessageId,
    true,
    PROMPT_DEDUPE_LIMIT,
  )
  if (isFirstNewPrompt) session.replenishStandby()
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
  let effectiveUsage: Usage | null | undefined
  let lastStep: StepUsage | undefined
  let turnEstimatedTokens: number | undefined
  let turnWokeUp = false
  let turnWakeupUsage: StepUsage | undefined
  let pendingFinish:
    { status: 'completed' | 'canceled' | 'failed'; error?: string } | undefined
  const isCurrent = () =>
    session.state.activeTurn === turn &&
    !session.isClosing() &&
    session.state.nativeId === nativeId &&
    session.transport === observationTransport
  const settleTurnUsage = (isError: boolean) => {
    if (!started) return
    const preserveContext = turn.hadContextUsage && !turnWokeUp
    const fallbackContextUsed =
      !preserveContext &&
      !lastStep &&
      turnEstimatedTokens !== undefined &&
      (!isError || isCurrent())
        ? turnEstimatedTokens
        : undefined
    if (effectiveUsage || turnWakeupUsage) {
      const next = promptUsage(effectiveUsage, {
        preserveContext,
        lastStep,
        wakeup: turnWakeupUsage,
      })
      if (
        next.contextWindowUsedTokens === undefined &&
        fallbackContextUsed !== undefined
      ) {
        next.contextWindowUsedTokens = fallbackContextUsed
      }
      session.publishUsage(next)
    } else if (lastStep && !preserveContext && (!isError || isCurrent())) {
      session.publishStepContext(lastStep)
    } else if (fallbackContextUsed !== undefined) {
      session.publishUsage({ contextWindowUsedTokens: fallbackContextUsed })
    } else if (!isError || isCurrent()) {
      session.syncModelContextWindow()
    }
  }
  const stopObservation = () => observation?.stop()
  turnAbort.signal.addEventListener('abort', stopObservation, { once: true })
  const waitPrompt = async () => {
    turnAbort.signal.throwIfAborted()
    const transport = session.transport
    let settledOutcome:
      | {
          ok: true
          value: Awaited<ReturnType<typeof session.transport.rpc.prompt>>
        }
      | { ok: false; error: unknown }
      | null = null
    const outcome = transport.rpc
      .prompt({
        sessionId: nativeId,
        prompt: blocks,
      })
      .then(
        (value) => {
          if (
            session.state.activeTurn === turn &&
            session.transport === transport
          )
            turn.inSettle = true
          return (settledOutcome = { ok: true as const, value })
        },
        (error) => (settledOutcome = { ok: false as const, error }),
      )
    const promptEntry = {
      transport,
      settled: outcome.then(() => undefined),
    }
    turn.activePrompt = promptEntry
    let aborted!: () => void
    const cancellation = new Promise<null>((resolve) => {
      aborted = () => resolve(null)
    })
    turnAbort.signal.addEventListener('abort', aborted, { once: true })
    try {
      if (turnAbort.signal.aborted) aborted()
      let result = await Promise.race([outcome, cancellation])
      if (!result) {
        if (session.isClosing()) throw turnAbort.signal.reason
        await session.state.interrupting
        if (session.isClosing()) throw turnAbort.signal.reason
        result = settledOutcome
        if (!result) {
          if (session.state.failureError) throw session.state.failureError
          if (transport.closed || session.transport !== transport) {
            turnAbort.signal.throwIfAborted()
            throw new Error('ACP runtime closed during prompt')
          }
          result = await outcome
        }
      }
      if (!result.ok) throw result.error
      return result.value
    } finally {
      if (turn.activePrompt === promptEntry) turn.activePrompt = null
      turnAbort.signal.removeEventListener('abort', aborted)
    }
  }
  const finalDrain = async () => {
    observation?.stop()
    if (session.isClosing()) return
    await session.state.interrupting
    await session.drain()
    if (
      session.state.activeTurn !== turn ||
      session.isClosing() ||
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
      if (minStepIndex === undefined) return
      await finalDrain()
      if (!isCurrent()) return
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
  const finalizeTurnUsage = async (isError: boolean) => {
    lastStep = isError
      ? await reconcileFinalSnapshot(lastStep).catch(
          () => observation?.usage.latest,
        )
      : await reconcileFinalSnapshot(lastStep)
    if (!session.isClosing() && session.state.nativeId === nativeId) {
      const refreshed = isError
        ? await session.refreshCompactionWindow().catch(() => undefined)
        : await session.refreshCompactionWindow()
      const estimated = refreshed?.estimatedTokens ?? turn.estimatedTokens
      turnEstimatedTokens = isError
        ? (turnEstimatedTokens ?? estimated)
        : estimated
    }
    turnWokeUp ||= observation?.wokeUp === true
  }
  try {
    await session.waitForRuntimeReady(turnAbort.signal)
    turnAbort.signal.throwIfAborted()
    observationTransport = session.transport
    turn.running = true
    started = true
    const [readStep, stepErrorBaseline] = await Promise.all([
      readLastStepIndex(session.brainRoot, session.state.nativeId).catch(
        () => -1,
      ),
      captureConversationStepBaseline(
        session.brainRoot,
        session.state.nativeId,
      ),
    ])
    minStepIndex = Math.max(
      session.state.lastKnownStepIndex,
      session.state.runtimeEpoch?.afterStepIndex ?? -1,
      readStep,
    )
    session.state.lastKnownStepIndex = minStepIndex
    turnAbort.signal.throwIfAborted()
    observation = (
      session.createObservation ??
      ((...args: ConstructorParameters<typeof BrainTurnObservation>) =>
        new BrainTurnObservation(...args))
    )(
      session.brainRoot,
      nativeId,
      session.id,
      minStepIndex,
      {
        isCurrent,
        onUsage: (usage) => {
          if (turn.hadContextUsage && !observation?.wokeUp) return false
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
      session.observationPollIntervalMs,
      session.createTranscriptReader?.(),
      session.state.runtimeEpoch?.afterStepIndex ?? -1,
    )
    turn.observation = observation
    observation.start()
    const result = await waitPrompt()
    await session.drain()
    if (!isCurrent() && !turnAbort.signal.aborted)
      throw new Error('ACP runtime replaced during turn')
    effectiveUsage = hasPositivePromptUsage(result.usage)
      ? result.usage
      : undefined
    let status: 'completed' | 'canceled' | 'failed' =
      result.stopReason === 'cancelled'
        ? 'canceled'
        : result.stopReason === 'refusal'
          ? 'failed'
          : 'completed'
    let turnError: string | undefined
    const readHiddenStepError = () =>
      readConversationStepError(
        session.brainRoot,
        session.state.nativeId,
        stepErrorBaseline,
        result.stopReason,
      )
    if (result.stopReason === 'refusal') {
      const hiddenError = await readHiddenStepError()
      turnError = hiddenError?.message ?? 'Request was refused.'
    } else if (status === 'completed') {
      turn.inSettle = true
      const settled = await settleBrainTurn(
        session.brainRoot,
        session.state.nativeId,
        session.id,
        session.emitItem,
        {
          signal: turnAbort.signal,
          observation,
          mcpServerNames: session.mcpServerNames,
          onSubagents: session.subagents.rememberSubagents,
          minStepIndex,
          runtimeEpoch: session.state.runtimeEpoch,
          readHiddenError: async () => (await readHiddenStepError())?.message,
          // Pass the live Set by reference so post-RPC wakeup tool_call notifications remain visible while settling.
          acpToolCallIds: turn.toolCallIds,
          drainAcp: session.drain,
          toolSnapshot: (id) => session.timeline.toolCallSnapshot(id),
          hasToolResult: (id) => session.timeline.hasToolResult(id),
          onWakeupState: (wakeup) => {
            turnWokeUp = wakeup.wokeUp
            turnWakeupUsage = wakeup.wakeupUsage
          },
          foregroundAcp: [...turn.foregroundAcp],
          hasPostRpcAcp: () => turn.sawPostRpcAcp,
          deniedToolCallIds: turn.deniedToolCallIds,
          shouldUpdateTool: (item) =>
            session.timeline.isRunning(item.id) ||
            (item.detail.type === 'sub_agent' &&
              item.detail.childSessionId !== undefined &&
              session.subagents.canLinkChild(
                item.detail.childSessionId,
                session.state.nativeId,
                item.name,
              ) &&
              // Unknown children open, interrupted ones resume.
              (!session.subagents.hasChild(item.detail.childSessionId) ||
                session.subagents.isChildResumable(
                  item.detail.childSessionId,
                ))),
          onStep: (stepIndex) => {
            if (stepIndex > session.state.lastKnownStepIndex)
              session.state.lastKnownStepIndex = stepIndex
          },
          // A model turn always writes USER_INPUT; give agy a moment to flush it. Native commands may write none.
          transcriptGraceMs: turn.hadModelOutput ? 2000 : 0,
          stallTimeoutMs: session.stallTimeoutMs,
          retryTimeoutMs: session.retryTimeoutMs,
          isWaitingOnUser: () => session.permissions.size > 0,
          isWaitingOnQuestion: () =>
            [...session.permissions.values()].some(
              (permission) => permission.type === 'question',
            ),
          takeQuestionAnswer: (questions, wokeUp, callId) => {
            const index = turn.handledElicitations.findIndex(
              (entry) =>
                entry.wokeUp === wokeUp &&
                (!callId || !entry.callId || entry.callId === callId) &&
                sameNormalizedQuestions(entry.questions, questions),
            )
            if (index >= 0) return turn.handledElicitations.splice(index, 1)[0]
          },
          isWakeupQuestionDeclined: () => turn.wakeupQuestionDeclined,
        },
      )
      await session.subagents.syncKilledChildren()
      status = settled.status
      turnError = settled.error
      lastStep = settled.lastStepUsage
      turnWokeUp = Boolean(settled.wokeUp)
      turnWakeupUsage = settled.wakeupUsage
      if (!turnAbort.signal.aborted && !session.isUnavailable()) {
        if (settled.status === 'failed') {
          await session.subagents.flushActive()
        }
        const needsFailureCleanup =
          Boolean(settled.stalled) ||
          (settled.status === 'failed' &&
            (session.subagents.hasRunningChildren() ||
              (await session.hasBackgroundWork().catch(() => false))))
        if (needsFailureCleanup) {
          await session.interrupt().catch(() => undefined)
        }
      }
      if (
        status === 'completed' &&
        !turnAbort.signal.aborted &&
        !session.isUnavailable()
      ) {
        const hiddenError = await readHiddenStepError()
        if (hiddenError) {
          status = 'failed'
          turnError = hiddenError.message
        }
      }
    }
    if (turn.wakeupQuestionDeclined && !turnAbort.signal.aborted) {
      await session.interrupt()
      status = 'canceled'
    }
    await finalizeTurnUsage(false)
    if (!session.isClosing() && session.state.failureError)
      throw sessionRuntimeExitedError(session.state.failureError)
    if (turnAbort.signal.aborted) {
      if (!isUserCancelAbort(turnAbort.signal))
        turnAbort.signal.throwIfAborted()
      // Stall or child failures cancel the runtime for cleanup without changing their terminal status.
      if (status !== 'failed') status = 'canceled'
    }
    settleTurnUsage(false)
    pendingFinish = { status, error: turnError }
  } catch (error) {
    const restartError = session.isClosing()
      ? undefined
      : await session.state.interrupting?.then(
          () => undefined,
          (err: unknown) => err,
        )
    await finalizeTurnUsage(true)
    settleTurnUsage(true)
    const failure = session.state.failureError ?? restartError ?? error
    pendingFinish =
      !session.state.failureError &&
      !restartError &&
      isUserCancelAbort(turnAbort.signal)
        ? { status: 'canceled' }
        : { status: 'failed', error: formatErrorMessage(failure) }
  } finally {
    turnAbort.signal.removeEventListener('abort', stopObservation)
    const disposing = observation?.dispose()
    if (session.state.activeTurn === turn) {
      const clearTurnState = () => {
        if (session.state.activeTurn !== turn) return
        session.state.activeTurn = null
        session.timeline.boundary()
      }
      session.cancelPermissions()
      if (pendingFinish) {
        session.finishTurnOnce(
          turnId,
          pendingFinish.status,
          pendingFinish.error,
          clearTurnState,
        )
      }
      clearTurnState()
    }
    await disposing
    if (!session.isUnavailable()) {
      await session.syncBackgroundProbe()
    }
  }
}
