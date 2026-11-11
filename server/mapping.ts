import { z } from 'zod'
import type {
  SessionUpdate,
  ToolCall,
  ToolCallUpdate,
  SessionConfigOption,
  NewSessionResponse,
  ContentBlock,
  UsageUpdate,
  Usage,
} from '@agentclientprotocol/sdk'
import type {
  ProviderConfigState,
  ProviderMode,
  ProviderTimelineItem,
  ProviderToolCallDetail,
  ProviderContent,
  ProviderUsage,
} from '@getpaseo/plugin/server/provider'
import {
  cleanToolName,
  isAskQuestionName,
  sessionIdPattern,
  setBounded,
  record,
  str,
  num,
  nonnegative,
  text,
} from './common.ts'
import { normalizeAskQuestionInput } from './questions.ts'

export const FULL_ACCESS_MODE_ID = 'full-access'

const modeVisuals: Record<
  string,
  Pick<ProviderMode, 'icon' | 'colorTier' | 'isUnattended'>
> = {
  default: { icon: 'Shield', colorTier: 'safe' },
  'accept-edits': { icon: 'ShieldPlus', colorTier: 'moderate' },
  plan: { icon: 'ShieldEllipsis', colorTier: 'planning' },
  [FULL_ACCESS_MODE_ID]: {
    icon: 'ShieldOff',
    colorTier: 'dangerous',
    isUnattended: true,
  },
}

function joinChunks(
  chunks: readonly Record<string, unknown>[] | undefined,
  ...keys: string[]
): string | undefined {
  if (!chunks) return undefined
  return chunks
    .map((c) => {
      for (const k of keys) {
        const v = str(c[k])
        if (v !== undefined) return v
      }
      return ''
    })
    .join('\n...\n')
}

const searchToolNames: ReadonlyArray<
  readonly [string, 'grep' | 'glob' | 'web_search']
> = [
  ['grep', 'grep'],
  ['find_by_name', 'glob'],
  ['glob', 'glob'],
  ['search_web', 'web_search'],
  ['web_search', 'web_search'],
]

function extractToolContentText(
  tool: Partial<ToolCall> | ToolCallUpdate,
): string | undefined {
  const joined = tool.content
    ?.map((c) =>
      c.type === 'content' && c.content.type === 'text' ? c.content.text : '',
    )
    .filter(Boolean)
    .join('\n')
  return joined || undefined
}

function cleanReadOutput(output: string | undefined): {
  content: string | undefined
  startLine?: number
  endLine?: number
} {
  if (!output) return { content: output }
  const header = output.match(
    /^(?:Created At:[^\n]*\n)?(?:Completed At:[^\n]*\n)?Showing lines (\d+)-(\d+) of \d+\r?\n/,
  )
  const body = header ? output.slice(header[0].length) : output
  const lines = body.split('\n')
  const numbered =
    lines.length > 0 &&
    lines.every((line, i) =>
      i === lines.length - 1 && line === '' ? true : /^\d+:(?: |$)/.test(line),
    )
  return {
    content: numbered
      ? lines.map((line) => line.replace(/^\d+:(?: |$)/, '')).join('\n')
      : body,
    ...(header
      ? { startLine: Number(header[1]), endLine: Number(header[2]) }
      : {}),
  }
}

function nonEmptyRawOutput(rawOutput: unknown): string | undefined {
  if (typeof rawOutput === 'string')
    return rawOutput.trim() ? rawOutput : undefined
  if (rawOutput == null || rawOutput === '') return undefined
  const obj = record(rawOutput)
  const keys = Object.keys(obj)
  if (keys.length === 0) return undefined
  if (keys.length === 1 && obj.output === '') return undefined
  return JSON.stringify(rawOutput)
}

/** Map native tool arguments and output to Paseo detail cards. */
const canonicalWriteToolPattern =
  /^(?:write_file|write_to_file|create_file|fs\/write_text_file|write_text_file)$/i
// Native identities and ACP kinds declare semantics; argument keys do not.
const nativeToolKinds: Record<string, ToolCall['kind']> = {
  invoke_subagent: 'other',
  send_message: 'other',
  ask_question: 'other',
  ask_user: 'other',
  run_command: 'execute',
  read_file: 'read',
  view_file: 'read',
  write_file: 'edit',
  write_to_file: 'edit',
  create_file: 'edit',
  'fs/write_text_file': 'edit',
  write_text_file: 'edit',
  edit_file: 'edit',
  replace_file_content: 'edit',
  multi_replace_file_content: 'edit',
  read_url_content: 'fetch',
  grep_search: 'search',
  find_by_name: 'search',
  glob: 'search',
  search_web: 'search',
  web_search: 'search',
}
const jsonValueSchema = z.json()
const TOOL_SNAPSHOT_LIMIT = 1024

function isTerminalToolStatus(status: unknown): boolean {
  return (
    status === 'completed' ||
    status === 'failed' ||
    status === 'canceled' ||
    status === 'cancelled'
  )
}

export function toolDetail(
  tool: Partial<ToolCall> | ToolCallUpdate,
): ProviderToolCallDetail {
  const input = record(tool.rawInput)
  const output =
    typeof tool.rawOutput === 'string'
      ? tool.rawOutput
      : (extractToolContentText(tool) ?? nonEmptyRawOutput(tool.rawOutput))
  const explicitRawName = str(record(tool).name)
  const rawName = cleanToolName(explicitRawName ?? tool.title ?? '')
  // Legacy ACP titles use `Run <native identity>` when no raw name is sent.
  const legacyName = rawName.startsWith('Run ')
    ? rawName.slice(4).toLowerCase()
    : ''
  const name =
    explicitRawName === undefined && Object.hasOwn(nativeToolKinds, legacyName)
      ? legacyName
      : rawName.toLowerCase()
  const kind = Object.hasOwn(nativeToolKinds, name)
    ? nativeToolKinds[name]
    : explicitRawName === undefined
      ? tool.kind
      : undefined
  if (isAskQuestionName(name)) {
    const questions = normalizeAskQuestionInput(input)
    if (questions.length > 0) {
      const formatted = questions
        .map((q) => {
          const opts = q.options
            .map((o, i) => `  ${i + 1}. ${o.label}`)
            .join('\n')
          return opts ? `${q.question}\n${opts}` : q.question
        })
        .join('\n\n')
      return {
        type: 'plain_text',
        label: rawName || 'ask_question',
        text: output ? `${formatted}\n\nAnswer:\n${output}` : formatted,
      }
    }
  }
  const filePath =
    str(
      input.Path ??
        input.TargetFile ??
        input.AbsolutePath ??
        input.file_path ??
        input.path,
    ) ?? tool.locations?.[0]?.path
  const command = str(input.CommandLine ?? input.command)
  if (kind === 'execute' && command) {
    const exitMatch = output?.match(/Command exited with code (\d+)/)
    const exitCode =
      num(record(tool.rawOutput).exitCode) ??
      (exitMatch ? Number(exitMatch[1]) : undefined)
    return {
      type: 'shell',
      command,
      cwd: str(input.Cwd ?? input.cwd),
      output,
      ...(exitCode !== undefined ? { exitCode } : {}),
    }
  }
  const url = str(input.Url ?? input.url)
  if (kind === 'fetch' && url)
    return { type: 'fetch', url, ...(output ? { result: output } : {}) }
  if (name === 'invoke_subagent' || name === 'send_message') {
    const data = record(tool.rawOutput)
    const childSessionId =
      str(data.conversationId ?? data.conversation_id) ??
      output?.match(
        /(?:conversationId|conversation_id)["\s:=]+([a-f0-9-]{36})/i,
      )?.[1] ??
      (typeof input.Recipient === 'string' &&
      sessionIdPattern.test(input.Recipient)
        ? input.Recipient
        : undefined)
    return {
      type: 'sub_agent',
      description: str(
        input.Prompt ??
          input.Task ??
          input.task ??
          input.Message ??
          input.message,
      ),
      subAgentType: str(
        input.Role ??
          input.TypeName ??
          input.Name ??
          input.name ??
          input.Recipient,
      ),
      childSessionId,
      log: output ?? '',
    }
  }
  const planText =
    kind === 'edit' && record(input.ArtifactMetadata).RequestFeedback === true
      ? str(input.Content ?? input.CodeContent ?? input.Replacement)
      : undefined
  if (planText) return { type: 'plan', text: planText }
  const diff = tool.content?.find((c) => c.type === 'diff')
  const nativeWriteContent = str(input.Content ?? input.CodeContent)
  const writeContent = nativeWriteContent ?? str(input.content)
  const hasEditTarget =
    input.Target !== undefined ||
    input.TargetContent !== undefined ||
    input.old_string !== undefined ||
    input.oldString !== undefined ||
    input.OldString !== undefined ||
    Array.isArray(input.ReplacementChunks) ||
    diff?.oldText != null
  const isCanonicalWrite = canonicalWriteToolPattern.test(name)
  const isUnnamedWriteFallback =
    explicitRawName === undefined &&
    kind === 'edit' &&
    !hasEditTarget &&
    (nativeWriteContent !== undefined ||
      (diff?.type === 'diff' &&
        Boolean(diff.path) &&
        diff.oldText == null &&
        Object.keys(input).length === 0 &&
        /^file(?:\s|$)/i.test(rawName)))
  if (
    (filePath || (diff?.type === 'diff' && diff.path)) &&
    (isCanonicalWrite || isUnnamedWriteFallback)
  ) {
    return {
      type: 'write',
      filePath: (filePath ?? diff?.path)!,
      content:
        writeContent ?? (diff?.type === 'diff' ? diff.newText : undefined),
    }
  }
  if (
    diff?.type === 'diff' &&
    (kind === 'edit' || !rawName) &&
    (diff.path || filePath)
  )
    return {
      type: 'edit',
      filePath: (diff.path || filePath)!,
      oldString: diff.oldText ?? '',
      newString: diff.newText,
    }
  const query = str(
    input.Query ??
      input.Pattern ??
      input.query ??
      input.pattern ??
      input.SearchDirectory,
  )
  if (query && kind === 'search') {
    const toolName = searchToolNames.find(([pat]) => name.includes(pat))?.[1]
    const filePaths = tool.locations
      ?.map((l) => l.path)
      .filter((p): p is string => Boolean(p))
    return {
      type: 'search',
      query,
      ...(toolName ? { toolName } : {}),
      content: output,
      ...(filePaths?.length ? { filePaths } : {}),
    }
  }
  if (filePath) {
    if (isCanonicalWrite)
      return {
        type: 'write',
        filePath,
        content: writeContent,
      }
    if (kind === 'edit') {
      const chunks = Array.isArray(input.ReplacementChunks)
        ? input.ReplacementChunks.map(record)
        : undefined
      return {
        type: 'edit',
        filePath,
        oldString:
          str(
            input.Target ??
              input.TargetContent ??
              input.old_string ??
              input.oldString ??
              input.OldString,
          ) ?? joinChunks(chunks, 'TargetContent', 'Target'),
        newString:
          str(
            input.Replacement ??
              input.ReplacementContent ??
              input.new_string ??
              input.newString ??
              input.NewString,
          ) ?? joinChunks(chunks, 'ReplacementContent', 'Replacement'),
      }
    }
    if (kind === 'read') {
      const cleaned = cleanReadOutput(output)
      const start = num(input.StartLine) ?? cleaned.startLine
      const end = num(input.EndLine) ?? cleaned.endLine
      return {
        type: 'read',
        filePath,
        content: cleaned.content,
        offset:
          num(
            input.offset ??
              input.StartLine ??
              input.ByteOffset ??
              input.ContentOffset,
          ) ?? cleaned.startLine,
        limit:
          num(input.limit) ??
          (start !== undefined && end !== undefined
            ? Math.max(1, end - start + 1)
            : undefined),
      }
    }
  }
  return {
    type: 'unknown',
    input: jsonValueSchema.parse(tool.rawInput ?? null),
    output: jsonValueSchema.parse(
      tool.rawOutput !== undefined ? tool.rawOutput : (tool.content ?? null),
    ),
  }
}

/** agy still advertises the legacy per-effort model inventory. */
export const nativeModelsSchema = z.object({
  currentModelId: z.string().optional(),
  availableModels: z.array(
    z.object({
      modelId: z.string(),
      name: z.string().optional(),
      description: z.string().optional(),
    }),
  ),
})
export type SessionMetadata = Pick<NewSessionResponse, 'configOptions'> & {
  models?: z.infer<typeof nativeModelsSchema>
  fullAccess?: boolean
}

/** Convert native catalog options to Paseo's model-specific composer controls. */
export function configState(response: SessionMetadata): ProviderConfigState {
  const options = (response.configOptions ?? []).filter(
    (o): o is Extract<SessionConfigOption, { type: 'select' }> =>
      o.type === 'select',
  )
  const model = options.find((o) => o.category === 'model' || o.id === 'model')
  const thinking = options.find(
    (o) => o.category === 'thought_level' || o.id === 'thought_level',
  )
  const mode = options.find((o) => o.category === 'mode' || o.id === 'mode')
  const choices = (o?: Extract<SessionConfigOption, { type: 'select' }>) =>
    o?.options
      .flatMap((v) => ('group' in v ? v.options : [v]))
      .map((v) => ({
        id: v.value,
        label: v.name,
        description: v.description ?? undefined,
      })) ?? []
  const thinkingOptions = choices(thinking)
  const available = response.models?.availableModels
  const models = (
    model
      ? choices(model)
      : (available?.map((m) => ({
          id: m.modelId,
          label: m.name ?? m.modelId,
          description: m.description,
        })) ?? [])
  ).map((m) => {
    const fixed = /-(low|medium|high)$/.test(m.id)
    const offered = available
      ?.filter((a) => a.modelId.startsWith(`${m.id}-`))
      .map((a) => a.modelId.slice(m.id.length + 1))
    const inventoried = available?.some(
      (a) => a.modelId === m.id || a.modelId.startsWith(`${m.id}-`),
    )
    const efforts = fixed
      ? []
      : inventoried
        ? thinkingOptions.filter((o) => offered?.includes(o.id))
        : thinkingOptions
    return {
      ...m,
      description:
        m.description ??
        available?.find((a) => a.modelId === m.id)?.description,
      contextWindowMaxTokens: contextWindowForModel(m.id),
      thinkingOptions: efforts,
    }
  })
  const modelId = model?.currentValue ?? response.models?.currentModelId
  const currentThinking =
    models.find((m) => m.id === modelId)?.thinkingOptions ?? thinkingOptions
  const nativeModes = choices(mode)
    .filter((m) => m.id !== FULL_ACCESS_MODE_ID)
    .map((m) => ({ ...m, ...modeVisuals[m.id] }))
  return {
    model: modelId,
    mode: response.fullAccess ? FULL_ACCESS_MODE_ID : mode?.currentValue,
    thinkingOption: currentThinking.some((o) => o.id === thinking?.currentValue)
      ? thinking?.currentValue
      : undefined,
    models,
    modes: [
      ...nativeModes,
      {
        id: FULL_ACCESS_MODE_ID,
        label: 'Full Access',
        description:
          'Approve file edits, commands and tool requests without asking.',
        ...modeVisuals[FULL_ACCESS_MODE_ID],
      },
    ],
    thinkingOptions: currentThinking,
    settings: [],
  }
}

/** Preserve attachment context as text while retaining native image blocks. */
export function promptContent(content: ProviderContent): ContentBlock {
  if (content.type === 'image')
    return { type: 'image', data: content.data, mimeType: content.mimeType }
  if (content.type === 'text') {
    const res =
      'externalResource' in content ? content.externalResource : undefined
    const title = 'title' in content ? text(content.title) : undefined
    if (!res && !title) return { type: 'text', text: content.text }
    const header = res
      ? `${res.providerLabel} ${res.identifier}: ${res.title}`
      : title
    return {
      type: 'text',
      text: [
        header,
        res && title && title !== res.title ? title : undefined,
        res?.url,
        content.text,
      ]
        .filter(Boolean)
        .join('\n'),
    }
  }
  if (content.type === 'uploaded_file')
    return {
      type: 'text',
      text: `Attached file: ${content.fileName} (${content.mimeType}, ${content.size} bytes)\nPath: ${content.path}`,
    }
  if (content.type === 'review') {
    return {
      type: 'text',
      text: [
        `Code review: ${content.cwd} (${content.mode}${content.baseRef ? `, base: ${content.baseRef}` : ''})`,
        ...content.comments.map(
          (c) =>
            `${c.filePath}:${c.lineNumber} (${c.side})\n${c.body}\n${c.context.hunkHeader}\n${c.context.lines.map((l) => l.content).join('\n')}`,
        ),
      ].join('\n\n'),
    }
  }
  return {
    type: 'text',
    text: [
      `${content.type} #${content.number}: ${content.title}`,
      content.url,
      'projectPath' in content ? content.projectPath : undefined,
      'baseRefName' in content
        ? `${content.headRefName ?? '?'} → ${content.baseRefName ?? '?'}`
        : undefined,
      content.body,
    ]
      .filter(Boolean)
      .join('\n'),
  }
}

/** Ignore malformed native usage; retain context size and USD cost when valid. */
export function contextUsage(update: UsageUpdate): ProviderUsage | undefined {
  if (
    !nonnegative(update.used) ||
    !nonnegative(update.size) ||
    update.size === 0
  )
    return
  return {
    contextWindowUsedTokens: update.used,
    contextWindowMaxTokens: update.size,
    ...(update.cost?.currency === 'USD' && nonnegative(update.cost.amount)
      ? { totalCostUsd: update.cost.amount }
      : {}),
  }
}

/** Native model limits used when ACP does not report a context window size. */
export function contextWindowForModel(modelId?: string): number {
  return modelId?.startsWith('gpt-oss') ? 131_072 : 256_000
}

export type StepUsage = {
  inputTokens: number
  outputTokens: number
  cachedReadTokens: number
  /** Only present when agy records a step-level cache write counter. */
  cachedWriteTokens?: number
}

export function addStepUsage(
  a: StepUsage | undefined,
  b: StepUsage,
): StepUsage {
  const cachedWriteTokens =
    (a?.cachedWriteTokens ?? 0) + (b.cachedWriteTokens ?? 0)
  return {
    inputTokens: (a?.inputTokens ?? 0) + b.inputTokens,
    outputTokens: (a?.outputTokens ?? 0) + b.outputTokens,
    cachedReadTokens: (a?.cachedReadTokens ?? 0) + b.cachedReadTokens,
    ...(cachedWriteTokens > 0 ? { cachedWriteTokens } : {}),
  }
}

/** Combine ACP turn token counters with per-step context occupancy from transcript_full.jsonl when available. */
export function promptUsage(
  usage?: Usage | null,
  options?: {
    model?: string
    preserveContext?: boolean
    lastStep?: StepUsage
    carriedWakeup?: StepUsage
    wakeup?: StepUsage
  },
): ProviderUsage {
  if (!usage && !options?.lastStep && !options?.wakeup) return {}
  const raw = record(usage)
  const carried =
    options?.carriedWakeup &&
    nonnegative(raw.inputTokens) &&
    raw.inputTokens >= options.carriedWakeup.inputTokens &&
    nonnegative(raw.outputTokens) &&
    raw.outputTokens >= options.carriedWakeup.outputTokens
      ? options.carriedWakeup
      : undefined
  const field = (
    rawVal: unknown,
    sub: number | undefined,
    add: number | undefined,
    fallback: number | undefined,
  ) =>
    nonnegative(rawVal)
      ? Math.max(0, rawVal - (sub ?? 0)) + (add ?? 0)
      : ((add || fallback) ?? 0)
  const input = field(
    raw.inputTokens,
    carried?.inputTokens,
    options?.wakeup?.inputTokens,
    options?.lastStep?.inputTokens,
  )
  const cacheRead = field(
    raw.cachedReadTokens,
    carried?.cachedReadTokens,
    options?.wakeup?.cachedReadTokens,
    options?.lastStep?.cachedReadTokens,
  )
  const cacheWrite = field(
    raw.cachedWriteTokens,
    carried?.cachedWriteTokens,
    options?.wakeup?.cachedWriteTokens,
    options?.lastStep?.cachedWriteTokens,
  )
  const output = field(
    raw.outputTokens,
    carried?.outputTokens,
    options?.wakeup?.outputTokens,
    options?.lastStep?.outputTokens,
  )
  const step = options?.lastStep
  // Each source is summed on its own: per-step counters never mix with cumulative turn counters.
  const used = step
    ? step.inputTokens +
      step.cachedReadTokens +
      (step.cachedWriteTokens ?? 0) +
      step.outputTokens
    : input + cacheRead + cacheWrite + output ||
      (nonnegative(raw.totalTokens) ? raw.totalTokens : 0)
  return {
    inputTokens: input,
    outputTokens: output,
    cachedInputTokens: cacheRead,
    ...(!options?.preserveContext
      ? {
          contextWindowUsedTokens: used,
          contextWindowMaxTokens: contextWindowForModel(options?.model),
        }
      : {}),
  }
}

/** Assemble full snapshots and terminalize tools whose final native update is absent. */
export class Timeline {
  private sequence = 0
  private message?: Extract<
    ProviderTimelineItem,
    { type: 'user_message' | 'assistant_message' | 'reasoning' }
  >
  private tools = new Map<string, ToolCall>()
  private running = new Map<
    string,
    Extract<ProviderTimelineItem, { type: 'tool_call' }>
  >()
  private prefix: string
  constructor(prefix: string) {
    this.prefix = prefix
  }
  boundary() {
    this.message = undefined
  }
  track(item: ProviderTimelineItem) {
    if (item.type !== 'tool_call') return
    if (item.status === 'running') this.running.set(item.id, item)
    else this.running.delete(item.id)
  }
  isRunning(id: string): boolean {
    return this.running.has(id)
  }
  terminalize(
    status: 'completed' | 'canceled' | 'failed',
    error = 'Tool did not finish',
    denied?: ReadonlySet<string>,
  ): ProviderTimelineItem[] {
    const items = [...this.running.values()].map((item) => {
      const isDenied = Boolean(item.callId && denied?.has(item.callId))
      const nextStatus = isDenied ? ('canceled' as const) : status
      if (item.callId) {
        const prev = this.tools.get(item.callId)
        if (prev) {
          setBounded(
            this.tools,
            item.callId,
            {
              ...prev,
              status: (nextStatus === 'canceled'
                ? 'cancelled'
                : nextStatus) as ToolCall['status'],
            },
            TOOL_SNAPSHOT_LIMIT,
          )
        }
      }
      if (nextStatus === 'failed')
        return { ...item, status: 'failed' as const, error }
      return { ...item, status: nextStatus, error: null }
    })
    this.running.clear()
    return items
  }
  update(
    update: SessionUpdate,
    denied?: ReadonlySet<string>,
  ): ProviderTimelineItem | undefined {
    if (
      update.sessionUpdate === 'user_message_chunk' ||
      update.sessionUpdate === 'agent_message_chunk' ||
      update.sessionUpdate === 'agent_thought_chunk'
    ) {
      if (update.content.type !== 'text') return
      const type =
        update.sessionUpdate === 'user_message_chunk'
          ? 'user_message'
          : update.sessionUpdate === 'agent_message_chunk'
            ? 'assistant_message'
            : 'reasoning'
      if (this.message?.type !== type)
        this.message = {
          id: `${this.prefix}:message:${++this.sequence}`,
          type,
          text: '',
        }
      this.message = {
        ...this.message!,
        text: this.message!.text + update.content.text,
      }
      return this.message
    }
    this.boundary()
    if (
      update.sessionUpdate === 'tool_call' ||
      update.sessionUpdate === 'tool_call_update'
    ) {
      const old =
        update.sessionUpdate === 'tool_call_update'
          ? this.tools.get(update.toolCallId)
          : undefined
      if (
        !old &&
        update.sessionUpdate === 'tool_call_update' &&
        update.status == null &&
        update.title == null &&
        str(record(update).name) === undefined
      ) {
        return undefined
      }
      const oldStatus = str((old as { status?: unknown } | undefined)?.status)
      const incomingStatus = str((update as { status?: unknown }).status)
      const preserveStatus =
        update.sessionUpdate === 'tool_call_update' &&
        isTerminalToolStatus(oldStatus) &&
        !isTerminalToolStatus(incomingStatus)
      const tool = {
        ...old,
        ...Object.fromEntries(
          Object.entries(update).filter(
            ([key, value]) =>
              value != null ||
              ((key === 'rawInput' || key === 'rawOutput') && value === null),
          ),
        ),
        ...(preserveStatus ? { status: old!.status } : {}),
      } as ToolCall
      // New content supersedes a running null placeholder, not an explicit result.
      if (
        old?.rawOutput === null &&
        !isTerminalToolStatus(oldStatus) &&
        update.rawOutput === undefined &&
        update.content != null
      ) {
        delete tool.rawOutput
      }
      setBounded(this.tools, update.toolCallId, tool, TOOL_SNAPSHOT_LIMIT)
      const base = {
        type: 'tool_call' as const,
        id: `${this.prefix}:tool:${tool.toolCallId}`,
        callId: tool.toolCallId,
        name: cleanToolName(
          str(record(tool).name) ??
            tool.title ??
            str(record(old).name) ??
            old?.title ??
            'Tool',
        ),
        detail: toolDetail(tool),
      }
      const rawStatus = str((tool as { status?: unknown }).status)
      const isDenied = denied?.has(tool.toolCallId) === true
      const item: Extract<ProviderTimelineItem, { type: 'tool_call' }> =
        isDenied &&
        (tool.status === 'failed' ||
          rawStatus === 'cancelled' ||
          rawStatus === 'canceled')
          ? {
              ...base,
              status: 'canceled',
              error: null,
            }
          : tool.status === 'failed'
            ? {
                ...base,
                status: 'failed',
                error:
                  extractToolContentText(tool) ??
                  nonEmptyRawOutput(tool.rawOutput) ??
                  'Tool failed',
              }
            : rawStatus === 'cancelled' || rawStatus === 'canceled'
              ? {
                  ...base,
                  status: 'canceled',
                  error: null,
                }
              : {
                  ...base,
                  status: tool.status === 'completed' ? 'completed' : 'running',
                  error: null,
                }
      this.track(item)
      return item
    }
    if (update.sessionUpdate === 'plan')
      return {
        type: 'todo',
        id: `${this.prefix}:plan`,
        items: update.entries.map((e) => ({
          text: e.content,
          completed: e.status === 'completed',
          status: e.status,
        })),
      }
  }
}
