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

export const sessionIdPattern =
  /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i

/** Narrow opaque protocol payloads before reading vendor fields. */
export function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {}
}
function str(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}
function num(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}
/** Map native tool arguments and output to Paseo detail cards. */
export function toolDetail(
  tool: Partial<ToolCall> | ToolCallUpdate,
): ProviderToolCallDetail {
  const input = record(tool.rawInput)
  const output =
    typeof tool.rawOutput === 'string'
      ? tool.rawOutput
      : tool.content
          ?.map((c) =>
            c.type === 'content' && c.content.type === 'text'
              ? c.content.text
              : '',
          )
          .filter(Boolean)
          .join('\n') ||
        (tool.rawOutput != null ? JSON.stringify(tool.rawOutput) : undefined)
  const name = tool.title ?? ''
  const filePath =
    str(
      input.Path ??
        input.TargetFile ??
        input.AbsolutePath ??
        input.file_path ??
        input.path,
    ) ?? tool.locations?.[0]?.path
  const command = str(input.CommandLine ?? input.command)
  if (command)
    return { type: 'shell', command, cwd: str(input.Cwd ?? input.cwd), output }
  const url = str(input.Url ?? input.url)
  if ((name === 'read_url_content' || tool.kind === 'fetch') && url)
    return { type: 'fetch', url, ...(output ? { result: output } : {}) }
  if (name.includes('invoke_subagent') || name.includes('send_message')) {
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
  const diff = tool.content?.find((c) => c.type === 'diff')
  if (diff?.type === 'diff')
    return {
      type: 'edit',
      filePath: diff.path,
      oldString: diff.oldText ?? '',
      newString: diff.newText,
    }
  if (filePath) {
    if (/write/.test(name))
      return {
        type: 'write',
        filePath,
        content: str(input.Content ?? input.CodeContent ?? input.content),
      }
    if (tool.kind === 'edit' || /edit|replace/.test(name))
      return {
        type: 'edit',
        filePath,
        oldString: str(input.Target ?? input.TargetContent ?? input.old_string),
        newString: str(
          input.Replacement ?? input.ReplacementContent ?? input.new_string,
        ),
      }
    const start = num(input.StartLine)
    const end = num(input.EndLine)
    return {
      type: 'read',
      filePath,
      content: output,
      offset: num(
        input.offset ??
          input.StartLine ??
          input.ByteOffset ??
          input.ContentOffset,
      ),
      limit:
        num(input.limit) ??
        (start !== undefined && end !== undefined
          ? Math.max(1, end - start + 1)
          : undefined),
    }
  }
  const query = str(
    input.Query ??
      input.SearchDirectory ??
      input.Pattern ??
      input.query ??
      input.pattern,
  )
  if (query) return { type: 'search', query, content: output }
  return {
    type: 'plain_text',
    label: name,
    text: output ?? JSON.stringify(tool.rawInput ?? {}),
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
  if (content.type === 'text') return { type: 'text', text: content.text }
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

function nonnegative(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
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
  ): ProviderTimelineItem[] {
    const items = [...this.running.values()].map((item) =>
      status === 'failed'
        ? { ...item, status, error }
        : { ...item, status, error: null },
    )
    this.running.clear()
    return items
  }
  update(update: SessionUpdate): ProviderTimelineItem | undefined {
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
      const old = this.tools.get(update.toolCallId)
      const tool = {
        ...old,
        ...Object.fromEntries(
          Object.entries(update).filter(([, v]) => v != null),
        ),
      } as ToolCall
      this.tools.set(update.toolCallId, tool)
      const base = {
        type: 'tool_call' as const,
        id: `${this.prefix}:tool:${tool.toolCallId}`,
        callId: tool.toolCallId,
        name: tool.title ?? old?.title ?? 'Tool',
        detail: toolDetail(tool),
      }
      const item: Extract<ProviderTimelineItem, { type: 'tool_call' }> =
        tool.status === 'failed'
          ? {
              ...base,
              status: 'failed',
              error: JSON.stringify(tool.rawOutput ?? 'Tool failed'),
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
