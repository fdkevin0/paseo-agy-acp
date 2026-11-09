import { z } from 'zod'
import type {
  ProviderToolCallDetail,
  ProviderToolPolicy,
} from '@getpaseo/plugin/server/provider'
import { str } from './mapping.ts'

/** Validate the permission fields consumed by the bridge before creating pending work. */
export const RequestPermissionRequestSchema = z.object({
  sessionId: z.string().min(1),
  toolCall: z
    .object({
      toolCallId: z.string().min(1),
      title: z.string().nullish(),
      rawInput: z.json().optional(),
    })
    .passthrough(),
  options: z
    .array(
      z.object({
        optionId: z.string().min(1),
        name: z.string(),
        kind: z.enum([
          'allow_once',
          'allow_always',
          'reject_once',
          'reject_always',
        ]),
      }),
    )
    .min(1),
})

/** Multiple options of the same allow_* kind represent a multiple-choice question rather than a binary approval. */
export function isChooserRequest(
  options: ReadonlyArray<{ kind: string }>,
): boolean {
  const seen = new Set<string>()
  for (const option of options) {
    if (!option.kind.startsWith('allow')) continue
    if (seen.has(option.kind)) return true
    seen.add(option.kind)
  }
  return false
}

/** Select the matching ACP permission option for an allow/deny decision, preferring *_once when no optionId is specified. */
export function pickPermissionOption<
  T extends { optionId: string; kind: string },
>(
  options: readonly T[],
  behavior: 'allow' | 'deny',
  selectedActionId?: string,
): T | undefined {
  const prefix = behavior === 'allow' ? 'allow' : 'reject'
  if (selectedActionId) {
    return options.find(
      (o) => o.optionId === selectedActionId && o.kind.startsWith(prefix),
    )
  }
  const preferred = `${prefix}_once`
  return (
    options.find((o) => o.kind === preferred) ??
    options.find((o) => o.kind.startsWith(prefix))
  )
}

/** Classify an ACP permission request into Paseo's permission kinds. */
export function permissionKind(
  request: {
    options: ReadonlyArray<{ kind: string }>
    toolCall: { title?: string | null; kind?: unknown }
  },
  detail: ProviderToolCallDetail,
): 'question' | 'mode' | 'plan' | 'tool' {
  const title = (request.toolCall.title ?? '').replace(/^default_api:/, '')
  if (
    isChooserRequest(request.options) ||
    /^ask_(?:question|user)$/i.test(title)
  )
    return 'question'
  if (
    request.toolCall.kind === 'switch_mode' ||
    /^(?:switch_mode|exit_plan_mode)$/i.test(title)
  )
    return 'mode'
  if (detail.type === 'plan') return 'plan'
  return 'tool'
}

/** Check whether an ACP tool call matches an exact MCP preapproval grant in config.toolPolicy. */
export function isPreapprovedMcpTool(
  title: string | null | undefined,
  input: Record<string, unknown>,
  policy: ProviderToolPolicy | undefined,
): boolean {
  if (!policy?.preapproved.length) return false
  const name = title ?? ''
  const lazyServer = str(input.ServerName ?? input.serverName ?? input.server)
  const lazyTool = str(input.ToolName ?? input.toolName ?? input.tool)
  return policy.preapproved.some((grant) => {
    if (grant.kind !== 'mcp') return false
    if (
      (name === 'call_mcp_tool' || name.endsWith(':call_mcp_tool')) &&
      lazyServer === grant.server &&
      lazyTool === grant.tool
    )
      return true
    return (
      name === `mcp__${grant.server}__${grant.tool}` ||
      name === `${grant.server}:${grant.tool}`
    )
  })
}

export interface NormalizedQuestionOption {
  id: string
  label: string
  description?: string
}

export interface NormalizedQuestion {
  id: string
  header: string
  question: string
  options: NormalizedQuestionOption[]
  multiSelect: boolean
  allowOther: boolean
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

/** Parse agy's `elicitation/create` requestedSchema into normalized Paseo questions. */
export function normalizeElicitationQuestions(
  params: Record<string, unknown>,
): NormalizedQuestion[] {
  const schema = asRecord(params.requestedSchema)
  const properties = asRecord(schema?.properties)
  if (!properties) {
    const message =
      typeof params.message === 'string' ? params.message.trim() : ''
    return message
      ? [
          {
            id: 'q1',
            header: 'Question 1',
            question: message,
            options: [],
            multiSelect: false,
            allowOther: true,
          },
        ]
      : []
  }

  const baseKeys: string[] = []
  for (const key of Object.keys(properties)) {
    const base = key.endsWith('_other') ? key.slice(0, -6) : key
    if (base && !baseKeys.includes(base)) baseKeys.push(base)
  }

  const questions: NormalizedQuestion[] = []
  for (const [idx, baseKey] of baseKeys.entries()) {
    const prop = asRecord(properties[baseKey])
    const otherProp = asRecord(properties[`${baseKey}_other`])
    if (!prop && !otherProp) continue
    const header = `Question ${idx + 1}`
    if (prop) {
      const multiSelect = prop.type === 'array'
      const items = asRecord(prop.items)
      const rawChoices = multiSelect
        ? Array.isArray(items?.oneOf)
          ? items.oneOf
          : []
        : Array.isArray(prop.oneOf)
          ? prop.oneOf
          : []
      const options: NormalizedQuestionOption[] = rawChoices.flatMap(
        (choice, optIdx) => {
          const rec = asRecord(choice)
          if (!rec) return []
          const id =
            rec.const !== undefined && rec.const !== null
              ? String(rec.const)
              : String(optIdx)
          const label =
            typeof rec.title === 'string' && rec.title ? rec.title : id
          const description =
            typeof rec.description === 'string' && rec.description
              ? rec.description
              : undefined
          return [{ id, label, ...(description ? { description } : {}) }]
        },
      )
      const question =
        typeof prop.title === 'string' && prop.title
          ? prop.title
          : typeof prop.description === 'string' && prop.description
            ? prop.description
            : header
      questions.push({
        id: baseKey,
        header,
        question,
        options,
        multiSelect,
        allowOther: Boolean(otherProp),
      })
    } else if (otherProp) {
      const question =
        typeof otherProp.title === 'string' &&
        otherProp.title &&
        otherProp.title !== 'Other'
          ? otherProp.title
          : typeof otherProp.description === 'string' && otherProp.description
            ? otherProp.description.replace(
                /^Optional free-form answer to:\s*/i,
                '',
              )
            : header
      questions.push({
        id: baseKey,
        header,
        question,
        options: [],
        multiSelect: false,
        allowOther: true,
      })
    }
  }
  return questions
}

/** Parse raw `ask_question` tool arguments into normalized Paseo questions. */
export function normalizeAskQuestionInput(
  rawInput: Record<string, unknown>,
): NormalizedQuestion[] {
  const rawQuestions = Array.isArray(rawInput.questions)
    ? rawInput.questions
    : typeof rawInput.question === 'string'
      ? [rawInput]
      : []
  return rawQuestions.flatMap((item, idx) => {
    const rec = asRecord(item)
    if (!rec) return []
    const id =
      typeof rec.id === 'string' && rec.id.trim()
        ? rec.id.trim()
        : `q${idx + 1}`
    const header =
      typeof rec.header === 'string' && rec.header.trim()
        ? rec.header.trim()
        : `Question ${idx + 1}`
    const question =
      typeof rec.question === 'string' && rec.question.trim()
        ? rec.question.trim()
        : header
    const rawOptions = Array.isArray(rec.options) ? rec.options : []
    const options: NormalizedQuestionOption[] = rawOptions.flatMap(
      (opt, optIdx) => {
        if (typeof opt === 'string') {
          const text = opt.trim()
          return text ? [{ id: String(optIdx), label: text }] : []
        }
        const optRec = asRecord(opt)
        if (!optRec) return []
        const optId =
          typeof optRec.id === 'string' && optRec.id
            ? optRec.id
            : typeof optRec.const === 'string' && optRec.const
              ? optRec.const
              : String(optIdx)
        const label =
          typeof optRec.label === 'string' && optRec.label
            ? optRec.label
            : typeof optRec.title === 'string' && optRec.title
              ? optRec.title
              : optId
        const description =
          typeof optRec.description === 'string' && optRec.description
            ? optRec.description
            : undefined
        return [{ id: optId, label, ...(description ? { description } : {}) }]
      },
    )
    const multiSelect = Boolean(rec.is_multi_select ?? rec.multiSelect)
    const allowOther = rec.allowOther !== false && rec.isOther !== false
    return [{ id, header, question, options, multiSelect, allowOther }]
  })
}

/** Build the `input` payload expected by Paseo's QuestionCard UI. */
export function toQuestionPermissionInput(questions: NormalizedQuestion[]) {
  return {
    questions: questions.map((q) => ({
      id: q.id,
      header: q.header,
      question: q.question,
      options: q.options.map((o) => ({
        label: o.label,
        ...(o.description ? { description: o.description } : {}),
      })),
      multiSelect: q.multiSelect,
      allowOther: q.allowOther,
      isOther: q.allowOther,
    })),
  }
}

/**
 * Split a comma-joined multi-select answer string into known option labels and
 * optional custom "Other" text, matching Paseo's longest-label-first parser.
 */
export function splitMultiSelectAnswer(
  value: string,
  optionLabels: string[],
): { selected: string[]; custom?: string } {
  let remaining = value.trim()
  if (!remaining) return { selected: [] }
  const candidates = Array.from(
    new Set(
      optionLabels
        .map((label) => label.trim())
        .filter((label) => label.length > 0),
    ),
  ).sort((a, b) => b.length - a.length)
  const selected: string[] = []
  const unmatched: string[] = []
  while (remaining.length > 0) {
    const matched = candidates.find(
      (candidate) =>
        remaining === candidate || remaining.startsWith(`${candidate}, `),
    )
    if (matched) {
      selected.push(matched)
      if (remaining === matched) break
      remaining = remaining.slice(matched.length + 2).trimStart()
      continue
    }
    const separatorIndex = remaining.indexOf(', ')
    if (separatorIndex === -1) {
      unmatched.push(remaining)
      break
    }
    const token = remaining.slice(0, separatorIndex).trim()
    if (token) unmatched.push(token)
    remaining = remaining.slice(separatorIndex + 2).trimStart()
  }
  return {
    selected,
    ...(unmatched.length > 0 ? { custom: unmatched.join(', ') } : {}),
  }
}

function lookupRawAnswer(
  q: NormalizedQuestion,
  idx: number,
  questions: readonly NormalizedQuestion[],
  answers: Record<string, unknown> | undefined,
  selectedActionId?: string,
  source?: NormalizedQuestion,
): unknown {
  if (answers) {
    const candidateKeys = [
      q.header,
      q.question,
      q.id,
      source?.header,
      source?.question,
      source?.id,
      `Question ${idx + 1}`,
      `q${idx + 1}`,
    ]
    for (const key of candidateKeys) {
      if (key && answers[key] !== undefined) return answers[key]
    }
    if (questions.length === 1) {
      const values = Object.values(answers)
      if (values.length === 1) return values[0]
    }
  }
  return selectedActionId
}

function matchQuestionOption(
  q: NormalizedQuestion,
  value: string,
  source?: NormalizedQuestion,
): NormalizedQuestionOption | undefined {
  const trimmed = value.trim()
  const direct = q.options.find(
    (o) =>
      o.label === value ||
      o.id === value ||
      o.label.trim() === trimmed ||
      o.id === trimmed,
  )
  if (direct) return direct
  if (source && source.options.length === q.options.length) {
    const sourceIndex = source.options.findIndex(
      (o) =>
        o.label === value ||
        o.id === value ||
        o.label.trim() === trimmed ||
        o.id === trimmed,
    )
    if (sourceIndex >= 0) return q.options[sourceIndex]
  }
  return undefined
}

/**
 * Convert Paseo's `session.permission` response into agy's `elicitation/create`
 * result (`{ action: 'accept', content }` or `{ action: 'decline' }`).
 */
export function buildElicitationResponse(
  questions: NormalizedQuestion[],
  response: {
    behavior: 'allow' | 'deny'
    selectedActionId?: string
    updatedInput?: Record<string, unknown>
  },
  sourceQuestions?: readonly NormalizedQuestion[],
): {
  action: 'accept' | 'decline' | 'cancel'
  content?: Record<string, unknown>
} {
  if (response.behavior === 'deny') return { action: 'decline' }
  const updated = asRecord(response.updatedInput)
  const answers = asRecord(updated?.answers)
  const content: Record<string, unknown> = {}

  for (const [idx, q] of questions.entries()) {
    const source = sourceQuestions?.[idx]
    const key = q.id
    const otherKey = `${q.id}_other`
    const raw = lookupRawAnswer(
      q,
      idx,
      questions,
      answers,
      response.selectedActionId,
      source,
    )
    if (q.options.length === 0) {
      if (typeof raw === 'string' && raw.trim()) {
        content[otherKey] = raw.trim()
      }
      continue
    }
    if (!q.multiSelect) {
      if (typeof raw === 'string' && raw.trim()) {
        const trimmed = raw.trim()
        const matched = matchQuestionOption(q, raw, source)
        if (matched) content[key] = matched.id
        else content[otherKey] = trimmed
      }
      continue
    }
    const selectedIds: string[] = []
    const customParts: string[] = []
    if (Array.isArray(raw)) {
      for (const entry of raw) {
        if (typeof entry !== 'string' || !entry.trim()) continue
        const trimmed = entry.trim()
        const matched = matchQuestionOption(q, entry, source)
        if (matched) selectedIds.push(matched.id)
        else customParts.push(trimmed)
      }
    } else if (typeof raw === 'string' && raw.trim()) {
      const { selected, custom } = splitMultiSelectAnswer(
        raw,
        q.options.map((o) => o.label),
      )
      for (const label of selected) {
        const matched = matchQuestionOption(q, label, source)
        if (matched) selectedIds.push(matched.id)
      }
      if (custom?.trim()) {
        const byId = matchQuestionOption(q, custom.trim(), source)
        if (byId && selectedIds.length === 0) selectedIds.push(byId.id)
        else customParts.push(custom.trim())
      }
    }
    if (selectedIds.length > 0) content[key] = selectedIds
    if (customParts.length > 0) content[otherKey] = customParts.join(', ')
  }

  return { action: 'accept', content }
}

/** Format the user's question response into human-readable text. */
export function formatQuestionAnswersText(
  questions: NormalizedQuestion[],
  response: {
    behavior: 'allow' | 'deny'
    selectedActionId?: string
    updatedInput?: Record<string, unknown>
  },
): string {
  if (response.behavior === 'deny') return 'Declined'
  const updated = asRecord(response.updatedInput)
  const answers = asRecord(updated?.answers)
  const lines: string[] = []
  for (const [idx, q] of questions.entries()) {
    const raw = lookupRawAnswer(
      q,
      idx,
      questions,
      answers,
      response.selectedActionId,
    )
    const text = Array.isArray(raw)
      ? raw.map(String).join(', ').trim()
      : typeof raw === 'string'
        ? raw.trim()
        : ''
    if (!text) continue
    const byId = q.options.find((o) => o.id === text)
    const display = byId ? byId.label : text
    lines.push(
      questions.length === 1 ? display : `${q.question}\nAnswer: ${display}`,
    )
  }
  return lines.join('\n\n')
}

/** Verify that a replayed elicitation/create schema asks the exact same questions and options. */
export function sameNormalizedQuestions(
  a: readonly NormalizedQuestion[],
  b: readonly NormalizedQuestion[],
): boolean {
  if (a.length === 0 || a.length !== b.length) return false
  return a.every((q, i) => {
    const other = b[i]
    return (
      q.question.trim() === other.question.trim() &&
      q.multiSelect === other.multiSelect &&
      q.options.length === other.options.length &&
      q.options.every(
        (opt, j) => opt.label.trim() === other.options[j]?.label.trim(),
      )
    )
  })
}
