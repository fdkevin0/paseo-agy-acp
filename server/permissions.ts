import { z } from 'zod'
import type {
  ProviderToolCallDetail,
  ProviderToolPolicy,
} from '@getpaseo/plugin/server/provider'
import { cleanToolName, isAskQuestionName, record, str } from './mapping.ts'

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
  const title = cleanToolName(request.toolCall.title ?? '')
  if (isChooserRequest(request.options) || isAskQuestionName(title))
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
  /** Value agy expects back for this option (the schema `const`). */
  id: string
  label: string
  description?: string
}

export interface NormalizedQuestion {
  /** agy's schema property key; custom text goes to `${id}_other`. */
  id: string
  /** Paseo's QuestionCard keys each answer by header. */
  header: string
  question: string
  options: NormalizedQuestionOption[]
  multiSelect: boolean
  allowOther: boolean
}

type QuestionResponse = {
  behavior: 'allow' | 'deny'
  updatedInput?: Record<string, unknown>
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function withDescription(
  option: { id: string; label: string },
  description: unknown,
): NormalizedQuestionOption {
  const value = text(description)
  return value ? { ...option, description: value } : option
}

/** Parse agy's `elicitation/create` requestedSchema into normalized Paseo questions. */
export function normalizeElicitationQuestions(
  params: Record<string, unknown>,
): NormalizedQuestion[] {
  const properties = record(record(params.requestedSchema).properties)
  const keys = Object.keys(properties)
  if (keys.length === 0) {
    const message = text(params.message)
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
  // agy pairs each question `qN` with an optional free-form `qN_other` field.
  const ids = [...new Set(keys.map((key) => key.replace(/_other$/, '')))]
  return ids.filter(Boolean).map((id, index) => {
    const prop = record(properties[id])
    const other = record(properties[`${id}_other`])
    const multiSelect = prop.type === 'array'
    const choices = multiSelect ? record(prop.items).oneOf : prop.oneOf
    const options = (Array.isArray(choices) ? choices : []).map((choice, i) => {
      const c = record(choice)
      const value = c.const == null ? String(i) : String(c.const)
      return withDescription(
        { id: value, label: text(c.title) ?? value },
        c.description,
      )
    })
    const header = `Question ${index + 1}`
    const otherTitle = text(other.title)
    return {
      id,
      header,
      question:
        text(prop.title) ??
        text(prop.description) ??
        (otherTitle !== 'Other' ? otherTitle : undefined) ??
        text(other.description)?.replace(
          /^Optional free-form answer to:\s*/i,
          '',
        ) ??
        header,
      options,
      multiSelect,
      allowOther: `${id}_other` in properties,
    }
  })
}

/** Parse raw `ask_question` tool arguments into normalized Paseo questions. */
export function normalizeAskQuestionInput(
  rawInput: Record<string, unknown>,
): NormalizedQuestion[] {
  const items = Array.isArray(rawInput.questions)
    ? rawInput.questions
    : text(rawInput.question)
      ? [rawInput]
      : []
  return items.flatMap((item, index) => {
    if (!item || typeof item !== 'object') return []
    const q = record(item)
    const header = text(q.header) ?? `Question ${index + 1}`
    const options = (Array.isArray(q.options) ? q.options : []).flatMap(
      (option, i) => {
        const o = record(option)
        const label =
          typeof option === 'string'
            ? text(option)
            : (text(o.label) ?? text(o.title))
        return label
          ? [withDescription({ id: String(i), label }, o.description)]
          : []
      },
    )
    return [
      {
        id: text(q.id) ?? `q${index + 1}`,
        header,
        question: text(q.question) ?? header,
        options,
        multiSelect: Boolean(q.is_multi_select ?? q.multiSelect),
        allowOther: q.allowOther !== false,
      },
    ]
  })
}

/** Build the `input` payload expected by Paseo's QuestionCard UI. */
export function toQuestionPermissionInput(
  questions: readonly NormalizedQuestion[],
) {
  return {
    questions: questions.map((q) => ({
      header: q.header,
      question: q.question,
      options: q.options.map(({ label, description }) => ({
        label,
        ...(description ? { description } : {}),
      })),
      multiSelect: q.multiSelect,
      allowOther: q.allowOther,
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

/** Paseo's QuestionCard answers by header with one string; multi-select labels are comma-joined. */
function answerFor(response: QuestionResponse, header: string) {
  return text(record(record(response.updatedInput).answers)[header])
}

/**
 * Convert Paseo's QuestionCard response into agy's `elicitation/create` result.
 * `asked` are the questions the card showed, when a replayed schema is answered from an earlier card.
 */
export function buildElicitationResponse(
  questions: readonly NormalizedQuestion[],
  response: QuestionResponse,
  asked: readonly NormalizedQuestion[] = questions,
): { action: 'accept' | 'decline'; content?: Record<string, unknown> } {
  if (response.behavior === 'deny') return { action: 'decline' }
  const content: Record<string, unknown> = {}
  questions.forEach((q, index) => {
    const answer = answerFor(response, asked[index]?.header ?? q.header)
    if (!answer) return
    const byLabel = (label: string) => q.options.find((o) => o.label === label)
    if (!q.multiSelect) {
      const option = byLabel(answer)
      if (option) content[q.id] = option.id
      else content[`${q.id}_other`] = answer
      return
    }
    const { selected, custom } = splitMultiSelectAnswer(
      answer,
      q.options.map((o) => o.label),
    )
    const ids = selected.flatMap((label) => byLabel(label)?.id ?? [])
    if (ids.length > 0) content[q.id] = ids
    if (custom) content[`${q.id}_other`] = custom
  })
  return { action: 'accept', content }
}

/** Format the user's question response into human-readable text. */
export function formatQuestionAnswersText(
  questions: readonly NormalizedQuestion[],
  response: QuestionResponse,
): string {
  if (response.behavior === 'deny') return 'Declined'
  return questions
    .flatMap((q) => {
      const answer = answerFor(response, q.header)
      if (!answer) return []
      return [
        questions.length === 1 ? answer : `${q.question}\nAnswer: ${answer}`,
      ]
    })
    .join('\n\n')
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
