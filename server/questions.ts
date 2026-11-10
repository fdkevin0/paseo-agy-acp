import { record, text } from './common.ts'

interface NormalizedQuestionOption {
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

export type QuestionResponse = {
  behavior: 'allow' | 'deny'
  updatedInput?: Record<string, unknown>
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
    const items = record(prop.items)
    const target = multiSelect ? items : prop
    const choices = multiSelect
      ? (items.anyOf ?? items.oneOf)
      : (prop.oneOf ?? prop.anyOf)
    const options = Array.isArray(choices)
      ? choices.map((choice, i) => {
          const c = record(choice)
          const value = c.const == null ? String(i) : String(c.const)
          return withDescription(
            { id: value, label: text(c.title) ?? value },
            c.description,
          )
        })
      : Array.isArray(target.enum)
        ? target.enum.flatMap((entry) =>
            entry == null ? [] : [{ id: String(entry), label: String(entry) }],
          )
        : []
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
  const usedHeaders = new Set<string>()
  return items.flatMap((item, index) => {
    if (!item || typeof item !== 'object') return []
    const q = record(item)
    const baseHeader = text(q.header) ?? `Question ${index + 1}`
    const header = usedHeaders.has(baseHeader)
      ? `${baseHeader} (${index + 1})`
      : baseHeader
    usedHeaders.add(header)
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
function splitMultiSelectAnswer(
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
      else if (q.allowOther) content[`${q.id}_other`] = answer
      else content[q.id] = answer
      return
    }
    const { selected, custom } = splitMultiSelectAnswer(
      answer,
      q.options.map((o) => o.label),
    )
    const ids = selected.flatMap((label) => byLabel(label)?.id ?? [])
    if (ids.length > 0) content[q.id] = ids
    if (custom && q.allowOther) content[`${q.id}_other`] = custom
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
