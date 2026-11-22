import { z } from 'zod'
import { resolveMcpToolIdentity } from './mcp.ts'
import type {
  ProviderToolCallDetail,
  ProviderToolPolicy,
} from '@getpaseo/plugin/server/provider'
import {
  classifyPlanModeTool,
  cleanToolName,
  detachString,
  isAskQuestionName,
  record,
  str,
  text,
  unwrapToolTitle,
} from './common.ts'

/** Validate the permission fields consumed by the bridge before creating pending work. */
export const RequestPermissionRequestSchema = z
  .object({
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
        z
          .object({
            optionId: z.string().min(1),
            name: z.string(),
            kind: z.enum([
              'allow_once',
              'allow_always',
              'reject_once',
              'reject_always',
            ]),
            _meta: z.record(z.string(), z.unknown()).nullish(),
          })
          .passthrough(),
      )
      .min(1),
    _meta: z.record(z.string(), z.unknown()).nullish(),
  })
  .passthrough()

/** Multiple options of the same allow_* kind represent a multiple-choice question rather than a binary approval. */
function isChooserRequest(options: ReadonlyArray<{ kind: string }>): boolean {
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
    toolCall: { toolCallId?: string; title?: string | null; kind?: unknown }
  },
  detail: ProviderToolCallDetail,
): 'question' | 'mode' | 'plan' | 'tool' {
  if (
    typeof request.toolCall.toolCallId === 'string' &&
    request.toolCall.toolCallId.startsWith('interaction_')
  )
    return 'question'
  const explicitRawName = str(record(request.toolCall).name)
  const identity =
    explicitRawName !== undefined
      ? cleanToolName(explicitRawName)
      : unwrapToolTitle(request.toolCall.title ?? '')
  if (isChooserRequest(request.options) || isAskQuestionName(identity))
    return 'question'
  if (
    classifyPlanModeTool(
      explicitRawName,
      request.toolCall.title,
      str(request.toolCall.kind),
    ) !== undefined
  )
    return 'mode'
  if (detail.type === 'plan') return 'plan'
  return 'tool'
}

const SECURITY_WARNING_META_KEY = 'agy.security.warning'
const SECURITY_WARNING_MAX_CHARS = 512

function formatSecurityWarning(raw: unknown): string | undefined {
  const direct = text(raw)
  if (direct !== undefined) return direct
  const obj = record(raw)
  const title = text(obj.title)
  const message = text(obj.message)
  if (title && message) return `${title}: ${message}`
  return title ?? message
}

/** Extract deduplicated agy.security.warning metadata across request, toolCall, and options. */
export function extractSecurityWarning(request: {
  _meta?: unknown
  toolCall?: { _meta?: unknown }
  options: ReadonlyArray<{ optionId: string; _meta?: unknown }>
}): { description: string; warnedOptionIds: ReadonlySet<string> } | undefined {
  const warnings = new Set<string>()
  const warnedOptionIds = new Set<string>()
  const requestWarning = formatSecurityWarning(
    record(request._meta)[SECURITY_WARNING_META_KEY],
  )
  if (requestWarning !== undefined) warnings.add(requestWarning)
  const toolCallWarning = formatSecurityWarning(
    record(request.toolCall?._meta)[SECURITY_WARNING_META_KEY],
  )
  if (toolCallWarning !== undefined) warnings.add(toolCallWarning)
  for (const option of request.options) {
    const optionWarning = formatSecurityWarning(
      record(option._meta)[SECURITY_WARNING_META_KEY],
    )
    if (optionWarning !== undefined) {
      warnings.add(optionWarning)
      warnedOptionIds.add(option.optionId)
    }
  }
  if (warnings.size === 0) return undefined
  const combined = [...warnings].join('\n')
  const chars = [...combined]
  const truncated =
    chars.length > SECURITY_WARNING_MAX_CHARS
      ? `${chars.slice(0, SECURITY_WARNING_MAX_CHARS - 1).join('')}…`
      : combined
  return {
    description: detachString(truncated),
    warnedOptionIds,
  }
}

/** Check whether an ACP tool call matches an exact MCP preapproval grant in config.toolPolicy. */
export function isPreapprovedMcpTool(
  call: { name?: unknown; title?: string | null; rawInput?: unknown },
  policy: ProviderToolPolicy | undefined,
  serverNames: readonly string[] = [],
): boolean {
  const identity = resolveMcpToolIdentity(call, serverNames)
  return (
    identity !== undefined &&
    policy?.preapproved.some(
      (grant) =>
        grant.kind === 'mcp' &&
        grant.server === identity.server &&
        grant.tool === identity.tool,
    ) === true
  )
}
