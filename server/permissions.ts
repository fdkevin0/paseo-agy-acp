import { z } from 'zod'
import { resolveMcpToolIdentity } from './mcp.ts'
import type {
  ProviderToolCallDetail,
  ProviderToolPolicy,
} from '@getpaseo/plugin/server/provider'
import {
  classifyPlanModeTool,
  cleanToolName,
  isAskQuestionName,
  record,
  str,
  unwrapToolTitle,
} from './common.ts'

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
    toolCall: { title?: string | null; kind?: unknown }
  },
  detail: ProviderToolCallDetail,
): 'question' | 'mode' | 'plan' | 'tool' {
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

/** Check whether an ACP tool call matches an exact MCP preapproval grant in config.toolPolicy. */
export function isPreapprovedMcpTool(
  call: { name?: unknown; title?: string | null; rawInput?: unknown },
  policy: ProviderToolPolicy | undefined,
): boolean {
  const identity = resolveMcpToolIdentity(call)
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
