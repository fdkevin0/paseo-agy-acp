import { z } from 'zod'
import type { ProviderToolPolicy } from '@getpaseo/plugin/server/provider'

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

/** Check whether an ACP tool call matches an exact MCP preapproval grant in config.toolPolicy. */
export function isPreapprovedMcpTool(
  title: string | null | undefined,
  input: Record<string, unknown>,
  policy: ProviderToolPolicy | undefined,
): boolean {
  if (!policy?.preapproved.length) return false
  const name = title ?? ''
  const lazyServer =
    typeof (input.ServerName ?? input.serverName ?? input.server) === 'string'
      ? String(input.ServerName ?? input.serverName ?? input.server)
      : undefined
  const lazyTool =
    typeof (input.ToolName ?? input.toolName ?? input.tool) === 'string'
      ? String(input.ToolName ?? input.toolName ?? input.tool)
      : undefined
  return policy.preapproved.some((grant) => {
    if (grant.kind !== 'mcp') return false
    if (
      (name === 'call_mcp_tool' || name.endsWith(':call_mcp_tool')) &&
      lazyServer === grant.server &&
      lazyTool === grant.tool
    )
      return true
    return (
      name === `mcp_${grant.server}_${grant.tool}` ||
      name === `mcp__${grant.server}__${grant.tool}` ||
      name === `${grant.server}/${grant.tool}` ||
      name === `${grant.server}:${grant.tool}` ||
      name === `${grant.server}.${grant.tool}`
    )
  })
}
