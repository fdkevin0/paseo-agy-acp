import { cleanToolName, record, str, unwrapToolTitle } from './common.ts'

export type McpToolIdentity = { server: string; tool: string }

type ToolIdentityInput = {
  name?: unknown
  title?: string | null
  rawInput?: unknown
}

/** Resolve protocol identities only; display labels must never become permission keys. */
export function resolveMcpToolIdentity(
  call: ToolIdentityInput,
): McpToolIdentity | undefined {
  const explicit = str(call.name)
  const name =
    explicit !== undefined
      ? cleanToolName(explicit)
      : unwrapToolTitle(call.title ?? '')
  if (name.startsWith('contrib:')) return undefined
  // AGY 1.5.8 embeds: call_mcp_tool ... Judge by ServerName + ToolName + arguments.
  if (name === 'call_mcp_tool') {
    const input = record(call.rawInput)
    const server = str(input.ServerName ?? input.serverName ?? input.server)
    const tool = str(input.ToolName ?? input.toolName ?? input.tool)
    return server && tool ? { server, tool } : undefined
  }
  // Legacy adapter spellings; reject ambiguous boundaries rather than guessing.
  const match = name.startsWith('mcp__')
    ? name.slice(5).split('__')
    : name.split(':')
  if (
    match.length !== 2 ||
    !match.every((part) => /^[a-zA-Z0-9_.-]+$/.test(part))
  )
    return undefined
  return { server: match[0], tool: match[1] }
}

export function toolDisplayName(call: ToolIdentityInput): string {
  const mcp = resolveMcpToolIdentity(call)
  return mcp
    ? `${mcp.server}.${mcp.tool}`
    : cleanToolName(str(call.name) ?? call.title ?? 'Tool')
}
