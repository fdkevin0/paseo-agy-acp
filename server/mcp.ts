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
  serverNames: readonly string[] = [],
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
  // AGY 1.5.8 mcpcore.GetMcpToolNameWithPrefix (0x8804e60) joins mcp/server/tool
  // with single underscores; configured server names are needed to avoid guessing.
  const candidates = [...new Set(serverNames)].flatMap((server) => {
    const prefix = `mcp_${server}_`
    return server && name.startsWith(prefix) && name.length > prefix.length
      ? [{ server, tool: name.slice(prefix.length) }]
      : []
  })
  // Legacy adapter spellings; reject ambiguous boundaries rather than guessing.
  const parts = name.startsWith('mcp__')
    ? name.slice(5).split('__')
    : name.split(':')
  if (
    parts.length === 2 &&
    parts.every((part) => /^[a-zA-Z0-9_.-]+$/.test(part))
  )
    candidates.push({ server: parts[0], tool: parts[1] })
  return candidates.length === 1 ? candidates[0] : undefined
}

export function toolDisplayName(
  call: ToolIdentityInput,
  serverNames: readonly string[] = [],
): string {
  const mcp = resolveMcpToolIdentity(call, serverNames)
  return mcp
    ? `${mcp.server}.${mcp.tool}`
    : cleanToolName(str(call.name) ?? call.title ?? 'Tool')
}
