import { cleanToolName, normalizeToolCandidate, record, str } from './common.ts'

type McpToolIdentity = { server: string; tool: string }

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
  const name = normalizeToolCandidate(str(call.name), call.title)
  if (name === 'call_mcp_tool') {
    const input = record(call.rawInput)
    const server = str(input.ServerName ?? input.serverName ?? input.server)
    const tool = str(input.ToolName ?? input.toolName ?? input.tool)
    return server && tool ? { server, tool } : undefined
  }
  // agy joins mcp/server/tool with single underscores; configured server names avoid guessing.
  const candidates = [...new Set(serverNames)].flatMap((server) => {
    const prefix = `mcp_${server}_`
    return server && name.startsWith(prefix) && name.length > prefix.length
      ? [{ server, tool: name.slice(prefix.length) }]
      : []
  })
  // Built-in Paseo MCP fallback applies only when explicit servers produced no match,
  // preserving explicit servers like `paseo_custom` while supporting history replay.
  if (
    candidates.length === 0 &&
    name.startsWith('mcp_paseo_') &&
    name.length > 'mcp_paseo_'.length
  ) {
    candidates.push({ server: 'paseo', tool: name.slice('mcp_paseo_'.length) })
  }
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
