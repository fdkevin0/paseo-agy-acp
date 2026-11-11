import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

process.stdin.on('end', () => process.exit(0))
new AgentSideConnection(
  (client) => ({
    async initialize() {
      return { protocolVersion: 1, agentCapabilities: { loadSession: true } }
    },
    async authenticate() {
      return {}
    },
    async newSession() {
      return { sessionId: '11111111-1111-4111-8111-111111111111' }
    },
    async loadSession({ sessionId }) {
      await client.sessionUpdate({
        sessionId,
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [],
        },
      })
      const updates = JSON.parse(
        await readFile(join(process.env.HOME!, 'history-updates.json'), 'utf8'),
      )
      for (const update of updates)
        await client.sessionUpdate({ sessionId, update })
      return {}
    },
    async prompt() {
      return { stopReason: 'end_turn' as const }
    },
    async cancel() {},
  }),
  ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
)
