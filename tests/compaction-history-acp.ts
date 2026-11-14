import { AgentSideConnection, ndJsonStream } from '@agentclientprotocol/sdk'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
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
    async prompt(params) {
      const text = params.prompt
        .flatMap((block) => (block.type === 'text' ? [block.text] : []))
        .join('')
      if (text === 'stage-and-cancel') {
        const dir = join(
          process.env.HOME!,
          '.gemini/antigravity-cli/brain',
          params.sessionId,
          '.system_generated/logs',
        )
        await mkdir(dir, { recursive: true })
        const staged = await readFile(
          join(process.env.HOME!, 'stage-transcript.jsonl'),
          'utf8',
        )
        await writeFile(join(dir, 'transcript_full.jsonl'), staged)
        return { stopReason: 'cancelled' as const }
      }
      if (text === 'cancel-only') return { stopReason: 'cancelled' as const }
      return { stopReason: 'end_turn' as const }
    },
    async cancel() {},
  }),
  ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
)
