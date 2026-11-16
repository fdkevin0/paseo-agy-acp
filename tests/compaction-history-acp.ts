import { agent, methods, ndJsonStream } from '@agentclientprotocol/sdk/unstable'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

process.stdin.on('end', () => process.exit(0))
agent()
  .onRequest(methods.agent.initialize, async () => ({
    protocolVersion: 1,
    agentCapabilities: { loadSession: true },
  }))
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async () => ({
    sessionId: '11111111-1111-4111-8111-111111111111',
  }))
  .onRequest(
    methods.agent.session.load,
    async ({ params: { sessionId }, client }) => {
      await client.notify(methods.client.session.update, {
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
        await client.notify(methods.client.session.update, {
          sessionId,
          update,
        })
      return {}
    },
  )
  .onRequest(methods.agent.session.prompt, async ({ params }) => {
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
  })
  .onNotification(methods.agent.session.cancel, async () => {})
  .connect(
    ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
  )
