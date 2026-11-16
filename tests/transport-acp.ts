import { agent, methods, ndJsonStream } from '@agentclientprotocol/sdk/unstable'
import { spawn } from 'node:child_process'
import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { Readable, Writable } from 'node:stream'

process.stdin.on('end', () => {
  if (process.env.AGY_SHUTDOWN === 'stubborn') setInterval(() => {}, 1000)
  else process.exit(0)
})
agent()
  .onRequest(methods.agent.initialize, async () => ({
    protocolVersion: 1,
    agentCapabilities: {},
  }))
  .onRequest(methods.agent.authenticate, async () => ({}))
  .onRequest(methods.agent.session.new, async () => {
    // Native agy uses setsid for commands; the command can outlive ACP's EOF.
    const command = spawn(
      process.execPath,
      [
        '-e',
        "setTimeout(() => require('node:fs').writeFileSync(process.argv[1], 'leaked'), 700)",
        join(process.env.HOME!, 'old-result'),
      ],
      { detached: true, stdio: 'ignore' },
    )
    command.unref()
    writeFileSync(join(process.env.HOME!, 'command.pid'), String(command.pid))
    return { sessionId: '44444444-4444-4444-8444-444444444444' }
  })
  .onRequest(methods.agent.session.prompt, async () => ({
    stopReason: 'end_turn' as const,
  }))
  .onNotification(methods.agent.session.cancel, async () => {})
  .connect(
    ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)),
  )
