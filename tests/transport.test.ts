import assert from 'node:assert/strict'
import { test } from 'node:test'
import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import { connectAcp } from '../server/transport.ts'

for (const shutdown of ['graceful', 'stubborn']) {
  test(
    `transport ${shutdown} shutdown stops detached native commands without touching other processes`,
    { skip: process.platform === 'win32' },
    async () => {
      const home = await mkdtemp(join(tmpdir(), 'agy-transport-'))
      const unrelated = spawn(
        process.execPath,
        [
          '-e',
          "setTimeout(() => require('node:fs').writeFileSync(process.argv[1], 'survived'), 700)",
          join(home, 'unrelated-result'),
        ],
        { stdio: 'ignore' },
      )
      const transport = connectAcp(
        {
          command: process.execPath,
          args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
          env: {
            HOME: home,
            PATH: process.env.PATH ?? '',
            AGY_SHUTDOWN: shutdown,
          },
        },
        home,
        {},
        {
          sessionUpdate: async () => {},
          requestPermission: async () => ({
            outcome: { outcome: 'cancelled' },
          }),
        },
        () => {},
      )
      let commandPid: number | undefined
      try {
        await transport.rpc.initialize({
          protocolVersion: 1,
          clientCapabilities: {},
        })
        await transport.rpc.newSession({ cwd: home, mcpServers: [] })
        commandPid = Number(await readFile(join(home, 'command.pid'), 'utf8'))
        process.kill(commandPid, 0)
        const closedAt = Date.now()
        await transport.close()
        assert.ok(Date.now() - closedAt < 2500)
        await delay(750)
        assert.equal(
          await stat(join(home, 'old-result')).then(
            () => true,
            () => false,
          ),
          false,
          'detached command must not execute after ACP shutdown',
        )
        assert.equal(
          await readFile(join(home, 'unrelated-result'), 'utf8'),
          'survived',
        )
        if (process.platform === 'linux') {
          const commandState = await readFile(
            `/proc/${commandPid}/stat`,
            'utf8',
          ).catch(() => '')
          assert.ok(
            !commandState || /^\d+ \([^)]*\) Z /.test(commandState),
            'command is still running',
          )
        }
      } finally {
        await transport.close()
        unrelated.kill()
        if (commandPid) {
          try {
            process.kill(-commandPid, 'SIGKILL')
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
          }
        }
        await rm(home, { recursive: true, force: true })
      }
    },
  )
}

test(
  'transport shutdown without a usable ps still stops agy and resolves',
  { skip: process.platform === 'win32' },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-transport-nops-'))
    const path = process.env.PATH
    const transport = connectAcp(
      {
        command: process.execPath,
        args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: path ?? '', AGY_SHUTDOWN: 'stubborn' },
      },
      home,
      {},
      {
        sessionUpdate: async () => {},
        requestPermission: async () => ({
          outcome: { outcome: 'cancelled' },
        }),
      },
      () => {},
    )
    let commandPid: number | undefined
    try {
      await transport.rpc.initialize({
        protocolVersion: 1,
        clientCapabilities: {},
      })
      await transport.rpc.newSession({ cwd: home, mcpServers: [] })
      commandPid = Number(await readFile(join(home, 'command.pid'), 'utf8'))
      // Simulate slim images without procps: `ps` cannot be found.
      process.env.PATH = join(home, 'no-such-bin')
      await transport.close()
      process.env.PATH = path
      await transport.exited
    } finally {
      process.env.PATH = path
      await transport.close()
      if (commandPid) {
        try {
          process.kill(-commandPid, 'SIGKILL')
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
        }
      }
      await rm(home, { recursive: true, force: true })
    }
  },
)

test('transport surfaces stderr on immediate startup failure and notifies onExit on request timeout', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-transport-errors-'))
  try {
    let crashError: Error | undefined
    const crashing = connectAcp(
      {
        command: process.execPath,
        args: [
          '-e',
          'process.stderr.write("OAuth token expired\\n"); process.exit(2)',
        ],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      home,
      {},
      {
        sessionUpdate: async () => {},
        requestPermission: async () => ({
          outcome: { outcome: 'cancelled' },
        }),
      },
      (err) => {
        crashError = err
      },
    )
    await assert.rejects(
      crashing.rpc.initialize({ protocolVersion: 1, clientCapabilities: {} }),
    )
    await delay(80)
    assert.ok(crashError, 'expected onExit error from startup crash')
    assert.match(crashError.message, /code 2/)
    assert.match(crashError.message, /OAuth token expired/)
    await crashing.close()

    let timeoutExit: Error | undefined
    const hung = connectAcp(
      {
        command: process.execPath,
        args: ['-e', 'setInterval(() => {}, 1000)'],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      home,
      {},
      {
        sessionUpdate: async () => {},
        requestPermission: async () => ({
          outcome: { outcome: 'cancelled' },
        }),
      },
      (err) => {
        timeoutExit = err
      },
      { requestTimeoutMs: 60 },
    )
    try {
      await assert.rejects(
        hung.request(new Promise(() => {})),
        /ACP request timed out/,
      )
      assert.equal(timeoutExit?.message, 'ACP request timed out')
    } finally {
      await hung.close()
    }
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})
