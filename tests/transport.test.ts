import assert from 'node:assert/strict'
import { test } from 'node:test'
import { spawn } from 'node:child_process'
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises'
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
    const transport = connectAcp(
      {
        command: process.execPath,
        args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
        env: {
          HOME: home,
          PATH: join(home, 'no-such-bin'),
          AGY_SHUTDOWN: 'stubborn',
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
    try {
      await transport.rpc.initialize({
        protocolVersion: 1,
        clientCapabilities: {},
      })
      await transport.rpc.newSession({ cwd: home, mcpServers: [] })
      const commandPid = Number(
        await readFile(join(home, 'command.pid'), 'utf8'),
      )
      try {
        await transport.close()
        await transport.exited
      } finally {
        try {
          process.kill(-commandPid, 'SIGKILL')
        } catch {}
      }
    } finally {
      await transport.close()
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
        hung.request(new Promise(() => {}), {
          timeoutMs: 20,
          fatalOnTimeout: false,
        }),
        /ACP request timed out/,
      )
      assert.equal(timeoutExit, undefined)
      await assert.rejects(
        hung.request(new Promise(() => {})),
        /ACP request timed out/,
      )
      assert.equal(
        (timeoutExit as Error | undefined)?.message,
        'ACP request timed out',
      )
    } finally {
      await hung.close()
    }
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test(
  'transport shutdown aborts hanging ps and still meets termination budget',
  { skip: process.platform === 'win32' },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-transport-hangps-'))
    const binDir = join(home, 'bin')
    await mkdir(binDir, { recursive: true })
    const psPath = join(binDir, 'ps')
    const invokedPath = join(home, 'ps-invoked')
    await writeFile(
      psPath,
      `#!${process.execPath}\nrequire('node:fs').writeFileSync(${JSON.stringify(invokedPath)}, '1')\nsetInterval(() => {}, 60_000)\n`,
    )
    await chmod(psPath, 0o755)
    const transport = connectAcp(
      {
        command: process.execPath,
        args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: binDir, AGY_SHUTDOWN: 'stubborn' },
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
    try {
      await transport.rpc.initialize({
        protocolVersion: 1,
        clientCapabilities: {},
      })
      await transport.rpc.newSession({ cwd: home, mcpServers: [] })
      const commandPid = Number(
        await readFile(join(home, 'command.pid'), 'utf8'),
      )
      try {
        const started = Date.now()
        await transport.close()
        await transport.exited
        assert.ok(
          Date.now() - started < 1800,
          'expected close() with hanging ps to finish under 1800ms',
        )
        assert.equal(await readFile(invokedPath, 'utf8'), '1')
      } finally {
        try {
          process.kill(-commandPid, 'SIGKILL')
        } catch {}
      }
    } finally {
      await transport.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)
