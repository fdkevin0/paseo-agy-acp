import assert from 'node:assert/strict'
import { once } from 'node:events'
import { test } from 'node:test'
import { spawn } from 'node:child_process'
import { readFileSync } from 'node:fs'
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
import { connectAcp } from '../server/transport.ts'
import {
  isolatedEnv,
  killProcessGroupIgnoreMissing,
  waitForPidExit,
} from './helpers.ts'

async function runShutdownTest(shutdown: 'graceful' | 'stubborn') {
  const home = await mkdtemp(join(tmpdir(), 'agy-transport-'))
  const unrelated = spawn(
    process.execPath,
    [
      '-e',
      "setTimeout(() => require('node:fs').writeFileSync(process.argv[1], 'survived'), 700)",
      join(home, 'unrelated-result'),
    ],
    { stdio: 'ignore', env: isolatedEnv(home) },
  )
  const unrelatedExited = once(unrelated, 'exit')
  const transport = connectAcp(
    {
      command: process.execPath,
      args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
      env: isolatedEnv(home, {
        PATH: '/usr/bin:/bin',
        AGY_SHUTDOWN: shutdown,
      }),
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
    await transport.close()
    await waitForPidExit(commandPid!)
    await unrelatedExited
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
}

test(
  'transport graceful shutdown stops detached native commands without touching other processes',
  { skip: process.platform === 'win32', timeout: 5000 },
  () => runShutdownTest('graceful'),
)

test(
  'transport stubborn shutdown stops detached native commands without touching other processes',
  { skip: process.platform === 'win32', timeout: 5000 },
  () => runShutdownTest('stubborn'),
)

test(
  'transport shutdown without a usable ps still stops agy and resolves',
  { skip: process.platform === 'win32' },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-transport-nops-'))
    const transport = connectAcp(
      {
        command: process.execPath,
        args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
        env: isolatedEnv(home, {
          PATH: join(home, 'no-such-bin'),
          AGY_SHUTDOWN: 'stubborn',
        }),
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
        killProcessGroupIgnoreMissing(commandPid)
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
    let resolveExit!: (err: Error) => void
    const exited = new Promise<Error>((resolve) => {
      resolveExit = resolve
    })
    const crashing = connectAcp(
      {
        command: process.execPath,
        args: [
          '-e',
          'process.stderr.write("OAuth token expired\\n"); process.exit(2)',
        ],
        env: isolatedEnv(home),
      },
      home,
      {},
      {
        sessionUpdate: async () => {},
        requestPermission: async () => ({
          outcome: { outcome: 'cancelled' },
        }),
      },
      resolveExit,
    )
    await assert.rejects(
      crashing.rpc.initialize({ protocolVersion: 1, clientCapabilities: {} }),
    )
    const crashError = await exited
    assert.match(crashError.message, /code 2/)
    assert.match(crashError.message, /OAuth token expired/)
    await crashing.close()

    let timeoutExit: Error | undefined
    const hung = connectAcp(
      {
        command: process.execPath,
        args: ['-e', 'setInterval(() => {}, 1000)'],
        env: isolatedEnv(home),
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

function killBestEffort(pid: number): void {
  try {
    process.kill(pid, 'SIGKILL')
  } catch {}
}

function killPidFileBestEffort(path: string): void {
  try {
    process.kill(Number(readFileSync(path, 'utf8')), 'SIGKILL')
  } catch {}
}

test(
  'transport shutdown aborts hanging ps and still meets termination budget',
  { skip: process.platform === 'win32' },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-transport-hangps-'))
    const binDir = join(home, 'bin')
    await mkdir(binDir, { recursive: true })
    const psPath = join(binDir, 'ps')
    const invokedPath = join(home, 'ps-invoked')
    await writeFile(
      psPath,
      `#!/bin/sh\ntrap '' TERM\nprintf '%s' "$$" > ${JSON.stringify(`${invokedPath}.tmp`)} && mv ${JSON.stringify(`${invokedPath}.tmp`)} ${JSON.stringify(invokedPath)}\nexec sleep 60\n`,
    )
    await chmod(psPath, 0o755)
    const transport = connectAcp(
      {
        command: process.execPath,
        args: [fileURLToPath(new URL('./transport-acp.ts', import.meta.url))],
        env: isolatedEnv(home, {
          PATH: `${binDir}:/usr/bin:/bin`,
          AGY_SHUTDOWN: 'stubborn',
        }),
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
    t.after(async () => {
      killPidFileBestEffort(invokedPath)
      await transport.close()
      await rm(home, { recursive: true, force: true })
    })
    await transport.rpc.initialize({
      protocolVersion: 1,
      clientCapabilities: {},
    })
    await transport.rpc.newSession({ cwd: home, mcpServers: [] })
    const commandPid = Number(await readFile(join(home, 'command.pid'), 'utf8'))
    t.after(() => killBestEffort(-commandPid))
    await transport.close()
    await transport.exited
    const psPid = Number(await readFile(invokedPath, 'utf8'))
    assert.ok(Number.isInteger(psPid) && psPid > 0)
    await waitForPidExit(psPid)
  },
)

test(
  'transport request idleTimeoutMs resets on partial wire bytes while enforcing silence and maxTimeoutMs',
  { timeout: 5000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-transport-idle-'))
    const childScript = `
    const readline = require('node:readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      const msg = JSON.parse(line);
      if (msg.method === 'initialize') {
        const body = JSON.stringify({
          jsonrpc: '2.0',
          id: msg.id,
          result: { protocolVersion: 1, agentCapabilities: {} },
        }) + '\\n';
        const parts = [
          body.slice(0, 12),
          body.slice(12, 24),
          body.slice(24, 40),
          body.slice(40, 56),
          body.slice(56, 72),
          body.slice(72),
        ];
        let i = 0;
        const send = () => {
          process.stdout.write(parts[i++]);
          if (i < parts.length) setTimeout(send, 70);
        };
        send();
      } else if (msg.method === 'session/prompt') {
        process.stdout.write('{"jsonrpc":"2.0"');
        setInterval(() => process.stdout.write(' '), 30);
      }
    });
  `
    let exitError: Error | undefined
    const transport = connectAcp(
      {
        command: process.execPath,
        args: ['-e', childScript],
        env: isolatedEnv(home),
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
        exitError = err
      },
    )
    t.after(async () => {
      await transport.close()
      await rm(home, { recursive: true, force: true })
    })

    // Partial chunks span ~350ms (> 250ms idleTimeoutMs) before newline completes the frame.
    const init = await transport.request(
      transport.rpc.initialize({ protocolVersion: 1, clientCapabilities: {} }),
      { idleTimeoutMs: 250, maxTimeoutMs: 3000 },
    )
    assert.equal(init.protocolVersion, 1)
    assert.equal(exitError, undefined)

    // Complete silence times out at idleTimeoutMs well before maxTimeoutMs.
    await assert.rejects(
      transport.request(
        transport.rpc.newSession({ cwd: home, mcpServers: [] }),
        { idleTimeoutMs: 80, maxTimeoutMs: 5000, fatalOnTimeout: false },
      ),
      /ACP request timed out/,
    )
    assert.equal(exitError, undefined)

    // Continuous partial bytes keep resetting idleTimeoutMs until maxTimeoutMs ceiling hits.
    await assert.rejects(
      transport.request(transport.rpc.prompt({ sessionId: 's1', prompt: [] }), {
        idleTimeoutMs: 150,
        maxTimeoutMs: 220,
      }),
      /ACP request timed out/,
    )
    assert.equal(
      (exitError as Error | undefined)?.message,
      'ACP request timed out',
    )
  },
)
