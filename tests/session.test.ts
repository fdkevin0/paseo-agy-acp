import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'

function manager(extraArgs: string[] = []) {
  return new SessionManager({
    launch: {
      command: process.execPath,
      args: [
        fileURLToPath(new URL('./mock-acp.ts', import.meta.url)),
        ...extraArgs,
      ],
      env: { PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: [],
    emit() {},
  })
}

function config(cwd: string, env: Record<string, string> = {}) {
  return { cwd, env, mcpServers: {}, settings: {}, persist: true }
}

test('warm runtimes reuse across workspaces unless launch or environment scopes differ', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-scopes-'))
  const m = manager()
  const rel = manager(['--gemini-dir=.gemini'])
  try {
    const other = join(root, 'other')
    await mkdir(other)
    const first = await m.start('a', root)
    await first.close(true)
    const second = await m.start('b', other)
    assert.equal(second.transport, first.transport)
    await second.close(true)
    const changed = await m.start('c', root, config(root, { CUSTOM: '1' }))
    assert.notEqual(changed.transport, first.transport)
    await changed.close(true)
    const reused = await m.start('d', root)
    assert.equal(reused.transport, first.transport)
    await reused.close(true)
    await assert.rejects(m.start('missing', join(root, 'missing')))
    const again = await m.start('e', root)
    assert.equal(again.transport, first.transport)

    const relFirst = await rel.start('r1', root)
    await relFirst.close(true)
    const relSecond = await rel.start('r2', other)
    assert.notEqual(relSecond.transport, relFirst.transport)
  } finally {
    await m.close()
    await rel.close()
    await rm(root, { recursive: true, force: true })
  }
})

test('warm pool evicts the oldest of four and never parks agent-specific environments', async () => {
  const m = manager()
  try {
    const sessions = []
    for (let i = 0; i < 5; i++) {
      const session = await m.start(
        String(i),
        process.cwd(),
        config(process.cwd(), { SLOT: String(i) }),
      )
      sessions.push(session)
      await session.close(true)
    }
    assert.equal(sessions[0].transport.closed, true)
    await sessions[0].transport.exited
    assert.ok(sessions.slice(1).every((s) => !s.transport.closed))
    const oneOff = await m.start(
      'agent',
      process.cwd(),
      config(process.cwd(), { PASEO_AGENT_ID: 'one-off' }),
    )
    await oneOff.close(true)
    assert.equal(oneOff.transport.closed, true)
    await oneOff.transport.exited
  } finally {
    await m.close()
  }
})

test('parked runtimes expire after sixty seconds', async (t) => {
  const m = manager()
  try {
    const session = await m.start('idle', process.cwd())
    t.mock.timers.enable({ apis: ['setTimeout'] })
    await session.close(true)
    t.mock.timers.tick(59_999)
    assert.equal(session.transport.closed, false)
    t.mock.timers.tick(1)
    assert.equal(session.transport.closed, true)
    t.mock.timers.reset()
    await session.transport.exited
  } finally {
    t.mock.timers.reset()
    await m.close()
  }
})

for (const parked of [false, true]) {
  test(
    `protocol disconnection kills a ${parked ? 'parked' : 'active'} child and shutdown waits for it`,
    { timeout: 5000 },
    async () => {
      const m = manager()
      try {
        const session = await m.start('broken', process.cwd())
        if (parked) await session.close(true)
        let exited = false
        void session.transport.exited.then(() => {
          exited = true
        })
        const prompt = session.transport.rpc.prompt({
          sessionId: 'disconnect-test',
          prompt: [{ type: 'text', text: 'disconnect' }],
        })
        await assert.rejects(prompt)
        assert.equal(session.transport.closed, true)
        if (!parked) assert.equal(session.state.failed, true)
        await m.close()
        assert.equal(exited, true)
      } finally {
        await m.close()
      }
    },
  )
}
