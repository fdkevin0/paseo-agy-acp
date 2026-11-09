import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
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

test(
  'protocol disconnection kills an active child and shutdown waits for it',
  { timeout: 5000 },
  async () => {
    const m = manager()
    try {
      const session = await m.start('broken', process.cwd())
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
      assert.equal(session.state.failed, true)
      await m.close()
      assert.equal(exited, true)
    } finally {
      await m.close()
    }
  },
)

test(
  'stall failure interrupts the native session and rejects between-turn permissions, while ERROR_MESSAGE failure does not interrupt',
  { timeout: 5000 },
  async () => {
    const home = await mkdtemp(join(tmpdir(), 'agy-session-stall-'))
    const events: ProviderEvent[] = []
    const m = new SessionManager({
      launch: {
        command: process.execPath,
        args: [fileURLToPath(new URL('./interrupt-acp.ts', import.meta.url))],
        env: { HOME: home, PATH: process.env.PATH ?? '' },
      },
      usage: new AgyUsageSource(),
      capabilities: ['prompt.message', 'permission', 'session.configure'],
      emit(event) {
        events.push(event)
      },
      stallTimeoutMs: 30,
    })
    const waitTurn = async (state: 'completed' | 'failed' | 'canceled') => {
      const deadline = Date.now() + 2000
      while (
        !events.some((e) => e.type === 'session.turn' && e.state === state)
      ) {
        assert.ok(Date.now() < deadline, JSON.stringify(events))
        await delay(5)
      }
    }
    const readRuntimeLog = async () =>
      (await readFile(join(home, 'runtime.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
    try {
      await m.handle({
        type: 'session.open',
        requestId: 'open',
        sessionId: 's',
        history: 'skip',
        config: {
          cwd: home,
          env: {},
          mcpServers: {},
          settings: {},
          persist: true,
          mode: 'full-access',
        },
      })
      // 1. Native ERROR_MESSAGE fails the turn without calling interrupt()
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'err-1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'error-step' }],
          },
        },
      })
      await waitTurn('failed')
      const afterErrorLog = await readRuntimeLog()
      assert.equal(
        afterErrorLog.some((r) => r.event === 'cancel'),
        false,
        'non-stall ERROR_MESSAGE must not interrupt the session',
      )

      // 2. Stall failure interrupts the native session, and late permission after turn end is canceled even in full-access mode
      events.length = 0
      await m.handle({
        type: 'session.prompt',
        sessionId: 's',
        prompt: {
          clientMessageId: 'stall-1',
          delivery: 'auto',
          input: {
            type: 'message',
            content: [{ type: 'text', text: 'stall' }],
          },
        },
      })
      await waitTurn('failed')
      await m.handle({
        type: 'session.configure',
        requestId: 'cfg-after-stall',
        sessionId: 's',
        changes: { thinkingOption: 'high' },
      })
      const afterStallLog = await readRuntimeLog()
      assert.equal(
        afterStallLog.some((r) => r.event === 'cancel'),
        true,
        'stalled turn must interrupt the native session',
      )
      assert.deepEqual(
        afterStallLog.find((r) => r.event === 'late-permission')?.outcome,
        { outcome: 'cancelled' },
        'permission arriving when !state.turn must be canceled even in full-access mode',
      )
    } finally {
      await m.close()
      await rm(home, { recursive: true, force: true })
    }
  },
)
