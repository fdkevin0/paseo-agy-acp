import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { ProviderEvent } from '@getpaseo/plugin/server/provider'
import { SessionManager } from '../server/session.ts'
import { AgyUsageSource } from '../server/usage.ts'
import { extractStepUsage, readLatestStepUsage } from '../server/brain-io.ts'

async function harness(t: TestContext, restoring = false) {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-regression-'))
  const events: ProviderEvent[] = []
  const m = new SessionManager({
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: { HOME: home, PATH: process.env.PATH ?? '' },
    },
    usage: new AgyUsageSource(),
    capabilities: ['prompt.message', 'session.configure'],
    emit: (e) => events.push(structuredClone(e)),
  })
  t.after(async () => {
    await m.close()
    await rm(home, { recursive: true, force: true })
  })
  const start = m.start.bind(m)
  let session!: Awaited<ReturnType<typeof m.start>>
  t.mock.method(
    m,
    'start',
    async (...args: Parameters<typeof start>) =>
      (session = await start(...args)),
  )
  const native = '11111111-1111-4111-8111-111111111111'
  let path = join(
    home,
    '.gemini/antigravity-cli/brain',
    native,
    '.system_generated/logs/transcript_full.jsonl',
  )
  const write = async (rows: object[], partial = '') => {
    await mkdir(join(path, '..'), { recursive: true })
    await writeFile(
      path,
      rows.map((r) => JSON.stringify(r) + '\n').join('') + partial,
    )
  }
  if (restoring)
    await write([
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        input_tokens: 70,
        output_tokens: 3,
      },
    ])
  await m.open({
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
      ...(restoring ? { model: 'gpt-oss-120b-medium' } : {}),
    },
    ...(restoring
      ? { persistence: { version: 1, data: { sessionId: native } } }
      : {}),
  })
  if (!restoring)
    path = join(
      home,
      '.gemini/antigravity-cli/brain',
      session.state.nativeId,
      '.system_generated/logs/transcript_full.jsonl',
    )
  let sequence = 0
  const prompt = (text = 'synthetic') =>
    m.handle({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: `p${++sequence}`,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  const latest = () => {
    const event = events.findLast((e) => e.type === 'session.usage')
    assert.ok(event?.type === 'session.usage')
    return event.usage
  }
  return { m, session, events, write, path, prompt, latest, home }
}

for (const throws of [false, true])
  test(`cancel snapshot follows interrupt work and advances cross-turn boundary (throw: ${throws})`, async (t) => {
    const h = await harness(t)
    const original = [
      { step_index: 0, type: 'USER_INPUT', content: 'work' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        input_tokens: 20,
        output_tokens: 1,
      },
    ]
    const flushed = [
      ...original,
      {
        step_index: 2,
        type: 'PLANNER_RESPONSE',
        input_tokens: 70207,
        output_tokens: 256,
      },
      { step_index: 3, type: 'GENERIC', status: 'CANCELED' },
    ]
    t.mock.method(h.session.transport.rpc, 'prompt', async () => {
      await h.write(original)
      if (!throws) h.session.state.turnAbort?.abort()
      // Controlled local interruption work (e.g. relaunch) completes on await; ACP cancel itself has no ACK.
      h.session.state.interrupting = {
        then: (done: () => void, error: (e: unknown) => void) =>
          h.write(flushed).then(done, error),
      } as Promise<void>
      if (throws) throw new Error('Synthetic RPC failure')
      return {
        stopReason: 'cancelled' as const,
        usage: { inputTokens: 600000, outputTokens: 4405, totalTokens: 604405 },
      }
    })
    await h.prompt()
    assert.equal(h.latest().contextWindowUsedTokens, 70463)
    if (!throws)
      assert.equal(
        h.latest().inputTokens,
        600000,
        'explicit cancellation keeps reported ACP consumption',
      )
    assert.equal(h.session.state.lastKnownStepIndex, 3)
    h.session.state.interrupting = null
    await h.write([])
    const count = h.events.filter((e) => e.type === 'session.usage').length
    t.mock.method(h.session.transport.rpc, 'prompt', async () => {
      await h.write(flushed)
      return { stopReason: 'cancelled' as const }
    })
    await h.prompt()
    assert.equal(
      h.events.filter((e) => e.type === 'session.usage').length,
      count,
      'an empty next-turn baseline cannot republish the canceled turn as new usage',
    )
  })

test('snapshot-only cancellation preserves consumption and wakeup debt for the next bill', async (t) => {
  const h = await harness(t)
  h.session.state.wakeup.carriedUsage = {
    inputTokens: 80,
    outputTokens: 8,
    cachedReadTokens: 4,
  }
  h.session.publishUsage({
    inputTokens: 180,
    outputTokens: 18,
    cachedInputTokens: 4,
  })
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write([
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        input_tokens: 20,
        output_tokens: 2,
      },
    ])
    return { stopReason: 'cancelled' as const }
  })
  await h.prompt()
  assert.deepEqual(h.session.state.wakeup.carriedUsage, {
    inputTokens: 80,
    outputTokens: 8,
    cachedReadTokens: 4,
  })
  assert.equal(h.latest().inputTokens, 180)
  assert.equal(h.latest().outputTokens, 18)
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'cancelled' as const,
    usage: {
      inputTokens: 100,
      outputTokens: 10,
      cachedReadTokens: 4,
      totalTokens: 114,
    },
  }))
  await h.prompt()
  assert.equal(h.latest().inputTokens, 20)
  assert.equal(h.latest().outputTokens, 2)
})

test('restoration publishes context only after applying the selected model', async (t) => {
  const h = await harness(t, true)
  assert.equal(h.latest().contextWindowUsedTokens, 73)
  assert.equal(h.latest().contextWindowMaxTokens, 131072)
  assert.equal(h.latest().inputTokens, undefined)
  assert.equal(h.latest().outputTokens, undefined)
})

test('foreground native context does not freeze later wakeup occupancy or lose its size', async (t) => {
  const h = await harness(t)
  const nativePrompt = h.session.transport.rpc.prompt.bind(
    h.session.transport.rpc,
  )
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await nativePrompt({
      sessionId: h.session.state.nativeId,
      prompt: [{ type: 'text', text: 'usage-native' }],
    })
    await h.write([
      { step_index: 0, type: 'USER_INPUT', content: 'work' },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Waiting',
        input_tokens: 15000,
        output_tokens: 1,
      },
      {
        step_index: 2,
        type: 'SYSTEM_MESSAGE',
        content:
          '[Message] sender=worker priority=MESSAGE_PRIORITY_HIGH content=ready',
      },
      {
        step_index: 3,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Finished',
        input_tokens: 89744,
        output_tokens: 256,
      },
    ])
    return {
      stopReason: 'end_turn' as const,
      usage: { inputTokens: 15000, outputTokens: 1, totalTokens: 15001 },
    }
  })
  await h.prompt()
  assert.equal(h.latest().contextWindowUsedTokens, 90000)
  assert.equal(h.latest().contextWindowMaxTokens, 128000)
})

test('native capacity survives a later same-model transcript fallback', async (t) => {
  const h = await harness(t)
  await h.prompt('usage-native')
  await h.prompt('usage-cancel')
  assert.equal(h.latest().contextWindowMaxTokens, 128000)
  await h.m.handle({
    type: 'session.configure',
    requestId: 'model',
    sessionId: 's',
    changes: { model: 'gpt-oss-120b-medium' },
  })
  await h.prompt('usage-cancel')
  assert.equal(
    h.latest().contextWindowMaxTokens,
    131072,
    'native capacity from another model must not leak',
  )
  await h.m.handle({
    type: 'session.configure',
    requestId: 'model-back',
    sessionId: 's',
    changes: { model: 'test' },
  })
  await h.prompt('usage-cancel')
  assert.equal(
    h.latest().contextWindowMaxTokens,
    128000,
    'the observed capacity is remembered by model',
  )
})

test('all-zero planner counters are not an occupancy measurement', () => {
  assert.equal(
    extractStepUsage({
      type: 'PLANNER_RESPONSE',
      input_tokens: 0,
      output_tokens: 0,
      cache_read_tokens: 0,
      cache_write_tokens: 0,
    }),
    undefined,
  )
})

test('latest snapshot tail-read avoids unrelated large history and preserves complete UTF-8 rows', async (t) => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-tail-'))
  t.after(() => rm(home, { recursive: true, force: true }))
  const native = '11111111-1111-4111-8111-111111111111'
  const dir = join(home, native, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const snapshot = {
    step_index: 21,
    type: 'PLANNER_RESPONSE',
    input_tokens: 70,
    output_tokens: 3,
    content: '中'.repeat(30000),
  }
  await writeFile(
    join(dir, 'transcript_full.jsonl'),
    'unrelated old history'.repeat(100000) +
      '\n' +
      JSON.stringify(snapshot) +
      '\n' +
      JSON.stringify({ step_index: 22, type: 'GENERIC' }) +
      '\n' +
      '{"step_index":23',
  )
  const result = await readLatestStepUsage(home, native, 20)
  assert.deepEqual(result, {
    lastStepUsage: { inputTokens: 70, outputTokens: 3, cachedReadTokens: 0 },
    lastStepIndex: 22,
  })
})

test('late historical counters stay outside the next turn but remain recoverable on restore', async (t) => {
  const h = await harness(t)
  const initial = [
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      input_tokens: 70,
      output_tokens: 3,
    },
    { step_index: 2, type: 'GENERIC', status: 'CANCELED' },
  ]
  t.mock.method(h.session.transport.rpc, 'prompt', async () => {
    await h.write(initial)
    return { stopReason: 'cancelled' as const }
  })
  await h.prompt()
  assert.equal(h.latest().contextWindowUsedTokens, 73)
  assert.equal(h.session.state.lastKnownStepIndex, 2)
  await h.write([
    {
      step_index: 1,
      type: 'PLANNER_RESPONSE',
      input_tokens: 700,
      output_tokens: 30,
    },
    { step_index: 2, type: 'GENERIC', status: 'CANCELED' },
  ])
  const count = h.events.filter((e) => e.type === 'session.usage').length
  t.mock.method(h.session.transport.rpc, 'prompt', async () => ({
    stopReason: 'cancelled' as const,
  }))
  await h.prompt()
  assert.equal(
    h.events.filter((e) => e.type === 'session.usage').length,
    count,
    'late previous-turn data must not become a new-turn bill or snapshot',
  )
  const opened = h.events.find((e) => e.type === 'session.opened')
  assert.ok(opened?.type === 'session.opened')
  await h.m.handle({
    type: 'session.close',
    requestId: 'close',
    sessionId: 's',
  })
  await h.m.open({
    type: 'session.open',
    requestId: 'restore',
    sessionId: 'restored',
    history: 'skip',
    persistence: opened.persistence,
    config: {
      cwd: h.home,
      env: {},
      mcpServers: {},
      settings: {},
      persist: true,
    },
  })
  assert.equal(h.latest().contextWindowUsedTokens, 730)
  assert.equal(
    h.latest().inputTokens,
    undefined,
    'restoring late history does not create a consumption bill',
  )
})
