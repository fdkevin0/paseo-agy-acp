import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, mkdir, writeFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  PROVIDER_CAPABILITIES,
  type ProviderConnection,
  type ProviderEvent,
  type ProviderLaunch,
  type ProviderTimelineItem,
} from '@getpaseo/plugin/server/provider'
import { AgyUsageSource } from '../server/usage.ts'
import { record } from '../server/common.ts'
import { dataDirectory } from '../server/preflight.ts'
import { createProvider } from '../server/provider.ts'
import { SessionManager } from '../server/session.ts'
import { connectAcp } from '../server/transport.ts'
import {
  collectProviderEvents,
  createEventCollector,
  isolatedEnv,
} from './helpers.ts'

async function harness(env: Record<string, string> = {}) {
  const autoHome = env.HOME
    ? undefined
    : await mkdtemp(join(tmpdir(), 'agy-provider-'))
  const home = env.HOME ?? autoHome!
  const launchEnv = isolatedEnv(home, env)
  const usage = new AgyUsageSource({ env: launchEnv })
  const rawProvider = await createProvider(usage).connect({
    versions: [1],
    capabilities: PROVIDER_CAPABILITIES,
    launch: {
      command: process.execPath,
      args: [fileURLToPath(new URL('./mock-acp.ts', import.meta.url))],
      env: launchEnv,
    },
  })
  const provider: ProviderConnection = {
    ...rawProvider,
    close: async () => {
      try {
        await rawProvider.close()
      } finally {
        if (autoHome) await rm(autoHome, { recursive: true, force: true })
      }
    },
  }
  const { events, wait } = collectProviderEvents(provider)
  const config = {
    cwd: home,
    env: {},
    mcpServers: {},
    settings: {},
    persist: true,
  }
  return { provider, events, wait, config, usage, home }
}

type Harness = Awaited<ReturnType<typeof harness>>

async function assertRejectsRuntimeVersion(
  version: string,
  diagnostic: RegExp,
) {
  const h = await harness({ AGY_TEST_VERSION: version })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    const failure = await h.wait(
      (event) =>
        event.type === 'request.failed' || event.type === 'session.opened',
    )
    assert.ok(failure.type === 'request.failed')
    assert.match(failure.error.message, diagnostic)
    assert.equal(
      h.events.some((event) => event.type === 'session.opened'),
      false,
    )
  } finally {
    await h.provider.close()
  }
}

test('session.open rejects an unsupported ACP initialize version without status preflight', async () => {
  await assertRejectsRuntimeVersion(
    '1.5.9',
    /agy 1\.5\.9 is too old; requires >=1\.5\.10/,
  )
})

test('session.open rejects a minimum-version ACP prerelease', async () => {
  await assertRejectsRuntimeVersion(
    '1.5.10-rc.1',
    /agy 1\.5\.10-rc\.1 is too old/,
  )
})

test('session.open rejects ACP initialize without agentInfo version', async () => {
  await assertRejectsRuntimeVersion(
    'missing',
    /Could not determine agy version; requires >=1\.5\.10/,
  )
})

test('session configure rejects a replacement runtime below the ACP minimum', async () => {
  const h = await harness({ AGY_TEST_RELAUNCH_VERSION: '1.5.9' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((event) => event.type === 'session.ready')
    await h.provider.send({
      type: 'session.configure',
      requestId: 'relaunch',
      sessionId: 's',
      changes: { mode: 'full-access' },
    })
    const failure = await h.wait(
      (event) =>
        event.type === 'session.runtime_failed' ||
        (event.type === 'request.completed' && event.requestId === 'relaunch'),
    )
    assert.ok(failure.type === 'session.runtime_failed')
    assert.match(
      failure.error.message,
      /agy 1\.5\.9 is too old; requires >=1\.5\.10/,
    )
    await h.wait(
      (event) =>
        event.type === 'request.failed' && event.requestId === 'relaunch',
    )
  } finally {
    await h.provider.close()
  }
})

function interruptOnTurnStart(
  provider: ProviderConnection,
  sessionId: string,
  requestId: string,
) {
  let pending: Promise<void> | undefined
  provider.onEvent((e) => {
    if (
      e.type === 'session.turn' &&
      e.sessionId === sessionId &&
      e.state === 'started'
    ) {
      pending = provider.send({
        type: 'session.interrupt',
        requestId,
        sessionId,
      })
    }
  })
  return () => pending
}

async function assertCommandsPrecedeReady(
  h: Harness,
  sessionId: string,
  persistence?: { version: 1; data: { sessionId: string } },
) {
  h.events.length = 0
  await h.provider.send({
    type: 'session.open',
    requestId: sessionId,
    sessionId,
    config: h.config,
    history: 'skip',
    ...(persistence ? { persistence } : {}),
  })
  await h.wait((e) => e.type === 'session.ready')
  const commands = h.events.find((e) => e.type === 'session.commands')
  assert.ok(commands?.type === 'session.commands')
  assert.equal(commands.commands[0].name, 'compact')
  assert.equal(
    h.events.indexOf(commands) <
      h.events.findIndex((e) => e.type === 'session.ready'),
    true,
  )
}

async function seedSessionArtifacts(dir: string, nativeId: string) {
  const files = ['.db', '.db-wal', '.db-shm'].map((ext) =>
    join(dir, 'conversations', nativeId + ext),
  )
  const brain = join(dir, 'brain', nativeId)
  await mkdir(join(dir, 'conversations'), { recursive: true })
  await mkdir(brain, { recursive: true })
  await Promise.all(files.map((file) => writeFile(file, 'fixture')))
  const shared = join(dir, 'conversations', 'summary.db')
  await writeFile(shared, 'keep')
  return { paths: [...files, brain], shared }
}

async function assertPathsRemoved(paths: readonly string[]) {
  for (const path of paths) await assert.rejects(stat(path), { code: 'ENOENT' })
}

async function assertPathsExist(paths: readonly string[]) {
  for (const path of paths) await stat(path)
}

async function openAndCloseSession(h: Harness, id: string) {
  await h.provider.send({
    type: 'session.open',
    requestId: id,
    sessionId: id,
    config: h.config,
    history: 'replay',
  })
  await h.wait((e) => e.type === 'session.ready' && e.sessionId === id)
  await h.provider.send({
    type: 'session.close',
    requestId: `close-${id}`,
    sessionId: id,
  })
  await h.wait((e) => e.type === 'session.closed' && e.sessionId === id)
}

function timelineItems(
  events: readonly ProviderEvent[],
): ProviderTimelineItem[] {
  return events
    .filter(
      (e): e is Extract<ProviderEvent, { type: 'timeline.item' }> =>
        e.type === 'timeline.item',
    )
    .map((e) => e.item)
}

type SubAgentToolEvent = Extract<ProviderEvent, { type: 'timeline.item' }> & {
  item: Extract<ProviderTimelineItem, { type: 'tool_call' }> & {
    detail: Extract<
      Extract<ProviderTimelineItem, { type: 'tool_call' }>['detail'],
      { type: 'sub_agent' }
    >
  }
}

function subAgentToolEvents(
  events: readonly ProviderEvent[],
): SubAgentToolEvent[] {
  return events.filter(
    (e): e is SubAgentToolEvent =>
      e.type === 'timeline.item' &&
      e.item.type === 'tool_call' &&
      e.item.detail.type === 'sub_agent',
  )
}

function assertUnlinkedSendMessages(
  events: readonly ProviderEvent[],
  sessionId: string,
) {
  const messages = subAgentToolEvents(events).filter(
    (e) => e.sessionId === sessionId && e.item.name === 'send_message',
  )
  assert.ok(messages.length > 0)
  assert.deepEqual(
    messages.map((e) => e.item.detail.childSessionId),
    messages.map(() => undefined),
  )
}

test('open, permission, tool merge, duplicate prompt, cancel and runtime crash', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-provider-open-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_LOG_LOAD: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'replay',
    })
    await h.wait((e) => e.type === 'session.ready')
    const prompt = {
      type: 'session.prompt' as const,
      sessionId: 's',
      prompt: {
        clientMessageId: 'p',
        delivery: 'auto' as const,
        input: {
          type: 'message' as const,
          content: [{ type: 'text' as const, text: 'hello' }],
        },
      },
    }
    await h.provider.send(prompt)
    const permission = await h.wait((e) => e.type === 'session.permission')
    assert.ok(permission.type === 'session.permission')
    assert.equal(permission.request.title, 'run_command')
    assert.deepEqual(permission.request.input, { CommandLine: 'echo test' })
    assert.equal(permission.request.detail?.type, 'shell')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: permission.request.id,
      response: { behavior: 'deny', selectedActionId: 'yes' },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.equal(
      h.events.filter(
        (e) => e.type === 'timeline.item' && e.item.type === 'user_message',
      ).length,
      1,
    )
    const usage = h.events.filter((e) => e.type === 'session.usage').at(-1)
    assert.ok(usage?.type === 'session.usage')
    assert.deepEqual(usage.usage, {
      contextWindowUsedTokens: 10,
      contextWindowMaxTokens: 1000,
      totalCostUsd: 0.01,
      inputTokens: 12,
      outputTokens: 3,
      cachedInputTokens: 4,
    })
    const terminalIndex = h.events.findIndex(
      (e) => e.type === 'session.turn' && e.state === 'completed',
    )
    assert.ok(
      h.events.findLastIndex((e) => e.type === 'session.usage') < terminalIndex,
    )
    const tool = h.events.find(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.status === 'completed' &&
        e.item.callId === 'shell',
    )
    assert.ok(
      tool && tool.type === 'timeline.item' && tool.item.type === 'tool_call',
    )
    assert.deepEqual(tool.item.detail, {
      type: 'shell',
      command: 'echo test',
      cwd: undefined,
      output: 'test',
    })
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'cancelled',
      ),
    )
    await h.provider.send(prompt)
    assert.equal(
      h.events.filter((e) => e.type === 'session.prompt_result').length,
      1,
    )
    h.events.length = 0
    await h.provider.send({
      ...prompt,
      prompt: {
        ...prompt.prompt,
        clientMessageId: 'wait',
        input: { type: 'message', content: [{ type: 'text', text: 'wait' }] },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'unfinished' &&
        e.item.status === 'running',
    )
    await h.provider.send({
      type: 'session.interrupt',
      requestId: 'stop',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'canceled')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'unfinished' &&
          e.item.status === 'canceled',
      ),
    )
    assert.equal(await readFile(log, 'utf8'), 'new\ncancel\n')
    h.events.length = 0
    await h.provider.send({
      ...prompt,
      prompt: {
        ...prompt.prompt,
        clientMessageId: 'crash',
        input: { type: 'message', content: [{ type: 'text', text: 'crash' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'failed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.callId === 'unfinished' &&
          e.item.status === 'failed' &&
          e.item.error,
      ),
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('list/import and replay before ready; skip does not replay', async () => {
  const h = await harness()
  try {
    await h.provider.send({
      type: 'sessions',
      requestId: 'list',
      query: 'test',
      cwd: h.config.cwd,
    })
    const list = await h.wait((e) => e.type === 'sessions')
    assert.ok(list.type === 'sessions' && list.sessions.length === 1)
    assert.equal(list.sessions[0].description, 'Last prompt preview')

    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'replay',
      sessionId: 'replay',
      config: h.config,
      history: 'replay',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'replay')
    assert.equal(h.events.filter((e) => e.type === 'timeline.item').length, 3)
    assert.equal(h.events.at(-1)?.type, 'session.ready')
    assert.equal(
      h.events.filter((e) => e.type === 'session.opened' && e.parentSessionId)
        .length,
      1,
    )

    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'skip',
      sessionId: 'skip',
      config: h.config,
      history: 'skip',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 'skip')
    assert.equal(h.events.filter((e) => e.type === 'timeline.item').length, 0)
    assert.equal(h.events.at(-1)?.type, 'session.ready')
    assert.equal(
      h.events.filter((e) => e.type === 'session.opened' && e.parentSessionId)
        .length,
      0,
    )
  } finally {
    await h.provider.close()
  }
})

test('catalog requests deduplicate in-flight probes, re-probe on refresh, and skip unchanged opening config', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-catalog-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await Promise.all(
      ['a', 'b'].map((requestId) =>
        h.provider.send({ type: 'catalog', requestId, cwd: h.config.cwd }),
      ),
    )
    const catalog = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'a',
    )
    assert.ok(catalog.type === 'catalog')
    assert.deepEqual(
      catalog.catalog.models[0].thinkingOptions?.map((o) => o.id),
      ['low'],
    )
    await h.wait((e) => e.type === 'catalog' && e.requestId === 'b')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
    await h.provider.send({
      type: 'catalog',
      requestId: 'c',
      cwd: h.config.cwd,
    })
    await h.wait((e) => e.type === 'catalog' && e.requestId === 'c')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\nnew\nclose\n')
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: { ...h.config, model: 'test' },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\nnew\nclose\nnew\n')
    assert.ok(
      h.events.some(
        (e) => e.type === 'session.config' && e.config.model === 'test',
      ),
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('send rejects requests after the connection is closed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-provider-closed-'))
  try {
    const provider = await createProvider(
      new AgyUsageSource({ env: isolatedEnv(home) }),
    ).connect({
      versions: [1],
      capabilities: [],
    })
    await provider.close()
    await assert.rejects(
      provider.send({ type: 'catalog', requestId: 'closed', cwd: home }),
      /connection is closed/,
    )
  } finally {
    await rm(home, { recursive: true, force: true })
  }
})

test('clearPendingPermissions cancels approval before rejecting a busy prompt', async () => {
  const h = await harness()
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    const prompt = {
      clientMessageId: 'first',
      delivery: 'auto' as const,
      input: {
        type: 'message' as const,
        content: [{ type: 'text' as const, text: 'hello' }],
      },
    }
    await h.provider.send({ type: 'session.prompt', sessionId: 's', prompt })
    await h.wait((e) => e.type === 'session.permission')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        ...prompt,
        clientMessageId: 'second',
        clearPendingPermissions: true,
      },
    })
    await h.wait((e) => e.type === 'session.permission_resolved')
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'session.prompt_result' &&
          e.clientMessageId === 'second' &&
          e.result.type === 'failed',
      ),
    )
  } finally {
    await h.provider.close()
  }
})

test('catalog discovery survives an ACP close error', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-close-error-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_CLOSE_ERROR: '1',
    AGY_TEST_LOG: log,
  })
  try {
    await h.provider.send({
      type: 'catalog',
      requestId: 'catalog',
      cwd: h.config.cwd,
    })
    const catalog = await h.wait((e) => e.type === 'catalog')
    assert.ok(catalog.type === 'catalog' && catalog.catalog.models.length > 0)
    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
    assert.ok(!h.events.some((e) => e.type === 'request.failed'))
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('malformed wire permission is canceled instead of blocking or throwing', async () => {
  const h = await harness({ AGY_TEST_BAD_PERMISSION: '1' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'bad-permission',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(!h.events.some((e) => e.type === 'session.permission'))
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'cancelled',
      ),
    )
  } finally {
    await h.provider.close()
  }
})

test('interrupt before transcript baseline resolves never sends the native prompt', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-early-interrupt-'))
  const log = join(home, 'calls.log')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    const pendingInterrupt = interruptOnTurnStart(h.provider, 's', 'stop')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'p',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'must-not-run-after-interrupt' }],
        },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 's' &&
        e.state === 'canceled',
    )
    await pendingInterrupt()
    assert.ok(!(await readFile(log, 'utf8')).includes('unexpected-prompt'))
    assert.equal(
      h.events.filter((e) => e.type === 'session.turn' && e.state !== 'started')
        .length,
      1,
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('delayed commands precede readiness on both new and restored sessions; tier is observed', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-commands-'))
  const h = await harness({ HOME: home })
  try {
    await assertCommandsPrecedeReady(h, 'false')
    await assertCommandsPrecedeReady(h, 'true', {
      version: 1,
      data: { sessionId: 'native-test' },
    })
    assert.equal(
      h.usage.tiers.get(dataDirectory(undefined, { HOME: home })),
      'Google AI Ultra',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('readiness has a bounded wait when the runtime advertises no commands', async () => {
  const h = await harness({ AGY_TEST_NO_COMMANDS: '1' })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    assert.ok(!h.events.some((e) => e.type === 'session.commands'))
  } finally {
    await h.provider.close()
  }
})

test('close removes only new unprompted session artifacts and retains imported sessions', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-disposable-'))
  const log = join(home, 'calls')
  const id = '11111111-1111-4111-8111-111111111111'
  const dir = dataDirectory(undefined, { HOME: home })
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_CLOSE_ERROR: '1',
  })
  try {
    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    const newOpened = h.events.find((e) => e.type === 'session.opened')
    assert.ok(newOpened?.type === 'session.opened')
    const newArtifacts = await seedSessionArtifacts(
      dir,
      String(record(newOpened.persistence?.data).sessionId),
    )
    await h.provider.send({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
    await assertPathsRemoved(newArtifacts.paths)
    assert.equal(await readFile(newArtifacts.shared, 'utf8'), 'keep')

    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-imported',
      sessionId: 's',
      config: h.config,
      history: 'skip',
      persistence: { version: 1, data: { sessionId: id } },
    })
    await h.wait((e) => e.type === 'session.ready')
    const importedOpened = h.events.find((e) => e.type === 'session.opened')
    assert.ok(importedOpened?.type === 'session.opened')
    const importedArtifacts = await seedSessionArtifacts(
      dir,
      String(record(importedOpened.persistence?.data).sessionId),
    )
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-imported',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
    await assertPathsExist(importedArtifacts.paths)
    assert.equal(await readFile(importedArtifacts.shared, 'utf8'), 'keep')

    assert.equal(await readFile(log, 'utf8'), 'new\nclose\n')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('missing transcript leaves context unknown and prompted sessions remain resumable', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-context-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_NO_CONTEXT: '1',
    AGY_TEST_BAD_PERMISSION: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'p',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    const usage = h.events.findLast((e) => e.type === 'session.usage')
    assert.ok(usage?.type === 'session.usage')
    assert.equal(usage.usage.contextWindowUsedTokens, undefined)
    assert.equal(usage.usage.contextWindowMaxTokens, undefined)
    await h.provider.send({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
    assert.equal(await readFile(log, 'utf8'), 'new\n')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('failed probes clear pending catalog state, allow retries, and cannot replace remembered usage launches', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-failed-probe-'))
  const missingCwd = join(home, 'missing')
  const h = await harness({ HOME: home })
  try {
    await Promise.all(
      ['missing-a', 'missing-b'].map((requestId) =>
        h.provider.send({
          type: 'catalog',
          requestId,
          cwd: missingCwd,
        }),
      ),
    )
    await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'missing-a',
    )
    await h.wait(
      (e) => e.type === 'request.failed' && e.requestId === 'missing-b',
    )
    assert.deepEqual(
      await h.usage.discover({
        kind: 'session',
        provider: 'agy',
        env: { HOME: home },
      }),
      [],
    )
    await mkdir(missingCwd)
    await h.provider.send({
      type: 'catalog',
      requestId: 'retry',
      cwd: missingCwd,
    })
    const recovered = await h.wait(
      (e) => e.type === 'catalog' && e.requestId === 'retry',
    )
    assert.ok(
      recovered.type === 'catalog' && recovered.catalog.models.length > 0,
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('stale buffered/live notifications and permissions cannot cross native sessions or close', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-stale-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_STALE_EVENTS: '1',
  })
  try {
    await openAndCloseSession(h, 'one')
    await openAndCloseSession(h, 'two')
    assert.ok(
      !h.events.some(
        (e) =>
          e.type === 'session.permission' ||
          e.type === 'timeline.item' ||
          e.type === 'session.usage',
      ),
    )
    const commands = h.events.filter((e) => e.type === 'session.commands')
    assert.equal(commands.length, 2)
    assert.ok(
      commands.every(
        (e) => e.commands.length === 1 && e.commands[0].name === 'compact',
      ),
    )
    assert.ok(
      h.events
        .filter((e) => e.type === 'session.config')
        .every((e) => e.config.model === 'test'),
    )
    assert.equal(
      (await readFile(log, 'utf8'))
        .split('\n')
        .filter((s) => s === 'stale-permission:cancelled').length,
      6,
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('cancel is sticky and uses fast path without relaunching after a completed subagent turn', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-cancel-sticky-'))
  const log = join(home, 'calls')
  const parentId = '11111111-1111-4111-8111-111111111111'
  const childId = '22222222-2222-4222-8222-222222222222'
  const brainDir = join(home, '.gemini/antigravity-cli/brain')
  const now = new Date().toISOString()
  await mkdir(join(brainDir, parentId, '.system_generated/subagents'), {
    recursive: true,
  })
  await mkdir(join(brainDir, parentId, '.system_generated/logs'), {
    recursive: true,
  })
  await mkdir(join(brainDir, childId, '.system_generated/messages'), {
    recursive: true,
  })
  await mkdir(join(brainDir, childId, '.system_generated/logs'), {
    recursive: true,
  })
  await writeFile(
    join(brainDir, parentId, '.system_generated/subagents', `${childId}.json`),
    JSON.stringify({
      conversationId: childId,
      subagentDescriptor: { typeName: 'research', role: 'Researcher' },
      spawnStepIndex: 2,
      state: 'SUBAGENT_STATE_ALIVE',
    }),
  )
  await writeFile(
    join(brainDir, childId, '.system_generated/messages/init.json'),
    JSON.stringify({
      id: 'init',
      sender: parentId,
      recipient: childId,
      priority: 'MESSAGE_PRIORITY_HIGH',
      content: 'work',
      timestamp: now,
      sourceMetadata: {
        tool: {
          conversationId: parentId,
          stepIndex: 2,
          toolCall: {
            id: 'child',
            name: 'invoke_subagent',
            argumentsJson: '{}',
          },
        },
      },
    }),
  )
  await writeFile(
    join(brainDir, childId, '.system_generated/messages/read.json'),
    JSON.stringify({ init: true }),
  )
  await writeFile(
    join(brainDir, childId, '.system_generated/logs/transcript_full.jsonl'),
    [
      {
        step_index: 0,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=work\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Child finished',
      },
    ]
      .map((r) => JSON.stringify(r) + '\n')
      .join(''),
  )
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_LOG_LOAD: '1',
    AGY_TEST_CANCEL_DELAY: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'replay',
      persistence: { version: 1, data: { sessionId: parentId } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    await h.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === `s:child:${childId}` &&
        e.state === 'completed',
    )
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'wait',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'wait' }] },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'timeline.item' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'unfinished',
    )
    await h.provider.send({
      type: 'session.interrupt',
      requestId: 'cancel',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'canceled')
    assert.equal(await readFile(log, 'utf8'), 'load\ncancel\n')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('forwards systemPrompt via _meta.antigravity.systemPrompt.append on new and loaded sessions', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-sysprompt-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'new-sys',
      sessionId: 's1',
      config: { ...h.config, systemPrompt: 'Follow Paseo peer protocol.' },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's1')
    await h.provider.send({
      type: 'session.close',
      requestId: 'close-1',
      sessionId: 's1',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's1')
    await h.provider.send({
      type: 'session.open',
      requestId: 'load-sys',
      sessionId: 's2',
      config: { ...h.config, systemPrompt: 'Follow Paseo peer protocol.' },
      history: 'skip',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's2')
    assert.equal(
      await readFile(log, 'utf8'),
      'new:Follow Paseo peer protocol.\nclose\nload:Follow Paseo peer protocol.\n',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('full-access mode launches with --dangerously-skip-permissions, auto-approves permissions, and relaunches when toggled', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-full-access-'))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_LOG_LOAD: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: { ...h.config, mode: 'full-access' },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready')
    const configEvent = h.events.findLast((e) => e.type === 'session.config')
    assert.ok(configEvent?.type === 'session.config')
    assert.equal(configEvent.config.mode, 'full-access')
    assert.equal(
      configEvent.config.modes.find((m) => m.id === 'full-access')
        ?.isUnattended,
      true,
    )
    assert.equal(
      await readFile(log, 'utf8'),
      'flag:--dangerously-skip-permissions\nnew\nconfigure:mode=accept-edits\n',
    )

    h.events.length = 0
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'p1',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(!h.events.some((e) => e.type === 'session.permission'))
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'selected:yes',
      ),
    )

    h.events.length = 0
    await h.provider.send({
      type: 'session.configure',
      requestId: 'cfg-default',
      sessionId: 's',
      changes: { mode: 'default' },
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'cfg-default',
    )
    const afterDefault = h.events.findLast((e) => e.type === 'session.config')
    assert.ok(afterDefault?.type === 'session.config')
    assert.equal(afterDefault.config.mode, 'default')
    assert.ok(!h.events.some((e) => e.type === 'timeline.item'))
    assert.equal(
      await readFile(log, 'utf8'),
      'flag:--dangerously-skip-permissions\nnew\nconfigure:mode=accept-edits\nload\n',
    )

    h.events.length = 0
    await h.provider.send({
      type: 'session.configure',
      requestId: 'cfg-full',
      sessionId: 's',
      changes: { mode: 'full-access' },
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'cfg-full',
    )
    const afterFull = h.events.findLast((e) => e.type === 'session.config')
    assert.ok(afterFull?.type === 'session.config')
    assert.equal(afterFull.config.mode, 'full-access')
    assert.equal(
      await readFile(log, 'utf8'),
      'flag:--dangerously-skip-permissions\nnew\nconfigure:mode=accept-edits\nload\nflag:--dangerously-skip-permissions\nload\nconfigure:mode=accept-edits\n',
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('Incident B: in-RPC high-priority continuation is emitted once and does not double-bill Turn 1 or steal Turn 2 usage', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-inrpc-cont-'))
  const h = await harness({ HOME: home })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'inrpc-1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'in-rpc-continuation' }],
        },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 's' &&
        e.state === 'completed',
    )
    const turn1Items = timelineItems(h.events)
    assert.equal(
      turn1Items.filter(
        (i) =>
          i.type === 'assistant_message' && i.text === 'Final in-RPC reply.',
      ).length,
      1,
      'Final in-RPC reply must be emitted exactly once',
    )
    assert.equal(
      turn1Items.filter(
        (i) =>
          i.type === 'reasoning' &&
          i.text === 'In-RPC reasoning after HIGH wakeup',
      ).length,
      1,
      'In-RPC reasoning must be emitted exactly once',
    )
    assert.ok(
      turn1Items.some(
        (i) =>
          i.type === 'tool_call' &&
          i.callId === 'inrpc-bg' &&
          i.status === 'completed' &&
          i.detail.type === 'shell' &&
          i.detail.output?.includes('inrpc-ok'),
      ),
      'Unstreamed background tool completion must still be surfaced once',
    )
    const turn1Usage = h.events.findLast((e) => e.type === 'session.usage')
    assert.ok(turn1Usage?.type === 'session.usage')
    assert.deepEqual(turn1Usage.usage, {
      inputTokens: 55,
      outputTokens: 13,
      cachedInputTokens: 15,
      contextWindowUsedTokens: 48,
    })

    // The next RPC reports its own tokens, independent of the earlier in-RPC continuation.
    h.events.length = 0
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'inrpc-2',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'after-in-rpc-continuation' }],
        },
      },
    })
    await h.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 's' &&
        e.state === 'completed',
    )
    const turn2Usage = h.events.findLast((e) => e.type === 'session.usage')
    assert.ok(turn2Usage?.type === 'session.usage')
    assert.deepEqual(turn2Usage.usage, {
      inputTokens: 40,
      outputTokens: 10,
      cachedInputTokens: 0,
      contextWindowUsedTokens: 50,
    })
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('toolPolicy MCP preapprovals, allow_once default, deny interrupt, chooser question kind, and can_stop_runtime', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-permissions-layer-a-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: {
        ...h.config,
        mcpServers: { rea: { type: 'stdio', command: 'fixture-mcp' } },
        toolPolicy: {
          preapproved: [{ kind: 'mcp', server: 'rea', tool: 'open_binary' }],
        },
      },
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')

    // Idle session can stop runtime
    await h.provider.send({
      type: 'session.can_stop_runtime',
      requestId: 'stop-idle',
      sessionId: 's',
    })
    const idleStop = await h.wait(
      (e) =>
        e.type === 'session.can_stop_runtime' && e.requestId === 'stop-idle',
    )
    assert.ok(idleStop.type === 'session.can_stop_runtime' && idleStop.canStop)

    // Wrapped MCP titles and explicit raw names with descriptive titles auto-approve with allow_once ('yes')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'mcp-1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'mcp-preapproved' }],
        },
      },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(!h.events.some((e) => e.type === 'session.permission'))
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'mcp:yes,yes',
      ),
    )

    // Non-preapproved tool prompts; active turn with pending permission cannot stop runtime; allow without selectedActionId picks allow_once ('yes')
    h.events.length = 0
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'perm-default-allow',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    const perm = await h.wait((e) => e.type === 'session.permission')
    assert.ok(perm.type === 'session.permission')
    assert.equal(perm.request.kind, 'tool')

    await h.provider.send({
      type: 'session.can_stop_runtime',
      requestId: 'stop-busy',
      sessionId: 's',
    })
    const busyStop = await h.wait(
      (e) =>
        e.type === 'session.can_stop_runtime' && e.requestId === 'stop-busy',
    )
    assert.ok(busyStop.type === 'session.can_stop_runtime' && !busyStop.canStop)

    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: perm.request.id,
      response: { behavior: 'allow' },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'selected:yes',
      ),
    )

    // deny with interrupt: true cancels the active turn
    h.events.length = 0
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'perm-deny-interrupt',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    const permDeny = await h.wait((e) => e.type === 'session.permission')
    assert.ok(permDeny.type === 'session.permission')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: permDeny.request.id,
      response: { behavior: 'deny', interrupt: true },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'canceled')

    // Chooser request in full-access mode is classified as 'question' and is NOT auto-approved
    await h.provider.send({
      type: 'session.configure',
      requestId: 'cfg-full',
      sessionId: 's',
      changes: { mode: 'full-access' },
    })
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'cfg-full',
    )
    h.events.length = 0
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'chooser-1',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'chooser' }],
        },
      },
    })
    const chooserPerm = await h.wait((e) => e.type === 'session.permission')
    assert.ok(chooserPerm.type === 'session.permission')
    assert.equal(chooserPerm.request.kind, 'question')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: chooserPerm.request.id,
      response: { behavior: 'allow', selectedActionId: 'prod' },
    })
    await h.wait((e) => e.type === 'session.turn' && e.state === 'completed')
    assert.ok(
      h.events.some(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'assistant_message' &&
          e.item.text === 'choice:prod',
      ),
    )
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('recursive multi-level subagents link depth-2 grandchild under child session, persist:false omits persistence, and session/resume is used on skip when supported', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-depth2-resume-'))
  const log = join(home, 'calls')
  const parentId = '11111111-1111-4111-8111-111111111111'
  const childId = '22222222-2222-4222-8222-222222222222'
  const grandId = '33333333-3333-4333-8333-333333333333'
  const brainDir = join(home, '.gemini/antigravity-cli/brain')
  const now = new Date().toISOString()

  await mkdir(join(brainDir, childId, '.system_generated/logs'), {
    recursive: true,
  })
  await mkdir(join(brainDir, grandId, '.system_generated/logs'), {
    recursive: true,
  })
  await writeFile(
    join(brainDir, childId, '.system_generated/logs/transcript_full.jsonl'),
    [
      {
        step_index: 0,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${parentId} priority=MESSAGE_PRIORITY_HIGH content=spawn grandchild\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        tool_calls: [
          {
            id: 'spawn-grand',
            name: 'invoke_subagent',
            args: { Role: 'Grandchild Auditor', Prompt: 'audit details' },
          },
        ],
      },
      {
        step_index: 2,
        type: 'GENERIC',
        status: 'DONE',
        content: JSON.stringify({ conversationId: grandId }),
      },
      {
        step_index: 3,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Child waiting for grandchild.',
      },
    ]
      .map((r) => JSON.stringify(r) + '\n')
      .join(''),
  )
  await writeFile(
    join(brainDir, grandId, '.system_generated/logs/transcript_full.jsonl'),
    [
      {
        step_index: 0,
        type: 'SYSTEM_MESSAGE',
        content: `<SYSTEM_MESSAGE>\n[Message] timestamp=${now} sender=${childId} priority=MESSAGE_PRIORITY_HIGH content=audit details\n</SYSTEM_MESSAGE>`,
      },
      {
        step_index: 1,
        type: 'PLANNER_RESPONSE',
        status: 'DONE',
        content: 'Grandchild done.',
      },
    ]
      .map((r) => JSON.stringify(r) + '\n')
      .join(''),
  )

  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    AGY_TEST_RESUME_CAP: '1',
  })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-replay',
      sessionId: 's',
      config: h.config,
      history: 'replay',
      persistence: { version: 1, data: { sessionId: parentId } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    const grandOpened = h.events.find(
      (e) =>
        e.type === 'session.opened' && e.sessionId === `s:child:${grandId}`,
    )
    assert.ok(grandOpened?.type === 'session.opened')
    assert.equal(grandOpened.parentSessionId, `s:child:${childId}`)
    assert.equal(grandOpened.title, 'Grandchild Auditor')

    // history: 'skip' uses session/resume when resume capability is advertised
    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-resume',
      sessionId: 's-resumed',
      config: { ...h.config, systemPrompt: 'Resume instructions' },
      history: 'skip',
      persistence: { version: 1, data: { sessionId: 'native-test' } },
    })
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 's-resumed',
    )
    assert.ok(
      (await readFile(log, 'utf8')).includes('resume:Resume instructions\n'),
    )

    // persist: false omits persistence on session.opened
    h.events.length = 0
    await h.provider.send({
      type: 'session.open',
      requestId: 'open-ephemeral',
      sessionId: 's-ephemeral',
      config: { ...h.config, persist: false, title: 'Ephemeral Task' },
      history: 'skip',
    })
    await h.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 's-ephemeral',
    )
    const ephOpened = h.events.find(
      (e) => e.type === 'session.opened' && e.sessionId === 's-ephemeral',
    )
    assert.ok(ephOpened?.type === 'session.opened')
    assert.equal(ephOpened.persistence, undefined)
    assert.equal(ephOpened.title, 'Ephemeral Task')
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

test('messages recover only owned children and never create root, self or unknown sessions', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-message-ownership-'))
  const root = '11111111-1111-4111-8111-111111111111'
  const child = '22222222-2222-4222-8222-222222222222'
  const grand = '33333333-3333-4333-8333-333333333333'
  const sibling = '55555555-5555-4555-8555-555555555555'
  const unknown = '44444444-4444-4444-8444-444444444444'
  const brain = join(home, '.gemini/antigravity-cli/brain')
  async function transcript(nativeId: string, recipients: string[]) {
    const dir = join(brain, nativeId, '.system_generated/logs')
    await mkdir(dir, { recursive: true })
    await writeFile(
      join(dir, 'transcript_full.jsonl'),
      recipients
        .flatMap((recipient, i) => [
          {
            step_index: i * 2,
            type: 'PLANNER_RESPONSE',
            status: 'DONE',
            tool_calls: [
              {
                id: `message-${i}`,
                name: 'send_message',
                args: { Recipient: recipient, Message: 'report' },
              },
            ],
          },
          {
            step_index: i * 2 + 1,
            type: 'GENERIC',
            status: 'DONE',
            content: 'Message sent',
          },
        ])
        .map((row) => JSON.stringify(row) + '\n')
        .join(''),
    )
  }
  await transcript(child, [root, child, unknown, grand, grand, sibling])
  await transcript(grand, [root, child, grand, unknown, sibling])
  await transcript(sibling, [grand])
  const descriptors = join(brain, child, '.system_generated/subagents')
  await mkdir(descriptors, { recursive: true })
  await writeFile(
    join(descriptors, `${grand}.json`),
    JSON.stringify({ conversationId: grand }),
  )
  await writeFile(
    join(descriptors, `${sibling}.json`),
    JSON.stringify({ conversationId: sibling }),
  )
  const h = await harness({ HOME: home })
  try {
    await h.provider.send({
      type: 'session.open',
      requestId: 'open',
      sessionId: 's',
      config: h.config,
      history: 'replay',
      persistence: { version: 1, data: { sessionId: root } },
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    const opened = h.events.filter((e) => e.type === 'session.opened')
    assert.deepEqual(
      opened.map((e) => e.sessionId),
      ['s', `s:child:${child}`, `s:child:${grand}`, `s:child:${sibling}`],
    )
    assert.equal(opened[2].parentSessionId, `s:child:${child}`)
    assert.deepEqual(opened[1].capabilities, ['session.subsession'])
    assert.deepEqual(opened[2].capabilities, ['session.subsession'])
    const subAgentLinks = subAgentToolEvents(h.events).map((event) => ({
      sessionId: event.sessionId,
      target: event.item.detail.childSessionId,
    }))
    assert.ok(subAgentLinks.length > 0)
    assert.deepEqual(
      subAgentLinks.filter(
        ({ sessionId, target }) =>
          target !== undefined &&
          !opened.some(
            (e) => e.sessionId === target && e.parentSessionId === sessionId,
          ),
      ),
      [],
    )
    assertUnlinkedSendMessages(h.events, `s:child:${grand}`)
    assertUnlinkedSendMessages(h.events, `s:child:${sibling}`)
    await h.provider.send({
      type: 'session.close',
      requestId: 'close',
      sessionId: 's',
    })
    await h.wait((e) => e.type === 'session.closed' && e.sessionId === 's')
    assert.equal(h.events.filter((e) => e.type === 'session.closed').length, 4)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})

/** Open a full-access session so question cards prove they are never auto-approved. */
async function questionSession(name: string) {
  const home = await mkdtemp(join(tmpdir(), `agy-${name}-`))
  const log = join(home, 'calls')
  const h = await harness({
    HOME: home,
    AGY_TEST_LOG: log,
    // Also logs `cancel`, which proves an interrupt did not relaunch agy.
    AGY_TEST_LOG_LOAD: '1',
    AGY_TEST_LOG_ELICITATION_CAP: '1',
  })
  await h.provider.send({
    type: 'session.open',
    requestId: 'open',
    sessionId: 's',
    config: { ...h.config, mode: 'full-access' },
    history: 'skip',
  })
  await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
  h.events.length = 0
  const prompt = (text: string) =>
    h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: text,
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text }] },
      },
    })
  const seen = new Set<string>()
  /** Wait for the next unseen QuestionCard. */
  const nextQuestion = async () => {
    const e = await h.wait(
      (e) => e.type === 'session.permission' && !seen.has(e.request.id),
    )
    assert.ok(e.type === 'session.permission')
    seen.add(e.request.id)
    return e.request
  }
  const answer = (id: string, answers: Record<string, string>) =>
    h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: id,
      response: { behavior: 'allow', updatedInput: { answers } },
    })
  const turn = (state: 'completed' | 'canceled') =>
    h.wait((e) => e.type === 'session.turn' && e.state === state)
  const assistantTexts = () =>
    h.events.flatMap((e) =>
      e.type === 'timeline.item' && e.item.type === 'assistant_message'
        ? [e.item.text]
        : [],
    )
  const questionCards = () =>
    h.events.filter((e) => e.type === 'session.permission').length
  const close = async () => {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
  return {
    h,
    home,
    log,
    prompt,
    nextQuestion,
    answer,
    turn,
    assistantTexts,
    questionCards,
    close,
  }
}

test('a foreground elicitation becomes a QuestionCard in full-access mode and returns schema values', async () => {
  const q = await questionSession('elicitation')
  try {
    assert.ok(
      (await readFile(q.log, 'utf8')).includes('cap:elicitation.form\n'),
      'initialize must advertise elicitation.form',
    )
    await q.prompt('elicitation')
    const card = await q.nextQuestion()
    assert.equal(card.kind, 'question')
    assert.equal(card.name, 'ask_question')
    await q.answer(card.id, {
      'Question 1': '(Recommended) Option 1',
      'Question 2': 'Linux, x86_64, FreeBSD',
    })
    await q.turn('completed')
    assert.ok(
      q
        .assistantTexts()
        .includes(
          'elicited:{"action":"accept","content":{"q1":"1","q2":["1"],"q2_other":"FreeBSD"}}',
        ),
    )
  } finally {
    await q.close()
  }
})

test('an ask_question permission request is not auto-approved in full-access mode', async () => {
  const q = await questionSession('ask-permission')
  try {
    await q.prompt('ask-permission-binary')
    const card = await q.nextQuestion()
    assert.equal(card.kind, 'question')
    await q.h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: card.id,
      response: { behavior: 'allow', selectedActionId: 'allow_once' },
    })
    await q.turn('completed')
    assert.ok(
      q
        .assistantTexts()
        .includes('ask-bin:{"outcome":"selected","optionId":"allow_once"}'),
    )
  } finally {
    await q.close()
  }
})

test('two elicitations in one turn ask once each and settle both unlogged cards', async () => {
  const q = await questionSession('double-elicitation')
  try {
    await q.prompt('double-elicitation')
    await q.answer((await q.nextQuestion()).id, { 'Question 1': 'Yes' })
    await q.answer((await q.nextQuestion()).id, { 'Question 1': 'No' })
    await q.turn('completed')
    assert.equal(q.questionCards(), 2)
    assert.equal(
      q.h.events.filter(
        (e) =>
          e.type === 'timeline.item' &&
          e.item.type === 'tool_call' &&
          e.item.name === 'ask_question' &&
          e.item.status === 'completed',
      ).length,
      2,
    )
  } finally {
    await q.close()
  }
})

test('Full Access without a launch override preserves process.env and missing cwd reports a clear error', async () => {
  const root = await mkdtemp(join(tmpdir(), 'agy-default-launch-'))
  const mockPath = fileURLToPath(new URL('./mock-acp.ts', import.meta.url))
  const observedLaunches: Array<ProviderLaunch | undefined> = []
  const collector = createEventCollector()
  const manager = new SessionManager({
    usage: new AgyUsageSource({ env: isolatedEnv(root) }),
    capabilities: PROVIDER_CAPABILITIES,
    emit: collector.emit,
    connectAcp: (launch, cwd, env, client, onExit, opts) => {
      observedLaunches.push(launch)
      return connectAcp(
        {
          command: process.execPath,
          args: [mockPath],
          env: isolatedEnv(root),
        },
        cwd,
        env,
        client,
        onExit,
        opts,
      )
    },
  })
  try {
    await assert.rejects(
      manager.handle({
        type: 'session.open',
        requestId: 'open-missing-cwd',
        sessionId: 'missing-cwd',
        history: 'skip',
        config: {
          cwd: join(root, 'does-not-exist'),
          env: { HOME: root },
          mcpServers: {},
          settings: {},
          persist: false,
        },
      }),
      /Working directory does not exist/,
    )
    assert.equal(observedLaunches.length, 0)

    await manager.handle({
      type: 'session.open',
      requestId: 'open-full',
      sessionId: 'full',
      history: 'skip',
      config: {
        cwd: root,
        env: { HOME: root },
        mcpServers: {},
        settings: {},
        persist: false,
        mode: 'full-access',
      },
    })
    await collector.wait(
      (e) => e.type === 'session.ready' && e.sessionId === 'full',
    )
    assert.equal(observedLaunches.length, 1)
    const [launch] = observedLaunches
    assert.ok(launch)
    assert.equal(launch.command, 'agy')
    assert.deepEqual(launch.args, ['acp', '--dangerously-skip-permissions'])
    const expectedEnv = Object.fromEntries(
      Object.entries(process.env).filter(([, value]) => value !== undefined),
    )
    assert.deepEqual(launch.env, expectedEnv)
  } finally {
    await manager.close()
    await rm(root, { recursive: true, force: true })
  }
})

test('denied permission marks stranded tool card canceled and concurrent close cancels in-flight open', async () => {
  const home = await mkdtemp(join(tmpdir(), 'agy-deny-and-close-race-'))
  const log = join(home, 'calls')
  const h = await harness({ HOME: home, AGY_TEST_LOG: log })
  try {
    await Promise.all([
      h.provider.send({
        type: 'session.open',
        requestId: 'open-raced',
        sessionId: 'raced',
        config: { ...h.config, persist: false },
        history: 'skip',
      }),
      h.provider.send({
        type: 'session.close',
        requestId: 'close-raced',
        sessionId: 'raced',
      }),
    ])
    await h.wait(
      (e) => e.type === 'request.completed' && e.requestId === 'close-raced',
    )
    await h.wait(
      (e) =>
        (e.type === 'session.ready' && e.sessionId === 'raced') ||
        (e.type === 'request.failed' && e.requestId === 'open-raced'),
    )
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 'raced',
      prompt: {
        clientMessageId: 'after-close',
        delivery: 'auto',
        input: { type: 'message', content: [{ type: 'text', text: 'hello' }] },
      },
    })
    const closedPrompt = await h.wait(
      (e) =>
        e.type === 'session.prompt_result' &&
        e.clientMessageId === 'after-close',
    )
    assert.ok(
      closedPrompt.type === 'session.prompt_result' &&
        closedPrompt.result.type === 'failed',
    )

    await h.provider.send({
      type: 'session.open',
      requestId: 'open-s',
      sessionId: 's',
      config: h.config,
      history: 'skip',
    })
    await h.wait((e) => e.type === 'session.ready' && e.sessionId === 's')
    await h.provider.send({
      type: 'session.prompt',
      sessionId: 's',
      prompt: {
        clientMessageId: 'deny-msg',
        delivery: 'auto',
        input: {
          type: 'message',
          content: [{ type: 'text', text: 'permission-deny' }],
        },
      },
    })
    const perm = await h.wait(
      (e) => e.type === 'session.permission' && e.sessionId === 's',
    )
    assert.ok(perm.type === 'session.permission')
    await h.provider.send({
      type: 'session.permission',
      sessionId: 's',
      permissionId: perm.request.id,
      response: { behavior: 'deny' },
    })
    await h.wait(
      (e) =>
        e.type === 'session.turn' &&
        e.sessionId === 's' &&
        e.state === 'completed',
    )
    const toolItem = h.events.findLast(
      (e) =>
        e.type === 'timeline.item' &&
        e.sessionId === 's' &&
        e.item.type === 'tool_call' &&
        e.item.callId === 'unfinished',
    )
    assert.ok(
      toolItem?.type === 'timeline.item' && toolItem.item.type === 'tool_call',
    )
    assert.equal(toolItem.item.status, 'canceled')
    assert.equal(toolItem.item.error, null)
  } finally {
    await h.provider.close()
    await rm(home, { recursive: true, force: true })
  }
})
