import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdir, rename, utimes, writeFile } from 'node:fs/promises'
import { basename, dirname, join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import {
  ProviderEventSchema,
  type ProviderConnection,
  type ProviderEvent,
} from '@getpaseo/plugin/server/provider'

/** Poll `predicate` until true or fail after `timeoutMs`. */
export async function waitUntil(
  predicate: () => boolean,
  timeoutMs = 5000,
  describeFailure?: () => string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    assert.ok(
      Date.now() < deadline,
      describeFailure ? describeFailure() : 'Timed out waiting for condition',
    )
    await delay(10)
  }
}

/** Collect emitted `ProviderEvent` items with push-based waiting. */
export function createEventCollector(onEvent?: (event: ProviderEvent) => void) {
  const events: ProviderEvent[] = []
  const listeners = new Set<() => void>()

  function emit(event: ProviderEvent) {
    const cloned = structuredClone(event)
    events.push(cloned)
    onEvent?.(cloned)
    for (const notify of [...listeners]) notify()
  }

  async function wait(
    predicate: (event: ProviderEvent) => boolean,
    timeoutMs = 5000,
    assertHealthy?: () => void,
  ): Promise<ProviderEvent> {
    assertHealthy?.()
    const existing = events.find(predicate)
    if (existing) return existing
    let check!: () => void
    let timer!: ReturnType<typeof setTimeout>
    try {
      return await new Promise<ProviderEvent>((resolve, reject) => {
        check = () => {
          try {
            assertHealthy?.()
          } catch (err) {
            reject(err)
            return
          }
          const match = events.find(predicate)
          if (match) resolve(match)
        }
        timer = setTimeout(() => {
          try {
            assertHealthy?.()
            assert.fail(`Timed out: ${JSON.stringify(events)}`)
          } catch (err) {
            reject(err)
          }
        }, timeoutMs)
        listeners.add(check)
        check()
      })
    } finally {
      clearTimeout(timer)
      listeners.delete(check)
    }
  }

  return { events, emit, wait }
}

/** Validate and collect all events emitted on a `ProviderConnection`. */
export function collectProviderEvents(
  provider: ProviderConnection,
  onEvent?: (event: ProviderEvent) => void,
) {
  const opened = new Map<string, readonly string[]>()
  const protocolErrors: string[] = []
  const collector = createEventCollector((event) => {
    ProviderEventSchema.parse(event)
    if (event.type === 'session.opened') {
      if (
        event.parentSessionId &&
        !opened.get(event.parentSessionId)?.includes('session.subsession')
      ) {
        protocolErrors.push(
          `Parent lacks session.subsession: ${event.parentSessionId}`,
        )
      }
      opened.set(event.sessionId, event.capabilities)
    } else if (event.type === 'session.closed') {
      opened.delete(event.sessionId)
    }
    onEvent?.(event)
  })

  provider.onEvent(collector.emit)

  return {
    events: collector.events,
    protocolErrors,
    wait: (predicate: (event: ProviderEvent) => boolean, timeoutMs = 5000) =>
      collector.wait(predicate, timeoutMs, () =>
        assert.deepEqual(protocolErrors, []),
      ),
  }
}

/** Build an isolated environment anchored under `home` without inheriting host config or credentials. */
export function isolatedEnv(
  home: string,
  extra: Record<string, string> = {},
): Record<string, string> {
  return {
    HOME: home,
    XDG_CONFIG_HOME: join(home, '.config'),
    XDG_CACHE_HOME: join(home, '.cache'),
    PATH: '',
    ...extra,
  }
}

/** Per-call boundary double returning each scripted handler once in call order. */
export function sequenceDouble<Args extends unknown[], R>(
  handlers: readonly ((...args: Args) => R)[],
): (...args: Args) => R {
  let index = 0
  return (...args: Args): R => {
    assert.ok(
      index < handlers.length,
      `Unexpected call #${index + 1} (scripted ${handlers.length})`,
    )
    return handlers[index++](...args)
  }
}

/** Drain any in-flight observation pass, then run one fresh read strictly after prior disk writes. */
export async function flushObs(obs: { poll(): Promise<void> }): Promise<void> {
  await obs.poll()
  await obs.poll()
}

/** Write `content` (and optional `mtime`) atomically via a sibling `.tmp` file + `rename`. */
export async function writeFileAtomic(
  path: string,
  content: string,
  mtime?: Date,
): Promise<void> {
  const dir = dirname(path)
  await mkdir(dir, { recursive: true })
  const tmp = join(dir, `.${basename(path)}.${randomUUID()}.tmp`)
  await writeFile(tmp, content)
  if (mtime) await utimes(tmp, mtime, mtime)
  await rename(tmp, path)
}

/** Write JSONL rows to `<brainRoot>/<nativeId>/.system_generated/logs/transcript_full.jsonl`. */
export async function writeBrainTranscript(
  brainRoot: string,
  nativeId: string,
  rows: readonly Record<string, unknown>[],
  mtime?: Date,
): Promise<string> {
  const path = join(
    brainRoot,
    nativeId,
    '.system_generated/logs/transcript_full.jsonl',
  )
  const payload = rows.map((row) => JSON.stringify(row)).join('\n') + '\n'
  if (mtime) {
    await writeFileAtomic(path, payload, mtime)
  } else {
    await mkdir(dirname(path), { recursive: true })
    await writeFile(path, payload)
  }
  return path
}
