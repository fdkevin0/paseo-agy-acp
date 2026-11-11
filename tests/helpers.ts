import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
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

/** Validate and collect all events emitted on a `ProviderConnection`. */
export function collectProviderEvents(
  provider: ProviderConnection,
  onEvent?: (event: ProviderEvent) => void,
) {
  const events: ProviderEvent[] = []
  const opened = new Map<string, readonly string[]>()
  const protocolErrors: string[] = []

  provider.onEvent((event) => {
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
    const cloned = structuredClone(event)
    events.push(cloned)
    onEvent?.(cloned)
  })

  async function wait(
    predicate: (event: ProviderEvent) => boolean,
    timeoutMs = 5000,
  ): Promise<ProviderEvent> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      assert.deepEqual(protocolErrors, [])
      const match = events.find(predicate)
      if (match) return match
      await delay(10)
    }
    assert.deepEqual(protocolErrors, [])
    assert.fail(`Timed out: ${JSON.stringify(events)}`)
  }

  return { events, protocolErrors, wait }
}

/** Write JSONL rows to `<brainRoot>/<nativeId>/.system_generated/logs/transcript_full.jsonl`. */
export async function writeBrainTranscript(
  brainRoot: string,
  nativeId: string,
  rows: readonly Record<string, unknown>[],
): Promise<string> {
  const path = join(
    brainRoot,
    nativeId,
    '.system_generated/logs/transcript_full.jsonl',
  )
  await mkdir(dirname(path), { recursive: true })
  await writeFile(
    path,
    rows.map((row) => JSON.stringify(row)).join('\n') + '\n',
  )
  return path
}
