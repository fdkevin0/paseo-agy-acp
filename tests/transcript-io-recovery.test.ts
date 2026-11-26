import assert from 'node:assert/strict'
import { test, type TestContext } from 'node:test'
import {
  mkdtemp,
  mkdir,
  writeFile,
  appendFile,
  open,
  rm,
  type FileHandle,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { TranscriptReader, StepDedupe } from '../server/brain-io.ts'
import { BrainTurnObservation } from '../server/brain-observer.ts'
import { checkpointRow } from './brain-fixtures.ts'

const id = '11111111-1111-4111-8111-111111111111'
const checkpoint = (step_index: number) => checkpointRow(step_index)
const planner = (step_index: number, content: string) => ({
  step_index,
  type: 'PLANNER_RESPONSE',
  status: 'DONE',
  input_tokens: 70,
  output_tokens: 3,
  content,
})
const line = (row: object) => Buffer.from(JSON.stringify(row) + '\n')

async function fixture(t: TestContext) {
  const root = await mkdtemp(join(tmpdir(), 'agy-reader-io-'))
  const dir = join(root, id, '.system_generated/logs')
  await mkdir(dir, { recursive: true })
  const path = join(dir, 'transcript_full.jsonl')
  await writeFile(path, '')
  t.after(() => rm(root, { recursive: true, force: true }))
  return { root, path }
}

function createFaultyOpen() {
  let failReadAt: number | undefined
  let readFailed = false
  let failCloseAt: number | undefined
  let closeFailed = false

  const openFile = (async (
    ...args: Parameters<typeof open>
  ): Promise<FileHandle> => {
    const handle = await open(...args)
    let armedClose = false
    const wrapped = {
      stat: () => handle.stat(),
      read: async (
        buffer: Buffer,
        offset: number,
        length: number,
        position: number,
      ) => {
        // Exclude the small identity/probe reads; interrupt an actual payload chunk.
        if (
          failReadAt !== undefined &&
          !readFailed &&
          position === failReadAt &&
          length > 4096
        ) {
          readFailed = true
          throw Object.assign(new Error('controlled payload EBUSY'), {
            code: 'EBUSY',
          })
        }
        const result = await handle.read(buffer, offset, length, position)
        if (
          failCloseAt !== undefined &&
          !closeFailed &&
          position === failCloseAt
        ) {
          armedClose = true
        }
        return result
      },
      close: async () => {
        await handle.close()
        if (armedClose) {
          armedClose = false
          closeFailed = true
          throw Object.assign(new Error('controlled close EIO'), {
            code: 'EIO',
          })
        }
      },
    }
    return wrapped as unknown as FileHandle
  }) as typeof open

  return {
    openFile,
    failReadOnceAt(position: number) {
      failReadAt = position
      readFailed = false
      return () => readFailed
    },
    failCloseAfterReadAt(position: number) {
      failCloseAt = position
      closeFailed = false
      return () => closeFailed
    },
  }
}

async function assertReaderRecoveryAt(t: TestContext, position: number) {
  const f = await fixture(t)
  await writeFile(
    f.path,
    Buffer.concat([line(checkpoint(1)), line(planner(2, 'x'.repeat(1400000)))]),
  )
  const fault = createFaultyOpen()
  const reader = new TranscriptReader(fault.openFile)
  const failed = fault.failReadOnceAt(position)
  const items: unknown[] = []
  const o = new BrainTurnObservation(
    f.root,
    id,
    's',
    -1,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(item),
    },
    undefined,
    reader,
  )
  t.after(() => o.dispose())
  await assert.rejects(o.poll(), /controlled payload EBUSY/)
  assert.equal(failed(), true)
  assert.equal(
    items.length,
    0,
    'a rejected read has not delivered its parsed local rows',
  )
  await o.poll()
  assert.equal(o.error, undefined)
  assert.equal(o.lastStepIndex, 2)
  assert.equal(o.usage.latest?.inputTokens, 70)
  assert.equal(items.length, 1)
  await o.poll()
  await o.drain()
  assert.equal(items.length, 1)
}

test('reader recovery delivers an unreturned prefix exactly once after I/O at 0', (t) =>
  assertReaderRecoveryAt(t, 0))

test('reader recovery delivers an unreturned prefix exactly once after I/O at 1048576', (t) =>
  assertReaderRecoveryAt(t, 1024 * 1024))

test('append I/O rollback retains an already-delivered prefix and torn UTF-8 remainder', async (t) => {
  const f = await fixture(t)
  const fault = createFaultyOpen()
  const reader = new TranscriptReader(fault.openFile)
  const content = '界'.repeat(500000)
  const row = line(planner(2, content))
  const cut = row.indexOf(Buffer.from('界')) + 1
  const initial = Buffer.concat([line(checkpoint(1)), row.subarray(0, cut)])
  await writeFile(f.path, initial)
  const first = await reader.read(f.root, id)
  assert.deepEqual(first.rows, [checkpoint(1)])
  assert.equal(first.snapshotStepIndices, undefined)
  await appendFile(
    f.path,
    Buffer.concat([row.subarray(cut), line(checkpoint(3))]),
  )
  const failed = fault.failReadOnceAt(initial.length + 1024 * 1024)
  await assert.rejects(reader.read(f.root, id), /controlled payload EBUSY/)
  assert.equal(failed(), true)
  const recovered = await reader.read(f.root, id)
  assert.equal(
    recovered.reset,
    false,
    'rollback restores the prior valid cursor, not an unrelated generation',
  )
  assert.deepEqual(recovered.rows, [planner(2, content), checkpoint(3)])
  assert.deepEqual(recovered.snapshotStepIndices, new Set([1, 2, 3]))
  assert.deepEqual((await reader.read(f.root, id)).rows, [])
})

test('rewrite I/O rollback cannot prune old identities before the replacement UTF-8 tail completes', async (t) => {
  const f = await fixture(t)
  const fault = createFaultyOpen()
  const reader = new TranscriptReader(fault.openFile)
  const dedupe = new StepDedupe()
  await writeFile(
    f.path,
    Buffer.concat([line(checkpoint(1)), line(checkpoint(8))]),
  )
  const old = await reader.read(f.root, id)
  assert.deepEqual(old.snapshotStepIndices, new Set([1, 8]))
  assert.deepEqual(
    old.rows.map((row) => dedupe.fresh(row)),
    [true, true],
  )
  const tail = line(planner(9, '界'))
  const cut = tail.indexOf(Buffer.from('界')) + 1
  const replacement = [
    checkpoint(1),
    planner(2, 'x'.repeat(1400000)),
    checkpoint(3),
  ]
  await writeFile(
    f.path,
    Buffer.concat([...replacement.map(line), tail.subarray(0, cut)]),
  )
  const failed = fault.failReadOnceAt(1024 * 1024)
  await assert.rejects(reader.read(f.root, id), /controlled payload EBUSY/)
  assert.equal(failed(), true)
  assert.equal(dedupe.fresh(checkpoint(8)), false)
  const recovered = await reader.read(f.root, id)
  assert.equal(recovered.reset, true)
  assert.deepEqual(recovered.rows, replacement)
  assert.equal(recovered.snapshotStepIndices, undefined)
  assert.equal(dedupe.fresh(checkpoint(1)), false)
  assert.equal(dedupe.fresh(checkpoint(3)), true)
  assert.equal(
    dedupe.fresh(checkpoint(8)),
    false,
    'a successful partial read still has no deletion authority',
  )
  await appendFile(f.path, tail.subarray(cut))
  const complete = await reader.read(f.root, id)
  assert.equal(complete.reset, false)
  assert.deepEqual(complete.rows, [planner(9, '界')])
  assert.deepEqual(complete.snapshotStepIndices, new Set([1, 2, 3, 9]))
  dedupe.prune(complete.snapshotStepIndices!)
  assert.equal(dedupe.fresh(checkpoint(8)), true)
  assert.equal(dedupe.fresh(checkpoint(3)), false)
})

test('close failure rolls back snapshot candidates without retaining undelivered indices', async (t) => {
  const f = await fixture(t)
  const fault = createFaultyOpen()
  const reader = new TranscriptReader(fault.openFile)
  const row = line(planner(2, '界'))
  const cut = row.indexOf(Buffer.from('界')) + 1
  const initial = Buffer.concat([line(checkpoint(1)), row.subarray(0, cut)])
  await writeFile(f.path, initial)
  assert.deepEqual((await reader.read(f.root, id)).rows, [checkpoint(1)])
  await appendFile(
    f.path,
    Buffer.concat([row.subarray(cut), line(checkpoint(3))]),
  )
  const failed = fault.failCloseAfterReadAt(initial.length)
  await assert.rejects(reader.read(f.root, id), /controlled close EIO/)
  assert.equal(failed(), true)
  // Replace only the uncommitted suffix; the previously delivered cursor/probes remain valid.
  await writeFile(
    f.path,
    Buffer.concat([initial, row.subarray(cut), line(checkpoint(4))]),
  )
  const recovered = await reader.read(f.root, id)
  assert.equal(recovered.reset, false)
  assert.deepEqual(recovered.rows, [planner(2, '界'), checkpoint(4)])
  assert.deepEqual(
    recovered.snapshotStepIndices,
    new Set([1, 2, 4]),
    'failed candidate step 3 cannot authorize pruning',
  )
})
