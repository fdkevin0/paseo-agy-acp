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

const id = '11111111-1111-4111-8111-111111111111'
const checkpoint = (step_index: number) => ({
  step_index,
  type: 'CHECKPOINT',
  status: 'DONE',
})
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
  const handle = await open(path, 'r')
  const prototype = Object.getPrototypeOf(handle)
  await handle.close()
  t.after(() => rm(root, { recursive: true, force: true }))
  return { root, path, prototype }
}

function failReadOnce(t: TestContext, prototype: FileHandle, position: number) {
  const read = Reflect.get(prototype, 'read')
  let failed = false
  t.mock.method(
    prototype,
    'read',
    async function (this: FileHandle, ...args: unknown[]) {
      // Exclude the small identity/probe reads; interrupt an actual payload chunk.
      if (
        !failed &&
        args[3] === position &&
        typeof args[2] === 'number' &&
        args[2] > 4096
      ) {
        failed = true
        throw Object.assign(new Error('controlled payload EBUSY'), {
          code: 'EBUSY',
        })
      }
      return Reflect.apply(read, this, args)
    },
  )
  return () => failed
}

for (const position of [0, 1024 * 1024])
  test(`reader recovery delivers an unreturned prefix exactly once after I/O at ${position}`, async (t) => {
    const f = await fixture(t)
    await writeFile(
      f.path,
      Buffer.concat([
        line(checkpoint(1)),
        line(planner(2, 'x'.repeat(1400000))),
      ]),
    )
    const failed = failReadOnce(t, f.prototype, position)
    const items: unknown[] = []
    const o = new BrainTurnObservation(f.root, id, 's', -1, {
      isCurrent: () => true,
      onUsage: () => {},
      onItem: (item) => items.push(item),
    })
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
  })

test('append I/O rollback retains an already-delivered prefix and torn UTF-8 remainder', async (t) => {
  const f = await fixture(t),
    reader = new TranscriptReader()
  const content = '界'.repeat(500000),
    row = line(planner(2, content))
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
  const failed = failReadOnce(t, f.prototype, initial.length + 1024 * 1024)
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
  const f = await fixture(t),
    reader = new TranscriptReader(),
    dedupe = new StepDedupe()
  await writeFile(
    f.path,
    Buffer.concat([line(checkpoint(1)), line(checkpoint(8))]),
  )
  const old = await reader.read(f.root, id)
  assert.deepEqual(old.snapshotStepIndices, new Set([1, 8]))
  for (const row of old.rows) assert.equal(dedupe.fresh(row), true)
  const tail = line(planner(9, '界')),
    cut = tail.indexOf(Buffer.from('界')) + 1
  const replacement = [
    checkpoint(1),
    planner(2, 'x'.repeat(1400000)),
    checkpoint(3),
  ]
  await writeFile(
    f.path,
    Buffer.concat([...replacement.map(line), tail.subarray(0, cut)]),
  )
  const failed = failReadOnce(t, f.prototype, 1024 * 1024)
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
  const f = await fixture(t),
    reader = new TranscriptReader()
  const row = line(planner(2, '界')),
    cut = row.indexOf(Buffer.from('界')) + 1
  const initial = Buffer.concat([line(checkpoint(1)), row.subarray(0, cut)])
  await writeFile(f.path, initial)
  assert.deepEqual((await reader.read(f.root, id)).rows, [checkpoint(1)])
  await appendFile(
    f.path,
    Buffer.concat([row.subarray(cut), line(checkpoint(3))]),
  )
  const read = Reflect.get(f.prototype, 'read')
  let armed = false
  let failed = false
  t.mock.method(
    f.prototype,
    'read',
    async function (this: FileHandle, ...args: unknown[]) {
      const result = await Reflect.apply(read, this, args)
      if (!armed && args[3] === initial.length) {
        armed = true
        const close = this.close.bind(this)
        t.mock.method(this, 'close', async () => {
          await close()
          failed = true
          throw Object.assign(new Error('controlled close EIO'), {
            code: 'EIO',
          })
        })
      }
      return result
    },
  )
  await assert.rejects(reader.read(f.root, id), /controlled close EIO/)
  assert.equal(failed, true)
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
