import assert from 'node:assert/strict'
import { test } from 'node:test'
import { TranscriptReader } from '../server/brain-io.ts'
import { BrainTurnObservation } from '../server/brain-observer.ts'
import { ScriptedTranscriptReader } from './helpers.ts'

const busy = (message: string) =>
  Object.assign(new Error(message), { code: 'EBUSY' })

const observation = (reader: TranscriptReader, onItem: () => void = () => {}) =>
  new BrainTurnObservation(
    '',
    '',
    's',
    -1,
    {
      isCurrent: () => true,
      onUsage: () => {},
      onItem,
    },
    undefined,
    reader,
  )

test('a successful reader observation clears only the preceding I/O fault', async () => {
  const error = busy('temporary read fault')
  const reader = new ScriptedTranscriptReader([
    async () => {
      throw error
    },
    async () => ({ rows: [], reset: false }),
  ])
  const o = observation(reader)
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  await o.poll()
  assert.equal(o.error, undefined)
  assert.equal(reader.reads, 2)
  await o.dispose()
})

test('persistent reader I/O exposes its latest diagnostic and stops reading on disposal', async () => {
  const first = busy('first read fault')
  const latest = busy('latest read fault')
  const reader = new ScriptedTranscriptReader([
    async () => {
      throw first
    },
    async () => {
      throw latest
    },
  ])
  const o = observation(reader)
  await assert.rejects(o.poll(), (actual) => actual === first)
  await assert.rejects(o.poll(), (actual) => actual === latest)
  assert.equal(o.error, latest)
  await o.dispose()
  await o.poll()
  assert.equal(reader.reads, 2)
  assert.equal(o.error, latest)
})

test('an errno-shaped publication failure remains fatal instead of being cleared by another read', async () => {
  const error = busy('publisher failed')
  const reader = new ScriptedTranscriptReader([
    async () => ({
      rows: [{ step_index: 1, type: 'CHECKPOINT', status: 'DONE' }],
      reset: false,
    }),
  ])
  const o = observation(reader, () => {
    throw error
  })
  await assert.rejects(o.poll(), (actual) => actual === error)
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(
    reader.reads,
    1,
    'a partly applied callback cannot be retried with an empty cursor',
  )
  await o.dispose()
})

test('an errno-shaped handoff failure cannot become a recoverable reader failure', async () => {
  const error = busy('fold failed')
  const reader = new ScriptedTranscriptReader([
    async () => ({ rows: [], reset: false }),
  ])
  const o = observation(reader)
  await o.poll()
  await assert.rejects(
    o.attach(async () => {
      throw error
    }),
    (actual) => actual === error,
  )
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(reader.reads, 1)
  await o.dispose()
})

test('malformed committed data stays fatal even if a later cursor read would be empty', async () => {
  const error = new SyntaxError('invalid complete transcript record')
  const reader = new ScriptedTranscriptReader([
    async () => {
      throw error
    },
    async () => ({ rows: [], reset: false }),
  ])
  const o = observation(reader)
  await assert.rejects(o.poll(), (actual) => actual === error)
  await assert.rejects(o.poll(), (actual) => actual === error)
  assert.equal(o.error, error)
  assert.equal(reader.reads, 1)
  await o.dispose()
})
