import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { providerStatus } from '../../server/preflight.ts'
import { AgyUsageSource } from '../../server/usage.ts'
import { isolatedEnv } from '../helpers.ts'

test(
  'live agy CLI satisfies minimum version requirement',
  { timeout: 10_000 },
  async () => {
    const status = await providerStatus()
    assert.equal(status.available, true, status.diagnostic)
  },
)

test(
  'live agy usage CLI handles an isolated unauthenticated home without hanging',
  { timeout: 15_000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-live-isolated-'))
    t.after(() => rm(home, { recursive: true, force: true }))
    const usage = new AgyUsageSource({
      env: isolatedEnv(home, { PATH: process.env.PATH ?? '' }),
    })
    const accounts = await usage.discover({ kind: 'global' })
    assert.deepEqual(accounts, [])
  },
)
