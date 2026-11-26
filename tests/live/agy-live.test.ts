import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  catalogCacheKey,
  providerStatus,
  resolveCatalogCacheKey,
} from '../../server/preflight.ts'
import { AgyUsageSource } from '../../server/usage.ts'
import { isolatedEnv } from '../helpers.ts'

test(
  'live agy CLI satisfies minimum version requirement and resolves executable fingerprint',
  { timeout: 10_000 },
  async () => {
    const status = await providerStatus()
    assert.equal(status.available, true, status.diagnostic)
    assert.notEqual(await resolveCatalogCacheKey(), catalogCacheKey())
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

test(
  'live agy usage CLI reports no_quota under GEMINI_API_KEY mode',
  { timeout: 15_000 },
  async (t) => {
    const home = await mkdtemp(join(tmpdir(), 'agy-live-apikey-'))
    t.after(() => rm(home, { recursive: true, force: true }))
    const env = isolatedEnv(home, {
      PATH: process.env.PATH ?? '',
      GEMINI_API_KEY: 'dummy',
    })
    const usage = new AgyUsageSource({ env })
    const dataDir = usage.remember(
      {
        command: 'agy',
        args: ['--settings', '{"modelProvider":"gemini"}'],
        env,
      },
      {},
      home,
    )
    const report = await usage.fetch({ dataDir })
    assert.ok(report.status === 'unavailable')
    assert.equal(report.problem.kind, 'no_quota')
    assert.match(report.problem.detail, /Gemini API key/i)
  },
)
