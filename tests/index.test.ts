import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { PluginServerContext } from '@getpaseo/plugin/server'
import type { ProviderRegistration } from '@getpaseo/plugin/server/provider'
import type { UsageSourceRegistration } from '@getpaseo/plugin/server/usage'
import contribute from '../index.server.ts'
import { AgyUsageSource } from '../server/usage.ts'

test('contribute registers provider and usage source', () => {
  let registeredUsage: UsageSourceRegistration | undefined
  let registeredProvider: ProviderRegistration | undefined

  const mockServer = {
    registerUsageSource(source: UsageSourceRegistration) {
      registeredUsage = source
    },
    registerProvider(provider: ProviderRegistration) {
      registeredProvider = provider
    },
  } as unknown as PluginServerContext

  const cleanup = contribute(mockServer)
  assert.equal(typeof cleanup, 'function')
  cleanup()

  assert.ok(registeredUsage)
  assert.equal(registeredUsage.id, 'agy')
  assert.equal(registeredUsage.label, 'Antigravity')
  assert.equal(registeredUsage.icon, 'icon.svg')
  assert.equal(typeof registeredUsage.discover, 'function')
  assert.equal(typeof registeredUsage.fetch, 'function')

  assert.ok(registeredProvider)
  assert.equal(registeredProvider.id, 'agy')
  assert.equal(registeredProvider.label, 'Antigravity (ACP)')
})

test('AgyUsageSource.registration returns valid registration structure', () => {
  const usage = new AgyUsageSource()
  const reg = usage.registration()
  assert.equal(reg.id, 'agy')
  assert.equal(reg.label, 'Antigravity')
  assert.equal(reg.icon, 'icon.svg')
  assert.equal(typeof reg.discover, 'function')
  assert.equal(typeof reg.fetch, 'function')
  assert.ok(reg.input)
})
