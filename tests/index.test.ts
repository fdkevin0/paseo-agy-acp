import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
import { dirname, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { PluginServerContext } from '@getpaseo/plugin/server'
import type { ProviderRegistration } from '@getpaseo/plugin/server/provider'
import type { UsageSourceRegistration } from '@getpaseo/plugin/server/usage'
import contribute from '../index.server.ts'
import { PACKAGE_VERSION } from '../server/common.ts'
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

test('PACKAGE_VERSION matches package.json and server imports respect Paseo runtime boundaries', () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
  const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'))
  assert.equal(PACKAGE_VERSION, pkg.version)

  const allowedRoots = ['server', 'client', 'shared'].map((dir) =>
    resolve(root, dir),
  )
  const files = [
    resolve(root, 'index.server.ts'),
    ...readdirSync(resolve(root, 'server'))
      .filter((f) => f.endsWith('.ts'))
      .map((f) => resolve(root, 'server', f)),
  ]
  const specifierPattern =
    /\b(?:import|export)\s+(?:[^'"]*?\s+from\s+)?['"](\.[^'"]+)['"]/g
  for (const file of files) {
    const source = readFileSync(file, 'utf8')
    for (const match of source.matchAll(specifierPattern)) {
      const target = resolve(dirname(file), match[1])
      const insideAllowed = allowedRoots.some(
        (allowed) => target === allowed || target.startsWith(allowed + sep),
      )
      assert.ok(
        insideAllowed,
        `Runtime import ${match[1]} in ${relative(root, file)} escapes client/, server/, or shared/`,
      )
    }
  }
})
