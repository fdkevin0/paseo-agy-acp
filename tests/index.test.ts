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

function createRecordingServerContext() {
  let usage: UsageSourceRegistration | undefined
  let provider: ProviderRegistration | undefined
  const unexpected = (method: string) => () => {
    throw new Error(`Unexpected PluginServerContext.${method} call`)
  }
  const server: PluginServerContext = {
    registerSettings: unexpected('registerSettings'),
    handle: unexpected('handle'),
    addHook: unexpected('addHook'),
    on: unexpected('on'),
    before: unexpected('before'),
    registerUsageSource(source) {
      usage = source
    },
    registerProvider(registration) {
      provider = registration
    },
  }
  return {
    server,
    get usage() {
      return usage
    },
    get provider() {
      return provider
    },
  }
}

function findEscapingRuntimeImports(root: string): string[] {
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
  const violations: string[] = []
  for (const file of files) {
    const source = readFileSync(file, 'utf8')
    for (const match of source.matchAll(specifierPattern)) {
      const target = resolve(dirname(file), match[1])
      const insideAllowed = allowedRoots.some(
        (allowed) => target === allowed || target.startsWith(allowed + sep),
      )
      if (!insideAllowed) {
        violations.push(`${relative(root, file)} -> ${match[1]}`)
      }
    }
  }
  return violations
}

test('contribute registers provider and usage source', () => {
  const recorder = createRecordingServerContext()
  const cleanup = contribute(recorder.server)
  assert.equal(typeof cleanup, 'function')
  cleanup()

  assert.ok(recorder.usage)
  assert.equal(recorder.usage.id, 'agy')
  assert.equal(recorder.usage.label, 'Antigravity')
  assert.equal(recorder.usage.icon, 'icon.svg')
  assert.equal(typeof recorder.usage.discover, 'function')
  assert.equal(typeof recorder.usage.fetch, 'function')

  assert.ok(recorder.provider)
  assert.equal(recorder.provider.id, 'agy')
  assert.equal(recorder.provider.label, 'Antigravity (ACP)')
})

test('PACKAGE_VERSION matches package.json and server imports respect Paseo runtime boundaries', () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
  const pkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8'))
  assert.equal(PACKAGE_VERSION, pkg.version)
  assert.deepEqual(findEscapingRuntimeImports(root), [])
})
