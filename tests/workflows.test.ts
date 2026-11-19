import assert from 'node:assert/strict'
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { test, type TestContext } from 'node:test'
import { isolatedEnv } from './helpers.ts'

const workflow = readFileSync(
  new URL('../.github/workflows/release.yml', import.meta.url),
  'utf8',
)

function stepFor(name: string): string {
  const step = workflow
    .split('      - name: ')
    .find((block) => block.startsWith(`${name}\n`))
  assert.ok(step, `Missing workflow step: ${name}`)
  return step
}

// Execute the workflow's actual shell blocks against local command substitutes.
function scriptFor(name: string): string {
  const script = stepFor(name).split('        run: |\n')[1]
  assert.ok(script, `Missing shell block: ${name}`)
  return script
    .split('\n')
    .map((line) => line.slice(10))
    .join('\n')
}

function fixture(t: TestContext, version = '1.2.3') {
  const root = mkdtempSync(join(tmpdir(), 'paseo-agy-release-'))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  mkdirSync(join(root, 'bin'))
  writeFileSync(
    join(root, 'package.json'),
    JSON.stringify({ name: '@fdkevin/paseo-agy-acp', version }),
  )
  for (const [command, script] of Object.entries({
    curl: 'printf "curl %s\\n" "$*" >> "$CALLS_FILE"\nprintf "%s" "$HTTP_STATUS"\nexit "${CURL_EXIT:-0}"\n',
    gh: 'printf "gh %s\\n" "$*" >> "$CALLS_FILE"\ncase "$2" in\nview) exit "$VIEW_EXIT" ;;\ncreate) exit "$CREATE_EXIT" ;;\n*) exit 99 ;;\nesac\n',
    npm: 'printf "npm %s\\n" "$*" >> "$CALLS_FILE"\nDEFAULT=\'[{"files":[{"path":"npm-shrinkwrap.json"}]}]\'\nprintf "%s" "${NPM_STDOUT:-$DEFAULT}"\nexit "${NPM_EXIT:-0}"\n',
  })) {
    writeFileSync(join(root, 'bin', command), `#!/bin/sh\n${script}`, {
      mode: 0o755,
    })
  }
  return {
    root,
    run(name: string, env: Record<string, string> = {}) {
      const output = join(root, 'output')
      const calls = join(root, 'calls')
      writeFileSync(output, '')
      writeFileSync(calls, '')
      const result = spawnSync(
        'bash',
        ['-e', '-o', 'pipefail', '-c', scriptFor(name)],
        {
          cwd: root,
          encoding: 'utf8',
          timeout: 10_000,
          env: isolatedEnv(root, {
            PATH: `${join(root, 'bin')}:${dirname(process.execPath)}:/usr/bin:/bin`,
            GITHUB_OUTPUT: output,
            CALLS_FILE: calls,
            GITHUB_REF_TYPE: 'tag',
            GITHUB_REF_NAME: `v${version}`,
            HTTP_STATUS: '404',
            CURL_EXIT: '0',
            VIEW_EXIT: '0',
            CREATE_EXIT: '0',
            NPM_EXIT: '0',
            ...env,
          }),
        },
      )
      assert.ifError(result.error)
      return {
        ...result,
        output: readFileSync(output, 'utf8'),
        calls: readFileSync(calls, 'utf8'),
      }
    },
  }
}

const preflight = 'Validate release tag and check npm registry'

function assertTagRejected(
  run: ReturnType<typeof fixture>['run'],
  env: Record<string, string>,
) {
  const result = run(preflight, env)
  assert.notEqual(result.status, 0)
  assert.match(result.stdout, /Run this workflow on the existing v1\.2\.3 tag/)
  assert.equal(result.output, '')
  assert.equal(result.calls, '')
}

function assertVersionRejected(t: TestContext, version: string) {
  const result = fixture(t, version).run(preflight)
  assert.notEqual(result.status, 0)
  assert.match(result.stdout, /stable versions only/)
  assert.equal(result.calls, '')
  assert.equal(result.output, '')
}

function assertVersionAccepted(t: TestContext, version: string) {
  const result = fixture(t, version).run(preflight)
  assert.equal(result.status, 0, result.stderr)
  assert.equal(result.output, 'published=false\n')
}

function assertRegistryStatus(
  run: ReturnType<typeof fixture>['run'],
  status: string,
  published: string,
) {
  const result = run(preflight, { HTTP_STATUS: status })
  assert.equal(result.status, 0, result.stderr)
  assert.equal(result.output, `published=${published}\n`)
  assert.equal(
    result.calls,
    'curl --silent --show-error --max-time 30 --output /dev/null --write-out %{http_code} https://registry.npmjs.org/@fdkevin/paseo-agy-acp/1.2.3\n',
  )
}

function assertRegistryError(
  run: ReturnType<typeof fixture>['run'],
  env: Record<string, string>,
) {
  const result = run(preflight, env)
  assert.notEqual(result.status, 0)
  assert.equal(result.output, '')
}

test('release preflight rejects branch dispatch and tag mismatches before contacting npm', (t) => {
  const { run } = fixture(t)
  assertTagRejected(run, { GITHUB_REF_TYPE: 'branch', GITHUB_REF_NAME: 'main' })
  assertTagRejected(run, { GITHUB_REF_NAME: 'v9.9.9' })
  assertTagRejected(run, { GITHUB_REF_NAME: '1.2.3' })
})

test('release preflight accepts stable semver only', (t) => {
  assertVersionRejected(t, '1.2.3-rc.1')
  assertVersionRejected(t, '1.2.3+build')
  assertVersionRejected(t, '01.2.3')
  assertVersionRejected(t, '1.02.3')
  assertVersionRejected(t, '1.2.03')
  assertVersionRejected(t, '1.2')

  assertVersionAccepted(t, '0.0.0')
  assertVersionAccepted(t, '1.2.3')
  assertVersionAccepted(t, '10.20.30')
})

test('only registry 404 allows publication; HTTP errors and timeouts fail closed', (t) => {
  const { run } = fixture(t)
  assertRegistryStatus(run, '200', 'true')
  assertRegistryStatus(run, '404', 'false')

  assertRegistryError(run, { HTTP_STATUS: '301' })
  assertRegistryError(run, { HTTP_STATUS: '401' })
  assertRegistryError(run, { HTTP_STATUS: '403' })
  assertRegistryError(run, { HTTP_STATUS: '429' })
  assertRegistryError(run, { HTTP_STATUS: '500' })
  assertRegistryError(run, { HTTP_STATUS: '000', CURL_EXIT: '28' })
  assertRegistryError(run, { HTTP_STATUS: '404', CURL_EXIT: '7' })
})

test('npm manifest preparation removes only build and remains repeatable', (t) => {
  const { root, run } = fixture(t)
  const source = JSON.parse(
    readFileSync(new URL('../paseo-plugin.json', import.meta.url), 'utf8'),
  )
  assert.ok(source.build, 'Git installation needs its preparation command')
  const manifest = { ...source, future: { build: 'preserve nested fields' } }
  const path = join(root, 'paseo-plugin.json')
  writeFileSync(path, JSON.stringify(manifest))
  const { build: _build, ...expected } = manifest
  const first = run('Prepare npm manifest')
  assert.equal(first.status, 0, first.stderr)
  assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')), expected)
  assert.ok(readFileSync(path, 'utf8').endsWith('\n'))
  assert.equal(first.calls, 'npm pack --dry-run --json --ignore-scripts\n')
  const second = run('Prepare npm manifest')
  assert.equal(second.status, 0, second.stderr)
  assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')), expected)
  assert.ok(readFileSync(path, 'utf8').endsWith('\n'))
  assert.equal(second.calls, 'npm pack --dry-run --json --ignore-scripts\n')
  const missingShrinkwrap = run('Prepare npm manifest', {
    NPM_STDOUT: '[{"files":[{"path":"package.json"}]}]',
  })
  assert.notEqual(missingShrinkwrap.status, 0)
  assert.match(
    missingShrinkwrap.stderr,
    /npm pack omitted npm-shrinkwrap\.json/,
  )
  assert.notEqual(run('Prepare npm manifest', { NPM_EXIT: '1' }).status, 0)
  writeFileSync(path, '{invalid')
  const invalid = run('Prepare npm manifest')
  assert.notEqual(invalid.status, 0)
  assert.equal(invalid.calls, '')
})

test('GitHub Release recovery reuses the existing tag and propagates create failures', (t) => {
  const { run } = fixture(t)
  const existing = run('Ensure GitHub Release exists')
  assert.equal(existing.status, 0)
  assert.equal(existing.calls, 'gh release view v1.2.3\n')
  const recovered = run('Ensure GitHub Release exists', { VIEW_EXIT: '1' })
  assert.equal(recovered.status, 0)
  assert.equal(
    recovered.calls,
    'gh release view v1.2.3\ngh release create v1.2.3 --verify-tag --title v1.2.3 --generate-notes\n',
  )
  assert.notEqual(
    run('Ensure GitHub Release exists', { VIEW_EXIT: '1', CREATE_EXIT: '1' })
      .status,
    0,
  )
})
