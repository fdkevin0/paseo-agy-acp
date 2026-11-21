/** Executable discovery and narrowly scoped cleanup of disposable catalog sessions. */
import { createHash } from 'node:crypto'
import { constants, realpathSync } from 'node:fs'
import { access, lstat, realpath, rm, stat } from 'node:fs/promises'
import { delimiter, isAbsolute, join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { execCommand } from '@getpaseo/plugin/server'
import type {
  ProviderLaunch,
  ProviderStatus,
} from '@getpaseo/plugin/server/provider'
import { formatErrorMessage, sessionIdPattern, setBounded } from './common.ts'

const STATUS_CACHE_LIMIT = 100
const STATUS_CACHE_TTL_MS = 60_000
const statusCache = new Map<string, number>()
const pendingStatuses = new Map<string, Promise<ProviderStatus>>()

// agy 1.5.10 streams Reactive Wakeup output after session/prompt returns.
const MIN_AGY_VERSION = '1.5.10'
const agyVersionPattern =
  /v?(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?/
const agyVersionOutputPattern = new RegExp(`\\b${agyVersionPattern.source}\\b`)

export function agyVersionDiagnostic(version?: string): string | undefined {
  const match = version?.match(agyVersionPattern)
  if (!match || match[0] !== version)
    return `Could not determine agy version; requires >=${MIN_AGY_VERSION}.`
  const [, major, minor, patch, prerelease] = match
  const [minMajor, minMinor, minPatch] = MIN_AGY_VERSION.split('.').map(Number)
  const order =
    Number(major) - minMajor ||
    Number(minor) - minMinor ||
    Number(patch) - minPatch
  if (order < 0 || (order === 0 && prerelease))
    return `agy ${version} is too old; requires >=${MIN_AGY_VERSION}. Run agy update.`
}

async function fingerprintPath(path: string): Promise<string | undefined> {
  try {
    await access(path, constants.X_OK)
    const real = await realpath(path)
    const info = await stat(real, { bigint: true })
    if (!info.isFile()) return undefined
    return `${real}:${info.dev}:${info.ino}:${info.size}:${info.mtimeNs}`
  } catch {
    return undefined
  }
}

async function binaryFingerprint(
  launch?: ProviderLaunch,
): Promise<string | undefined> {
  const cmd = launch?.command ?? 'agy'
  if (isAbsolute(cmd) || cmd.includes('/') || cmd.includes('\\'))
    return fingerprintPath(cmd)
  const pathEnv = (launch?.env ?? process.env).PATH ?? ''
  for (const dir of pathEnv.split(delimiter)) {
    if (!dir) continue
    const found = await fingerprintPath(join(dir, cmd))
    if (found) return found
  }
  return undefined
}

async function probeProviderStatus(
  launch: ProviderLaunch | undefined,
  key: string,
): Promise<ProviderStatus> {
  const cmd = launch?.command ?? 'agy'
  try {
    const { stdout, stderr } = await execCommand(cmd, ['--version'], {
      env: launch?.env ?? process.env,
      timeout: 5000,
    })
    const version = `${stdout}\n${stderr}`.match(agyVersionOutputPattern)?.[0]
    const diagnostic = agyVersionDiagnostic(version)
    if (diagnostic) {
      statusCache.delete(key)
      return { available: false, diagnostic }
    }
    setBounded(statusCache, key, Date.now(), STATUS_CACHE_LIMIT)
    return { available: true }
  } catch (error) {
    statusCache.delete(key)
    const target = cmd === 'agy' ? 'agy --version' : `agy (${cmd} --version)`
    return {
      available: false,
      diagnostic: `Cannot run ${target}: ${formatErrorMessage(error)}`,
    }
  }
}

/** Verify that the configured executable supports the required ACP version. */
export async function providerStatus(
  launch?: ProviderLaunch,
): Promise<ProviderStatus> {
  const key = await resolveCatalogCacheKey(launch)
  const cached = statusCache.get(key)
  if (cached && Date.now() - cached < STATUS_CACHE_TTL_MS)
    return { available: true }
  const inFlight = pendingStatuses.get(key)
  if (inFlight) return inFlight
  const probe = probeProviderStatus(launch, key)
  setBounded(pendingStatuses, key, probe, STATUS_CACHE_LIMIT)
  const clear = () => {
    if (pendingStatuses.get(key) === probe) pendingStatuses.delete(key)
  }
  void probe.then(clear, clear)
  return probe
}

/** Include the full launch environment without exposing its values in the cache key. */
export function catalogCacheKey(
  launch?: ProviderLaunch,
  fingerprint?: string,
): string {
  const env = Object.entries(launch?.env ?? process.env).sort(([a], [b]) =>
    a.localeCompare(b),
  )
  const parts: unknown[] = [
    launch?.command ?? 'agy',
    launch?.args ?? ['acp'],
    env,
  ]
  if (fingerprint) parts.push(fingerprint)
  return createHash('sha256').update(JSON.stringify(parts)).digest('hex')
}

/** Include the resolved executable fingerprint so binary upgrades invalidate host catalog caches. */
export async function resolveCatalogCacheKey(
  launch?: ProviderLaunch,
): Promise<string> {
  return catalogCacheKey(launch, await binaryFingerprint(launch))
}

/** Resolve the native data directory using agy's documented directory flags. */
export function dataDirectory(
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
  cwd = process.cwd(),
  fallbackHome = homedir(),
): string {
  const args = launch?.args ?? []
  function flag(names: string[], fallback: string): string {
    let value = fallback
    for (let i = 0; i < args.length; i++) {
      for (const name of names) {
        const next = args[i + 1]
        if (args[i] === name && next && !next.startsWith('-')) value = next
        else if (args[i].startsWith(`${name}=`)) {
          const inline = args[i].slice(name.length + 1)
          if (inline) value = inline
        }
      }
    }
    return value
  }
  const home = env.HOME ?? launch?.env?.HOME ?? fallbackHome
  const gemini = flag(['--gemini-dir', '--gemini_dir'], join(home, '.gemini'))
  const app = flag(['--app-data-dir', '--app_data_dir'], 'antigravity-cli')
  if (isAbsolute(gemini)) {
    return resolve(gemini, app)
  }
  let baseCwd = cwd
  try {
    baseCwd = realpathSync.native(cwd)
  } catch {
    /* Fall back to lexical cwd when the workspace path does not exist yet. */
  }
  return resolve(baseCwd, gemini, app)
}

async function isExistingDirectory(path: string): Promise<boolean> {
  const info = await stat(path).catch(() => null)
  return Boolean(info?.isDirectory())
}

/** Whether launching `agy` from a different cwd preserves executable and data paths. */
export function isCwdIndependentLaunch(
  cwd: string,
  fallbackCwd: string,
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
  fallbackHome = homedir(),
): boolean {
  const cmd = launch?.command ?? 'agy'
  if (!isAbsolute(cmd) && /[/\\]/.test(cmd)) return false
  return (
    dataDirectory(launch, env, fallbackCwd, fallbackHome) ===
    dataDirectory(launch, env, cwd, fallbackHome)
  )
}

/**
 * Resolve an existing spawn directory, falling back to HOME only when allowed
 * and the launch does not depend on relative executable or `--gemini-dir` paths.
 */
export async function resolveSpawnCwd(
  cwd: string | undefined,
  launch?: ProviderLaunch,
  options?: {
    env?: Readonly<Record<string, string>>
    fallbackHome?: string
    allowMissingCwd?: boolean
  },
): Promise<string> {
  const env = options?.env ?? {}
  const fallback =
    env.HOME ?? launch?.env?.HOME ?? options?.fallbackHome ?? homedir()
  const target = cwd ?? fallback
  if (await isExistingDirectory(target)) return target
  if (
    options?.allowMissingCwd &&
    cwd !== undefined &&
    (await isExistingDirectory(fallback)) &&
    isCwdIndependentLaunch(cwd, fallback, launch, env, options.fallbackHome)
  )
    return fallback
  throw new Error(`Working directory does not exist: ${target}`)
}

function isMissingOrNonDir(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code
  return code === 'ENOENT' || code === 'ENOTDIR'
}

async function realDirectory(path: string): Promise<boolean> {
  try {
    return (await lstat(path)).isDirectory()
  } catch (error) {
    if (isMissingOrNonDir(error)) return false
    throw error
  }
}

/** Whether a session has local conversation database or Brain directory artifacts. */
export async function hasLocalSessionArtifacts(
  appData: string,
  sessionId: string,
): Promise<boolean> {
  if (!sessionIdPattern.test(sessionId) || !(await realDirectory(appData)))
    return false
  for (const [directory, name, kind] of [
    [join(appData, 'conversations'), `${sessionId}.db`, 'file'],
    [join(appData, 'brain'), sessionId, 'directory'],
  ] as const) {
    if (!(await realDirectory(directory))) continue
    try {
      const info = await lstat(join(directory, name))
      if (kind === 'file' ? info.isFile() : info.isDirectory()) return true
    } catch (error) {
      if (isMissingOrNonDir(error)) continue
      throw error
    }
  }
  return false
}

/** Delete only the UUID returned by a disposable, never-prompted, now-closed session. */
export async function removeSessionArtifacts(
  appData: string,
  sessionId: string,
): Promise<void> {
  if (!sessionIdPattern.test(sessionId))
    throw new Error('Unsafe catalog session ID')
  if (!(await realDirectory(appData))) return
  for (const [directory, names] of [
    [
      join(appData, 'conversations'),
      [`${sessionId}.db`, `${sessionId}.db-wal`, `${sessionId}.db-shm`],
    ],
    [join(appData, 'brain'), [sessionId]],
  ] as const) {
    if (!(await realDirectory(directory))) continue
    for (const name of names) {
      const path = join(directory, name)
      let stat
      try {
        stat = await lstat(path)
      } catch (error) {
        if (isMissingOrNonDir(error)) continue
        throw error
      }
      if (stat.isSymbolicLink())
        throw new Error(`Refusing to remove symlink: ${path}`)
      await rm(path, { recursive: stat.isDirectory(), force: true })
    }
  }
}
