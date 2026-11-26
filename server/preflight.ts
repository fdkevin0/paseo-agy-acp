/** Executable discovery and narrowly scoped cleanup of disposable catalog sessions. */
import { createHash } from 'node:crypto'
import { constants, realpathSync } from 'node:fs'
import { access, lstat, realpath, rm, stat } from 'node:fs/promises'
import { delimiter, extname, isAbsolute, join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { execCommand } from '@getpaseo/plugin/server'
import type {
  ProviderLaunch,
  ProviderStatus,
} from '@getpaseo/plugin/server/provider'
import {
  formatErrorMessage,
  isMissing,
  mergeEnv,
  sessionIdPattern,
  setBounded,
} from './common.ts'

const STATUS_CACHE_LIMIT = 100
const STATUS_CACHE_TTL_MS = 60_000
const statusCache = new Map<string, number>()
const pendingStatuses = new Map<string, Promise<ProviderStatus>>()

// agy 1.5.16+ returns -32002 for missing session/load, keeps turns open across
// non-daemon background wakeups, and forwards child-session permissions/questions.
const MIN_AGY_VERSION = '1.5.16'
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

const DEFAULT_WINDOWS_PATHEXT = ['.exe', '.cmd', '.bat', '.com']

interface BinaryFingerprintOptions {
  platform?: NodeJS.Platform
  fallbackHome?: string
  env?: Readonly<Record<string, string>>
}

function readEnv(
  env: Readonly<Record<string, string | undefined>> | undefined,
  name: string,
): string | undefined {
  if (!env) return undefined
  const direct = env[name]
  if (direct) return direct
  const lower = name.toLowerCase()
  for (const [key, value] of Object.entries(env)) {
    if (key.toLowerCase() === lower && value) return value
  }
  return undefined
}

/** Resolve the effective home directory (`HOME`, then `USERPROFILE`, ignoring empty strings). */
export function resolveHome(
  env: Readonly<Record<string, string | undefined>> | undefined,
  launchEnv?: Readonly<Record<string, string | undefined>>,
  fallbackHome?: string,
): string {
  return (
    readEnv(env, 'HOME') ??
    readEnv(env, 'USERPROFILE') ??
    readEnv(launchEnv, 'HOME') ??
    readEnv(launchEnv, 'USERPROFILE') ??
    (fallbackHome || homedir())
  )
}

function windowsPathExts(
  env: Readonly<Record<string, string | undefined>>,
): string[] {
  const raw = readEnv(env, 'PATHEXT')
  if (!raw) return DEFAULT_WINDOWS_PATHEXT
  const exts: string[] = []
  for (const entry of raw.split(';')) {
    const cleaned = entry.trim().toLowerCase()
    if (!cleaned) continue
    const ext = cleaned.startsWith('.') ? cleaned : `.${cleaned}`
    if (!exts.includes(ext)) exts.push(ext)
  }
  if (!exts.includes('.exe')) exts.unshift('.exe')
  return exts
}

function windowsCandidates(
  basePath: string,
  exts: readonly string[],
): string[] {
  const lower = basePath.toLowerCase()
  if (exts.some((ext) => lower.endsWith(ext))) return [basePath]
  if (extname(basePath))
    return [basePath, ...exts.map((ext) => `${basePath}${ext}`)]
  return [...exts.map((ext) => `${basePath}${ext}`), basePath]
}

function stripOuterQuotes(entry: string, isWindows: boolean): string {
  const raw = isWindows ? entry.trim() : entry
  return raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"')
    ? raw.slice(1, -1)
    : raw
}

async function fingerprintPath(
  path: string,
  isWindows: boolean,
): Promise<string | undefined> {
  try {
    // POSIX requires execute permission; Windows lacks POSIX X_OK semantics so stat().isFile() is authoritative.
    if (!isWindows) await access(path, constants.X_OK)
    const real = await realpath(path)
    const info = await stat(real, { bigint: true })
    if (!info.isFile()) return undefined
    return `${real}:${info.dev}:${info.ino}:${info.size}:${info.mtimeNs}`
  } catch {
    return undefined
  }
}

async function fingerprintCandidates(
  basePath: string,
  isWindows: boolean,
  exts: readonly string[],
): Promise<string | undefined> {
  if (!isWindows) return fingerprintPath(basePath, false)
  for (const candidate of windowsCandidates(basePath, exts)) {
    const found = await fingerprintPath(candidate, true)
    if (found) return found
  }
  return undefined
}

function defaultAgyInstallPaths(
  env: Readonly<Record<string, string | undefined>>,
  isWindows: boolean,
  fallbackHome: string,
): string[] {
  const home = resolveHome(env, undefined, fallbackHome)
  if (!isWindows) return home ? [join(home, '.local', 'bin', 'agy')] : []
  const paths: string[] = []
  const localAppData = readEnv(env, 'LOCALAPPDATA')
  if (localAppData) paths.push(join(localAppData, 'agy', 'bin', 'agy.exe'))
  if (home) paths.push(join(home, 'AppData', 'Local', 'agy', 'bin', 'agy.exe'))
  return [...new Set(paths)]
}

/** Resolve a stable fingerprint for the launch executable (including optional session env overrides). */
export async function binaryFingerprint(
  launch?: ProviderLaunch,
  options?: BinaryFingerprintOptions,
): Promise<string | undefined> {
  const platform = options?.platform ?? process.platform
  const isWindows = platform === 'win32'
  const pathDelimiter = options?.platform ? (isWindows ? ';' : ':') : delimiter
  const env = mergeEnv(launch?.env ?? process.env, options?.env, isWindows)
  const exts = isWindows ? windowsPathExts(env) : []
  const cmd = launch?.command ?? 'agy'
  if (isAbsolute(cmd) || cmd.includes('/') || cmd.includes('\\'))
    return fingerprintCandidates(cmd, isWindows, exts)
  const pathEnv = readEnv(env, 'PATH') ?? ''
  for (const entry of pathEnv.split(pathDelimiter)) {
    const dir = stripOuterQuotes(entry, isWindows)
    if (!dir) continue
    const found = await fingerprintCandidates(join(dir, cmd), isWindows, exts)
    if (found) return found
  }
  const lower = cmd.toLowerCase()
  if (
    cmd === 'agy' ||
    (isWindows && (lower === 'agy' || lower === 'agy.exe'))
  ) {
    for (const candidate of defaultAgyInstallPaths(
      env,
      isWindows,
      options?.fallbackHome ?? homedir(),
    )) {
      const found = await fingerprintPath(candidate, isWindows)
      if (found) return found
    }
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

/** Canonical SHA-256 key covering command, args, resolved environment, and optional binary fingerprint. */
export function hashLaunchKey(
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
  fingerprint?: string,
): string {
  const merged = mergeEnv(launch?.env ?? process.env, env)
  const sortedEnv = Object.entries(merged).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  )
  return createHash('sha256')
    .update(
      JSON.stringify([
        launch?.command ?? 'agy',
        launch?.args ?? ['acp'],
        sortedEnv,
        ...(fingerprint !== undefined ? [fingerprint] : []),
      ]),
    )
    .digest('hex')
}

/** Include the full launch environment without exposing its values in the cache key. */
export function catalogCacheKey(
  launch?: ProviderLaunch,
  fingerprint?: string,
): string {
  return hashLaunchKey(launch, {}, fingerprint || undefined)
}

/** Include the resolved executable fingerprint so binary upgrades invalidate host catalog caches. */
export async function resolveCatalogCacheKey(
  launch?: ProviderLaunch,
  options?: BinaryFingerprintOptions,
): Promise<string> {
  return catalogCacheKey(launch, await binaryFingerprint(launch, options))
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
  const home = resolveHome(env, launch?.env, fallbackHome)
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
  const fallback = resolveHome(
    env,
    launch?.env,
    options?.fallbackHome ?? homedir(),
  )
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

async function realDirectory(path: string): Promise<boolean> {
  try {
    return (await lstat(path)).isDirectory()
  } catch (error) {
    if (isMissing(error)) return false
    throw error
  }
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
        if (isMissing(error)) continue
        throw error
      }
      if (stat.isSymbolicLink())
        throw new Error(`Refusing to remove symlink: ${path}`)
      await rm(path, { recursive: stat.isDirectory(), force: true })
    }
  }
}
