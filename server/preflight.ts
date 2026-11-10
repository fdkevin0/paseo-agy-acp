/** Executable discovery and narrowly scoped cleanup of disposable catalog sessions. */
import { createHash } from 'node:crypto'
import { realpathSync } from 'node:fs'
import { lstat, rm } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { execCommand } from '@getpaseo/plugin/server'
import type {
  ProviderLaunch,
  ProviderStatus,
} from '@getpaseo/plugin/server/provider'
import { formatErrorMessage, sessionIdPattern, setBounded } from './common.ts'

const STATUS_CACHE_LIMIT = 100
const statusCache = new Map<string, number>()

/** Verify that the configured executable supports the required ACP version. */
export async function providerStatus(
  launch?: ProviderLaunch,
): Promise<ProviderStatus> {
  const key = catalogCacheKey(launch)
  const cached = statusCache.get(key)
  if (cached && Date.now() - cached < 60_000) return { available: true }
  const cmd = launch?.command ?? 'agy'
  try {
    const { stdout, stderr } = await execCommand(cmd, ['--version'], {
      env: launch?.env ?? process.env,
      timeout: 5000,
    })
    const version = `${stdout}\n${stderr}`.match(
      /\bv?(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?\b/,
    )
    if (!version)
      return {
        available: false,
        diagnostic: 'Could not determine agy version; requires >=1.5.3.',
      }
    const major = Number(version[1])
    const minor = Number(version[2])
    const patch = Number(version[3])
    const prerelease = version[4]
    const supported =
      major > 1 ||
      (major === 1 &&
        (minor > 5 ||
          (minor === 5 && (patch > 3 || (patch === 3 && !prerelease)))))
    if (supported) {
      setBounded(statusCache, key, Date.now(), STATUS_CACHE_LIMIT)
      return { available: true }
    }
    return {
      available: false,
      diagnostic: `agy ${version[0]} is too old; requires >=1.5.3. Run agy update.`,
    }
  } catch (error) {
    const target = cmd === 'agy' ? 'agy --version' : `agy (${cmd} --version)`
    return {
      available: false,
      diagnostic: `Cannot run ${target}: ${formatErrorMessage(error)}`,
    }
  }
}

/** Include the full launch environment without exposing its values in the cache key. */
export function catalogCacheKey(launch?: ProviderLaunch): string {
  const env = Object.entries(launch?.env ?? process.env).sort(([a], [b]) =>
    a.localeCompare(b),
  )
  return createHash('sha256')
    .update(
      JSON.stringify([launch?.command ?? 'agy', launch?.args ?? ['acp'], env]),
    )
    .digest('hex')
}

/** Resolve the native data directory using agy's documented directory flags. */
export function dataDirectory(
  launch?: ProviderLaunch,
  env: Readonly<Record<string, string>> = {},
  cwd = process.cwd(),
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
  let baseCwd = cwd
  try {
    baseCwd = realpathSync.native(cwd)
  } catch {
    /* Fall back to lexical cwd when the workspace path does not exist yet. */
  }
  const home = env.HOME ?? launch?.env?.HOME ?? homedir()
  const gemini = flag(['--gemini-dir', '--gemini_dir'], join(home, '.gemini'))
  const app = flag(['--app-data-dir', '--app_data_dir'], 'antigravity-cli')
  return resolve(baseCwd, gemini, app)
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
