/** Executable discovery and narrowly scoped cleanup of disposable catalog sessions. */
import { createHash } from 'node:crypto'
import { lstat, rm } from 'node:fs/promises'
import { dirname, join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { execCommand } from '@getpaseo/plugin/server'
import type {
  ProviderLaunch,
  ProviderStatus,
} from '@getpaseo/plugin/server/provider'
import { sessionIdPattern } from './mapping.ts'

const statusCache = new Map<string, number>()

/** Verify that the configured executable supports the required ACP version. */
export async function providerStatus(
  launch?: ProviderLaunch,
): Promise<ProviderStatus> {
  const key = catalogCacheKey(launch)
  const cached = statusCache.get(key)
  if (cached && Date.now() - cached < 60_000) return { available: true }
  try {
    const { stdout, stderr } = await execCommand(
      launch?.command ?? 'agy',
      ['--version'],
      {
        env: launch?.env ?? process.env,
        timeout: 5000,
      },
    )
    const version = (stdout.trim() || stderr.trim()).match(
      /\bv?(\d+)\.(\d+)\.(\d+)\b/,
    )
    if (!version)
      return {
        available: false,
        diagnostic: 'Could not determine agy version; requires >=1.4.12.',
      }
    const [major, minor, patch] = version.slice(1).map(Number)
    if (
      major > 1 ||
      (major === 1 && (minor > 4 || (minor === 4 && patch >= 12)))
    ) {
      statusCache.set(key, Date.now())
      return { available: true }
    }
    return {
      available: false,
      diagnostic: `agy ${version[0]} is too old; requires >=1.4.12. Run agy update.`,
    }
  } catch (error) {
    return {
      available: false,
      diagnostic: `Cannot run agy --version: ${String(error)}`,
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
        if (args[i] === name && args[i + 1]) value = args[i + 1]
        else if (args[i].startsWith(`${name}=`))
          value = args[i].slice(name.length + 1)
      }
    }
    return value
  }
  const home = env.HOME ?? launch?.env.HOME ?? homedir()
  const gemini = flag(['--gemini-dir', '--gemini_dir'], join(home, '.gemini'))
  const app = flag(['--app-data-dir', '--app_data_dir'], 'antigravity-cli')
  return resolve(cwd, gemini, app)
}

async function realDirectory(path: string): Promise<boolean> {
  const parent = dirname(path)
  if (parent !== path && !(await realDirectory(parent))) return false
  try {
    return (await lstat(path)).isDirectory()
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false
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
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue
        throw error
      }
      if (stat.isSymbolicLink())
        throw new Error(`Refusing to remove symlink: ${path}`)
      await rm(path, { recursive: stat.isDirectory(), force: true })
    }
  }
}
