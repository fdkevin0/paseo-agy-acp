/** Subscription quota obtained through agy's local /usage command, without a model turn. */
import { open, readdir, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { z } from 'zod'
import { execCommand } from '@getpaseo/plugin/server'
import type { ProviderLaunch } from '@getpaseo/plugin/server/provider'
import {
  hashAccountKey,
  unavailable,
  windowFromReportedDuration,
  toneFromUsedPct,
  type UsageAccount,
  type UsageReport,
  type UsageScope,
  type UsageSourceRegistration,
} from '@getpaseo/plugin/server/usage'
import { dataDirectory } from './preflight.ts'

const usageInputSchema = z
  .object({
    dataDir: z.string().min(1),
    command: z.string().min(1).optional(),
    args: z.array(z.string()).optional(),
  })
  .strict()

const groupsSchema = z.object({
  command: z.object({
    data: z.object({
      groups: z.array(
        z.object({
          name: z.string().min(1),
          buckets: z.array(
            z.object({
              id: z.string().optional(),
              name: z.string().optional(),
              window: z.string(),
              remaining_fraction: z.number().finite().nullish(),
              reset_time: z.string().nullish(),
            }),
          ),
        }),
      ),
    }),
  }),
})

/** Validate the CLI envelope before translating its quota groups. */
export function parseUsage(stdout: string) {
  return groupsSchema
    .parse(JSON.parse(stdout))
    .command.data.groups.map((group) => ({
      name: group.name,
      buckets: group.buckets.map((bucket) => ({
        id: bucket.id ?? bucket.window,
        name: bucket.name ?? bucket.window,
        window: bucket.window,
        remainingFraction: bucket.remaining_fraction ?? undefined,
        resetTime: bucket.reset_time ?? '',
      })),
    }))
}

/** Read only bounded log tails, preferring the most recently modified login log. */
export async function readLoggedEmail(
  dataDir: string,
): Promise<string | undefined> {
  let names: string[] = []
  try {
    names = (await readdir(join(dataDir, 'log'))).filter((name) =>
      /^cli-.*\.log$/.test(name),
    )
  } catch {
    /* Logs are optional. */
  }
  const recent = await Promise.all(
    names.map(async (name) => {
      const path = join(dataDir, 'log', name)
      return {
        path,
        modified: await stat(path).then(
          (s) => s.mtimeMs,
          () => 0,
        ),
      }
    }),
  )
  const paths = recent.sort((a, b) => b.modified - a.modified).slice(0, 10)
  const main = join(dataDir, 'cli.log')
  paths.push({
    path: main,
    modified: await stat(main).then(
      (s) => s.mtimeMs,
      () => 0,
    ),
  })
  for (const { path } of paths.sort((a, b) => b.modified - a.modified)) {
    try {
      const file = await open(path, 'r')
      try {
        const size = (await file.stat()).size
        const bytes = Buffer.alloc(Math.min(size, 64 * 1024))
        const { bytesRead } = await file.read(
          bytes,
          0,
          bytes.length,
          size - bytes.length,
        )
        const matches = [
          ...bytes
            .toString('utf8', 0, bytesRead)
            .matchAll(
              /(?:applyAuthResult:\s*email=|OAuth:\s*authenticated successfully as\s+)([^\s,]+)/g,
            ),
        ]
        if (matches.length) return matches.at(-1)![1]
      } finally {
        await file.close()
      }
    } catch {
      /* Missing or unreadable logs do not prevent quota checks. */
    }
  }
}

async function hasLogin(dataDir: string): Promise<boolean> {
  return stat(join(dataDir, 'antigravity-oauth-token')).then(
    (s) => s.isFile() && s.size > 0,
    () => false,
  )
}

function usageArgs(args: readonly string[]): string[] {
  const result = [...args]
  const acp = result.indexOf('acp')
  if (acp >= 0) result.splice(acp, 1)
  return result.filter(
    (arg) => !['--stdio', '--worker', '--process-per-session'].includes(arg),
  )
}

/** Share native launch/account observations with Paseo's subscription usage surface. */
export class AgyUsageSource {
  readonly tiers = new Map<string, string>()
  private launches = new Map<
    string,
    {
      launch?: ProviderLaunch
      env: Record<string, string>
      cwd?: string
      hadLogin: boolean
    }
  >()

  remember(
    launch?: ProviderLaunch,
    env: Readonly<Record<string, string>> = {},
    cwd?: string,
  ): string {
    const dir = dataDirectory(launch, env, cwd)
    this.launches.set(dir, {
      launch,
      env: { ...launch?.env, ...env },
      cwd,
      hadLogin: this.launches.get(dir)?.hadLogin ?? false,
    })
    return dir
  }

  async discover(scope: UsageScope): Promise<UsageAccount[]> {
    if (scope.kind === 'session' && scope.provider !== 'agy') return []
    let dirs: string[]
    if (scope.kind === 'global')
      dirs = [...this.launches.keys(), dataDirectory()]
    else {
      // shortcut: scope omits launch commands; use the latest match until Paseo exposes a launch identity.
      const observed = [...this.launches.entries()]
        .reverse()
        .find(
          ([dir, value]) =>
            dataDirectory(value.launch, scope.env, value.cwd) === dir,
        )
      dirs = [observed?.[0] ?? dataDirectory(undefined, scope.env)]
    }
    const accounts: UsageAccount[] = []
    for (const dir of new Set(dirs)) {
      let known = this.launches.get(dir)
      const loggedIn = await hasLogin(dir)
      if (!loggedIn && !known) continue
      if (!known) {
        this.remember(undefined, scope.kind === 'session' ? scope.env : {})
        known = this.launches.get(dir)
      }
      if (known && loggedIn) known.hadLogin = true
      accounts.push({
        key: hashAccountKey(dir),
        label: (await readLoggedEmail(dir)) ?? 'Antigravity',
        harness: 'Antigravity',
        input: {
          dataDir: dir,
          command: known?.launch?.command ?? 'agy',
          args: usageArgs(known?.launch?.args ?? []),
        },
      })
    }
    return accounts
  }

  async fetch(rawInput: unknown): Promise<UsageReport> {
    const { dataDir, command, args } = usageInputSchema.parse(rawInput)
    const known = this.launches.get(dataDir)
    const loggedIn = await hasLogin(dataDir)
    if (!loggedIn && (!known || known.hadLogin))
      throw new Error('Antigravity login store no longer exists')
    if (known && loggedIn) known.hadLogin = true
    try {
      const { stdout } = await execCommand(
        command ?? 'agy',
        [...usageArgs(args ?? []), '-p', '/usage', '--output-format', 'json'],
        {
          timeout: 15_000,
          env: known ? { ...process.env, ...known.env } : process.env,
          cwd: known?.cwd,
        },
      )
      const windows = parseUsage(stdout).flatMap((group) =>
        group.buckets.map((bucket) => {
          const utilizationPct =
            bucket.remainingFraction === undefined
              ? null
              : Math.round(
                  Math.max(0, Math.min(1, 1 - bucket.remainingFraction)) * 100,
                )
          return windowFromReportedDuration({
            durationSeconds:
              bucket.window === '5h'
                ? 18_000
                : bucket.window === 'weekly'
                  ? 604_800
                  : null,
            scope: {
              id: group.name
                .toLowerCase()
                .replace(/[^a-z0-9]+/g, '_')
                .replace(/^_|_$/g, ''),
              label: group.name,
            },
            unknown: {
              id: bucket.id,
              label: bucket.name,
              shortLabel: bucket.window,
            },
            utilizationPct,
            resetsAt: bucket.resetTime || null,
            summary: true,
            tone: toneFromUsedPct(utilizationPct),
          })
        }),
      )
      if (!windows.length)
        return unavailable({
          kind: 'no_quota',
          detail: 'No usage quota reported',
        })
      return {
        status: 'available',
        planLabel: this.tiers.get(dataDir),
        windows,
      }
    } catch (error) {
      return {
        status: 'error',
        error: error instanceof Error ? error.message : String(error),
      }
    }
  }

  registration(): UsageSourceRegistration {
    return {
      id: 'agy',
      label: 'Antigravity',
      icon: 'icon.svg',
      input: usageInputSchema,
      discover: (scope) => this.discover(scope),
      fetch: (input) => this.fetch(input),
    }
  }
}
