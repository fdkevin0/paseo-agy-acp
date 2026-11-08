import { readFileSync, writeFileSync } from 'node:fs'

writeFileSync(
  process.env.AGY_USAGE_ARGS!,
  JSON.stringify(process.argv.slice(2)),
)
if (process.env.AGY_USAGE_FAIL) {
  process.stderr.write('Authentication required')
  process.exit(1)
}
process.stdout.write(readFileSync(process.env.AGY_USAGE_FIXTURE!, 'utf8'))
