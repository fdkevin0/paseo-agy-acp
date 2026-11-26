import { readFileSync, writeFileSync } from 'node:fs'
import { MessageChannel } from 'node:worker_threads'

writeFileSync(
  process.env.AGY_USAGE_ARGS!,
  JSON.stringify(process.argv.slice(2)),
)
if (process.env.AGY_USAGE_PID_FILE) {
  writeFileSync(process.env.AGY_USAGE_PID_FILE, String(process.pid))
}
if (process.env.AGY_USAGE_AUTH_HANG) {
  process.stderr.write(
    'Authentication required. Please visit the URL to log in:\n' +
      '  https://accounts.google.com/o/oauth2/auth?code_challenge=secret-pkce\n' +
      'Waiting for authentication (timeout 60s)...\n',
  )
  // Keep the process alive until aborted by the usage runner's auth-prompt detector.
  const keepAlive = new MessageChannel()
  keepAlive.port1.on('message', () => {})
} else if (process.env.AGY_USAGE_FAIL) {
  process.stderr.write('Authentication required')
  process.exit(1)
} else if (process.env.AGY_USAGE_EXIT_CODE) {
  if (process.env.AGY_USAGE_FIXTURE) {
    process.stdout.write(readFileSync(process.env.AGY_USAGE_FIXTURE, 'utf8'))
  }
  if (process.env.AGY_USAGE_STDERR) {
    process.stderr.write(process.env.AGY_USAGE_STDERR)
  }
  process.exit(Number(process.env.AGY_USAGE_EXIT_CODE))
} else {
  process.stdout.write(readFileSync(process.env.AGY_USAGE_FIXTURE!, 'utf8'))
}
