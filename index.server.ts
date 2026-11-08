import type { PluginServerContext } from '@getpaseo/plugin/server'
import { AgyUsageSource } from './server/usage.ts'
import { createProvider } from './server/provider.ts'

export default function contribute(server: PluginServerContext) {
  const usage = new AgyUsageSource()
  server.registerUsageSource(usage.registration())
  server.registerProvider(createProvider(usage))
  return () => {}
}
