import type { NativeToolIdentitySource } from './brain-projector.ts'
import type { ProviderTimelineItem } from '@getpaseo/plugin/server/provider'

export interface HistoryEntry {
  item: ProviderTimelineItem
  timestamp?: string
  resultProvided?: boolean
  nativeToolIdentitySource?: NativeToolIdentitySource
}

/** Place checkpoints only in gaps proved by native call IDs or unique complete text. */
export function mergeCompactionHistory(
  acp: readonly HistoryEntry[],
  brain: readonly HistoryEntry[],
): HistoryEntry[] {
  if (!brain.some(({ item }) => item.type === 'compaction'))
    return [
      ...acp,
      ...brain.filter(
        ({ item }) => item.type === 'tool_call' && item.status !== 'running',
      ),
    ]
  const textIds = new Map<string, number>()
  for (const { item } of acp)
    if ('text' in item) textIds.set(item.id, (textIds.get(item.id) ?? 0) + 1)
  function key(
    { item, nativeToolIdentitySource }: HistoryEntry,
    source: 'acp' | 'brain',
  ): string | undefined {
    if (item.type === 'tool_call')
      return source === 'acp' || nativeToolIdentitySource !== undefined
        ? JSON.stringify([
            'tool',
            item.callId,
            item.status === 'running' ? 'running' : 'terminal',
          ])
        : undefined
    if (
      item.type === 'user_message' ||
      item.type === 'assistant_message' ||
      item.type === 'reasoning'
    )
      return item.text ? JSON.stringify([item.type, item.text]) : undefined
    return undefined
  }
  function positions(
    entries: readonly HistoryEntry[],
    source: 'acp' | 'brain',
  ) {
    const result = new Map<string, number | null>()
    entries.forEach((entry, index) => {
      const { item } = entry
      if (source === 'acp' && 'text' in item && (textIds.get(item.id) ?? 0) > 1)
        return
      const identity = key(entry, source)
      if (identity !== undefined)
        result.set(identity, result.has(identity) ? null : index)
    })
    return result
  }
  const acpPositions = positions(acp, 'acp')
  const brainPositions = positions(brain, 'brain')
  const candidates = brain.flatMap((entry, index) => {
    const { item } = entry
    const identity = key(entry, 'brain')
    const position =
      identity === undefined ? undefined : acpPositions.get(identity)
    return position != null && brainPositions.get(identity!) === index
      ? [{ brain: index, acp: position, tool: item.type === 'tool_call' }]
      : []
  })
  const ordered = (anchors: typeof candidates) =>
    anchors.every((anchor, i) => i === 0 || anchors[i - 1].acp < anchor.acp)
  const tools = candidates.filter((anchor) => anchor.tool)
  // Text cannot override conflicting native call IDs. Conflicting call IDs prove no gap.
  const anchors = ordered(candidates) ? candidates : ordered(tools) ? tools : []
  const slots = new Map<number, HistoryEntry[]>()
  const trailing: HistoryEntry[] = []
  const completed = new Set<string>()
  const acpTools = new Map(
    acp.flatMap(({ item, resultProvided }, index) =>
      item.type === 'tool_call'
        ? [[item.callId, { item, index, resultProvided }] as const]
        : [],
    ),
  )
  let cursor = 0
  const insert = (index: number, entry: HistoryEntry) => {
    const entries = slots.get(index) ?? []
    entries.push(entry)
    slots.set(index, entries)
  }
  brain.forEach((entry, index) => {
    while (cursor < anchors.length && anchors[cursor].brain < index) cursor++
    const left = cursor > 0 ? anchors[cursor - 1].acp + 1 : 0
    const right = cursor < anchors.length ? anchors[cursor].acp : acp.length
    const { item } = entry
    if (item.type === 'compaction') {
      if (completed.has(item.id)) return
      completed.add(item.id)
      if (anchors.length > 0 && left === right) insert(left, entry)
      else
        trailing.push({
          item: {
            id: `${item.id}:unlocated`,
            type: 'notification',
            level: 'info',
            message:
              'History contains a context compaction whose exact position is unknown.',
          },
        })
    } else if (item.type === 'tool_call' && item.status !== 'running') {
      const existing = acpTools.get(item.callId)
      if (existing) {
        if (existing.item.status === 'running')
          insert(existing.index + 1, entry)
        else if (!existing.resultProvided)
          insert(existing.index + 1, {
            ...entry,
            item: { ...existing.item, detail: item.detail },
          })
        else if (
          existing.item.detail.type === 'sub_agent' &&
          item.detail.type === 'sub_agent' &&
          !existing.item.detail.childSessionId &&
          item.detail.childSessionId
        )
          insert(existing.index + 1, {
            item: {
              ...existing.item,
              detail: {
                ...existing.item.detail,
                childSessionId: item.detail.childSessionId,
              },
            },
          })
        return
      }
      // Retain Brain-only tool recovery even when ACP provides no placement anchor.
      if (left === right) insert(left, entry)
      else trailing.push(entry)
    }
  })
  return acp
    .flatMap((entry, index) => [...(slots.get(index) ?? []), entry])
    .concat(slots.get(acp.length) ?? [], trailing)
}
