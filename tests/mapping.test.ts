import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  Timeline,
  toolDetail,
  configState,
  contextUsage,
  promptUsage,
  contextWindowForModel,
  promptContent,
} from '../server/mapping.ts'

test('stream snapshots retain identity and split across tools/turns', () => {
  const t = new Timeline('s')
  const first = t.update({
    sessionUpdate: 'agent_message_chunk',
    content: { type: 'text', text: 'a' },
  })
  const second = t.update({
    sessionUpdate: 'agent_message_chunk',
    content: { type: 'text', text: 'b' },
  })
  assert.equal(first?.id, second?.id)
  assert.ok(second?.type === 'assistant_message' && second.text === 'ab')
  t.boundary()
  assert.notEqual(
    t.update({
      sessionUpdate: 'agent_message_chunk',
      content: { type: 'text', text: 'c' },
    })?.id,
    first?.id,
  )
})

test('subagent and diff details, grouped model choices', () => {
  const id = 'b3bbd901-0d20-4a21-aa53-99f4072fb378'
  assert.deepEqual(
    toolDetail({
      title: 'invoke_subagent',
      rawInput: { Role: 'review' },
      rawOutput: `Created: {"conversationId": "${id}"}`,
    }),
    {
      type: 'sub_agent',
      subAgentType: 'review',
      description: undefined,
      childSessionId: id,
      log: `Created: {"conversationId": "${id}"}`,
    },
  )
  assert.equal(
    toolDetail({
      content: [{ type: 'diff', path: '/test', oldText: 'a', newText: 'b' }],
    }).type,
    'edit',
  )
  assert.equal(
    configState({
      configOptions: [
        {
          id: 'model',
          name: 'Model',
          type: 'select',
          currentValue: 'm',
          options: [
            { group: 'g', name: 'G', options: [{ name: 'M', value: 'm' }] },
          ],
        },
      ],
    }).models[0]?.id,
    'm',
  )
})

test('unfinished tools terminalize once on every terminal state', () => {
  for (const status of ['completed', 'canceled', 'failed'] as const) {
    const timeline = new Timeline('s')
    timeline.update({
      sessionUpdate: 'tool_call',
      toolCallId: 'pending',
      title: 'run_command',
      status: 'in_progress',
      rawInput: { CommandLine: 'echo test' },
    })
    const items = timeline.terminalize(status, 'crashed')
    assert.equal(items.length, 1)
    assert.ok(items[0].type === 'tool_call' && items[0].status === status)
    if (items[0].type === 'tool_call')
      assert.equal(items[0].error, status === 'failed' ? 'crashed' : null)
    assert.deepEqual(timeline.terminalize(status), [])
  }
})

test('usage rejects invalid context, ignores foreign currency, and computes lastStep occupancy', () => {
  assert.equal(contextUsage({ size: 0, used: 10 }), undefined)
  assert.deepEqual(contextUsage({ size: 100, used: 0 }), {
    contextWindowUsedTokens: 0,
    contextWindowMaxTokens: 100,
  })
  assert.equal(
    promptUsage(null, {
      lastStep: { inputTokens: 0, outputTokens: 0, cachedReadTokens: 0 },
    }).contextWindowUsedTokens,
    undefined,
  )
  assert.equal(contextUsage({ size: 100, used: -1 }), undefined)
  assert.equal(contextUsage({ size: Infinity, used: 1 }), undefined)
  assert.deepEqual(
    contextUsage({ size: 100, used: 10, cost: { amount: 3, currency: 'EUR' } }),
    { contextWindowUsedTokens: 10, contextWindowMaxTokens: 100 },
  )
  assert.deepEqual(
    promptUsage({
      totalTokens: 6,
      inputTokens: 4,
      outputTokens: 2,
      cachedReadTokens: 3,
    }),
    {
      inputTokens: 4,
      outputTokens: 2,
      cachedInputTokens: 3,
    },
  )
  const usage = {
    inputTokens: 120,
    cachedReadTokens: 40,
    cachedWriteTokens: 20,
    outputTokens: 9,
    totalTokens: 189,
  }
  assert.deepEqual(
    promptUsage(usage, {
      model: 'gpt-oss-120b',
      lastStep: { inputTokens: 40, cachedReadTokens: 20, outputTokens: 9 },
    }),
    {
      inputTokens: 120,
      outputTokens: 9,
      cachedInputTokens: 40,
      contextWindowUsedTokens: 69,
      contextWindowMaxTokens: 131_072,
    },
  )
  assert.deepEqual(promptUsage(usage, { preserveContext: true }), {
    inputTokens: 120,
    outputTokens: 9,
    cachedInputTokens: 40,
  })
  assert.equal(
    promptUsage({ inputTokens: -1, outputTokens: NaN, totalTokens: 30 })
      .contextWindowUsedTokens,
    undefined,
  )
  assert.equal(promptUsage(usage).contextWindowUsedTokens, undefined)
  assert.equal(
    promptUsage(usage, {
      lastStep: {
        inputTokens: 40,
        cachedReadTokens: 20,
        cachedWriteTokens: 5,
        outputTokens: 9,
      },
    }).contextWindowUsedTokens,
    74,
    'a step-level cache write counts, the cumulative ACP one never does',
  )
  assert.deepEqual(
    promptUsage(null, {
      lastStep: {
        inputTokens: 7,
        cachedReadTokens: 3,
        cachedWriteTokens: 2,
        outputTokens: 1,
      },
    }),
    {
      inputTokens: 7,
      outputTokens: 1,
      cachedInputTokens: 3,
      contextWindowUsedTokens: 13,
      contextWindowMaxTokens: 256_000,
    },
  )
  assert.deepEqual(promptUsage(null), {})
  assert.deepEqual(
    promptUsage(
      { totalTokens: 60, inputTokens: 50, outputTokens: 10 },
      {
        carriedWakeup: {
          inputTokens: 20,
          outputTokens: 4,
          cachedReadTokens: 99,
        },
        lastStep: { inputTokens: 30, cachedReadTokens: 15, outputTokens: 6 },
      },
    ),
    {
      inputTokens: 30,
      outputTokens: 6,
      cachedInputTokens: 15,
      contextWindowUsedTokens: 51,
      contextWindowMaxTokens: 256_000,
    },
    'missing ACP cachedReadTokens still falls back to lastStep when carriedWakeup is deducted',
  )
  assert.deepEqual(
    promptUsage(
      { totalTokens: 60, inputTokens: 50, outputTokens: 10 },
      {
        wakeup: { inputTokens: 10, outputTokens: 2, cachedReadTokens: 0 },
        lastStep: { inputTokens: 60, cachedReadTokens: 15, outputTokens: 12 },
      },
    ),
    {
      inputTokens: 60,
      outputTokens: 12,
      cachedInputTokens: 15,
      contextWindowUsedTokens: 87,
      contextWindowMaxTokens: 256_000,
    },
    'missing ACP cachedReadTokens falls back to lastStep even when wakeup step defaulted cachedReadTokens to 0',
  )
  assert.equal(contextWindowForModel('gemini'), 256_000)
})

test('model metadata restricts effort variants and hides fixed-effort selectors', () => {
  const state = configState({
    configOptions: [
      {
        type: 'select',
        id: 'model',
        name: 'Model',
        currentValue: 'gpt-oss-120b-medium',
        options: [
          { value: 'gemini', name: 'Gemini', description: 'Model description' },
          { value: 'gpt-oss-120b-medium', name: 'GPT-OSS' },
        ],
      },
      {
        type: 'select',
        id: 'thought_level',
        name: 'Thinking',
        currentValue: 'medium',
        options: ['low', 'medium', 'high'].map((value) => ({
          value,
          name: value,
        })),
      },
    ],
    models: {
      availableModels: [
        { modelId: 'gemini-low' },
        { modelId: 'gemini-high' },
        { modelId: 'gpt-oss-120b-medium' },
      ],
    },
  })
  assert.deepEqual(
    state.models[0].thinkingOptions?.map((o) => o.id),
    ['low', 'high'],
  )
  assert.equal(state.models[0].contextWindowMaxTokens, 256000)
  assert.equal(state.models[0].description, 'Model description')
  assert.equal(state.models[1].contextWindowMaxTokens, 131072)
  assert.deepEqual(state.thinkingOptions, [])
  assert.equal(state.thinkingOption, undefined)
  assert.deepEqual(state.modes, [
    {
      id: 'full-access',
      label: 'Full Access',
      description:
        'Approve file edits, commands and tool requests without asking.',
      icon: 'ShieldOff',
      colorTier: 'dangerous',
      isUnattended: true,
    },
  ])
})

test('attachments preserve file paths, issue context and review comments', () => {
  const file = promptContent({
    type: 'uploaded_file',
    id: 'f',
    fileName: 'file.txt',
    mimeType: 'text/plain',
    size: 3,
    path: '/tmp/file.txt',
  })
  assert.ok(file.type === 'text' && file.text.includes('/tmp/file.txt'))
  const issue = promptContent({
    type: 'github_issue',
    mimeType: 'application/github-issue',
    number: 12,
    title: 'Bug',
    body: 'Details',
    url: 'https://example.com/12',
  })
  assert.ok(
    issue.type === 'text' &&
      issue.text.includes('Details') &&
      issue.text.includes('#12'),
  )
  const review = promptContent({
    type: 'review',
    mimeType: 'application/paseo-review',
    cwd: '/repo',
    mode: 'uncommitted',
    comments: [
      {
        filePath: 'test.ts',
        side: 'new',
        lineNumber: 3,
        body: 'Fix here',
        context: {
          hunkHeader: '@@ -1 +1 @@',
          targetLine: {
            oldLineNumber: null,
            newLineNumber: 3,
            type: 'add',
            content: 'code',
          },
          lines: [],
        },
      },
    ],
  })
  assert.ok(
    review.type === 'text' &&
      review.text.includes('test.ts:3') &&
      review.text.includes('Fix here'),
  )
  const textAttachment = promptContent({
    type: 'text',
    mimeType: 'text/plain',
    title: 'Sprint Notes',
    text: 'Fix login timeout',
    externalResource: {
      provider: 'linear',
      providerLabel: 'Linear',
      resourceType: 'issue',
      id: 'id-1',
      identifier: 'ENG-42',
      title: 'Auth timeout on slow network',
      url: 'https://linear.app/team/issue/ENG-42',
    },
  })
  assert.deepEqual(textAttachment, {
    type: 'text',
    text: [
      'Linear ENG-42: Auth timeout on slow network',
      'Sprint Notes',
      'https://linear.app/team/issue/ENG-42',
      'Fix login timeout',
    ].join('\n'),
  })
  assert.deepEqual(promptContent({ type: 'text', text: 'plain message' }), {
    type: 'text',
    text: 'plain message',
  })
})

test('native PascalCase tools retain paths, content, ranges and recipients', () => {
  assert.deepEqual(
    toolDetail({
      title: 'read_file',
      rawInput: { Path: '/a', StartLine: 3, EndLine: 7 },
    }),
    {
      type: 'read',
      filePath: '/a',
      content: undefined,
      offset: 3,
      limit: 5,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'write_file',
      rawInput: { Path: '/a', Content: 'new' },
    }),
    {
      type: 'write',
      filePath: '/a',
      content: 'new',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'edit_file',
      rawInput: { Path: '/a', Target: 'old', Replacement: 'new' },
    }),
    {
      type: 'edit',
      filePath: '/a',
      oldString: 'old',
      newString: 'new',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'read_url_content',
      rawInput: { Url: 'https://example.com' },
      rawOutput: 'page',
    }),
    {
      type: 'fetch',
      url: 'https://example.com',
      result: 'page',
    },
  )
  const recipient = '22222222-2222-4222-8222-222222222222'
  assert.deepEqual(
    toolDetail({
      title: 'send_message',
      rawInput: { Recipient: recipient, Message: 'Continue' },
    }),
    {
      type: 'sub_agent',
      subAgentType: recipient,
      description: 'Continue',
      childSessionId: recipient,
      log: '',
    },
  )
  const invalid = toolDetail({
    title: 'send_message',
    rawInput: { Recipient: '../outside' },
  })
  assert.ok(
    invalid.type === 'sub_agent' && invalid.childSessionId === undefined,
  )
  const read = toolDetail({
    title: 'view_file',
    rawInput: { AbsolutePath: '/a', ContentOffset: 8 },
  })
  assert.ok(
    read.type === 'read' && read.offset === 8 && read.limit === undefined,
  )
})

test('tool details enrich shell exit codes, clean read gutters, map plan artifacts, multi-chunk edits, search metadata, and plain failed errors', () => {
  assert.deepEqual(
    toolDetail({
      title: 'run_command',
      rawInput: { CommandLine: 'false' },
      rawOutput: 'Command exited with code 2.\n\nCommand output:\nfail',
    }),
    {
      type: 'shell',
      command: 'false',
      cwd: undefined,
      output: 'Command exited with code 2.\n\nCommand output:\nfail',
      exitCode: 2,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'read_file',
      rawInput: { Path: '/src/index.ts' },
      rawOutput:
        'Created At: 2026-11-09T10:00:00Z\nCompleted At: 2026-11-09T10:00:01Z\nShowing lines 10-12 of 50\n10: const a = 1\n11: const b = 2\n12: export { a, b }',
    }),
    {
      type: 'read',
      filePath: '/src/index.ts',
      content: 'const a = 1\nconst b = 2\nexport { a, b }',
      offset: 10,
      limit: 3,
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'write_file',
      rawInput: {
        Path: '/brain/plan.md',
        Content: '# Implementation Plan',
        ArtifactMetadata: {
          Summary: 'Plan',
          UserFacing: true,
          RequestFeedback: true,
        },
      },
    }),
    {
      type: 'plan',
      text: '# Implementation Plan',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'multi_replace_file_content',
      rawInput: {
        TargetFile: '/src/app.ts',
        ReplacementChunks: [
          { TargetContent: 'old1', ReplacementContent: 'new1' },
          { TargetContent: 'old2', ReplacementContent: 'new2' },
        ],
      },
    }),
    {
      type: 'edit',
      filePath: '/src/app.ts',
      oldString: 'old1\n...\nold2',
      newString: 'new1\n...\nnew2',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'grep_search',
      rawInput: { Query: 'Timeline' },
      rawOutput: 'server/mapping.ts:25',
      locations: [{ path: '/repo/server/mapping.ts' }],
    }),
    {
      type: 'search',
      query: 'Timeline',
      toolName: 'grep',
      filePaths: ['/repo/server/mapping.ts'],
      content: 'server/mapping.ts:25',
    },
  )
  assert.deepEqual(
    toolDetail({
      title: 'find_by_name',
      rawInput: { SearchDirectory: '/repo/server', Pattern: '*.ts' },
      rawOutput: 'server/mapping.ts',
    }),
    {
      type: 'search',
      query: '*.ts',
      toolName: 'glob',
      content: 'server/mapping.ts',
    },
  )
  const timeline = new Timeline('s')
  const failed = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'err-1',
    title: 'run_command',
    status: 'failed',
    rawInput: { CommandLine: 'bad' },
    rawOutput: 'permission denied',
  })
  assert.ok(failed?.type === 'tool_call')
  assert.equal(failed.error, 'permission denied')

  const failedContent = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'err-2',
    title: 'run_command',
    status: 'failed',
    rawInput: { CommandLine: 'bad2' },
    content: [
      { type: 'content', content: { type: 'text', text: 'stderr detail' } },
    ],
  })
  assert.ok(failedContent?.type === 'tool_call')
  assert.equal(failedContent.error, 'stderr detail')

  const canceledTool = timeline.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'cancel-1',
    title: 'run_command',
    status: 'cancelled' as 'failed',
  })
  assert.ok(canceledTool?.type === 'tool_call')
  assert.equal(canceledTool.status, 'canceled')
  assert.equal(canceledTool.error, null)

  timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'denied-1',
    title: 'run_command',
    status: 'in_progress',
    rawInput: { CommandLine: 'rm -rf /' },
  })
  const [deniedSettled] = timeline.terminalize(
    'completed',
    undefined,
    new Set(['denied-1']),
  )
  assert.ok(deniedSettled?.type === 'tool_call')
  assert.equal(deniedSettled.status, 'canceled')

  const askRunning = timeline.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'ask-1',
    title: 'default_api:ask_question',
    status: 'in_progress',
    rawInput: {
      questions: [
        {
          question: 'Which approach?',
          options: ['Option A', 'Option B'],
          is_multi_select: false,
        },
      ],
    },
  })
  assert.ok(askRunning?.type === 'tool_call')
  assert.equal(askRunning.name, 'ask_question')
  assert.deepEqual(askRunning.detail, {
    type: 'plain_text',
    label: 'ask_question',
    text: 'Which approach?\n  1. Option A\n  2. Option B',
  })
  assert.deepEqual(
    toolDetail({
      title: 'default_api:ask_question',
      rawInput: {
        questions: [
          {
            question: 'Which approach?',
            options: ['Option A', 'Option B'],
          },
        ],
      },
      rawOutput: 'Option A',
    }),
    {
      type: 'plain_text',
      label: 'ask_question',
      text: 'Which approach?\n  1. Option A\n  2. Option B\n\nAnswer:\nOption A',
    },
  )
})

test('timeline maps plan session updates into todo items', () => {
  const t = new Timeline('sess-1')
  const item = t.update({
    sessionUpdate: 'plan',
    entries: [
      { content: 'Step 1: Explore', status: 'completed', priority: 'medium' },
      {
        content: 'Step 2: Implement',
        status: 'in_progress',
        priority: 'medium',
      },
      { content: 'Step 3: Verify', status: 'pending', priority: 'medium' },
    ],
  })
  assert.deepEqual(item, {
    type: 'todo',
    id: 'sess-1:plan',
    items: [
      { text: 'Step 1: Explore', completed: true, status: 'completed' },
      { text: 'Step 2: Implement', completed: false, status: 'in_progress' },
      { text: 'Step 3: Verify', completed: false, status: 'pending' },
    ],
  })
})

test('write_file with kind:edit and diff maps to Write card and denied failed update becomes canceled', () => {
  const t = new Timeline('sess-live')
  const writeItem = t.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'call_write',
    name: 'write_file',
    title: 'File ../../../../tmp/hello.txt',
    kind: 'edit',
    status: 'completed',
    content: [{ type: 'diff', path: '/tmp/hello.txt', newText: 'HELLO\n' }],
    locations: [{ path: '/tmp/hello.txt' }],
    rawInput: { Path: '/tmp/hello.txt', Content: 'HELLO' },
    rawOutput: {},
  } as unknown as Parameters<Timeline['update']>[0])
  assert.ok(writeItem?.type === 'tool_call')
  assert.equal(writeItem.name, 'write_file')
  assert.deepEqual(writeItem.detail, {
    type: 'write',
    filePath: '/tmp/hello.txt',
    content: 'HELLO',
  })

  const denied = new Set(['call_deny'])
  t.update(
    {
      sessionUpdate: 'tool_call',
      toolCallId: 'call_deny',
      title: 'echo LIVE_DENY_99',
      kind: 'execute',
      status: 'in_progress',
      rawInput: { CommandLine: 'echo LIVE_DENY_99' },
    },
    denied,
  )
  const deniedDone = t.update(
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'call_deny',
      status: 'failed',
      rawOutput: { output: '' },
    },
    denied,
  )
  assert.ok(deniedDone?.type === 'tool_call')
  assert.equal(deniedDone.status, 'canceled')
  assert.equal(deniedDone.error, null)

  // Late status-less update on a denied tool keeps its canceled status and metadata.
  const deniedLate = t.update(
    {
      sessionUpdate: 'tool_call_update',
      toolCallId: 'call_deny',
      content: [{ type: 'content', content: { type: 'text', text: 'denied' } }],
    },
    denied,
  )
  assert.ok(deniedLate?.type === 'tool_call')
  assert.equal(deniedLate.status, 'canceled')
  assert.equal(deniedLate.detail.type, 'shell')
  assert.equal(t.isRunning(deniedLate.id), false)
})

test('late status-less tool_call_update preserves terminal status and metadata while orphan updates are ignored', () => {
  const t = new Timeline('s')
  assert.equal(
    t.update({
      sessionUpdate: 'tool_call_update',
      toolCallId: 'orphan',
      content: [{ type: 'content', content: { type: 'text', text: 'stray' } }],
    }),
    undefined,
  )
  t.update({
    sessionUpdate: 'tool_call',
    toolCallId: 'a',
    title: 'run_command',
    status: 'in_progress',
    rawInput: { CommandLine: 'ls' },
  })
  t.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'a',
    status: 'completed',
  })
  const late = t.update({
    sessionUpdate: 'tool_call_update',
    toolCallId: 'a',
    content: [{ type: 'content', content: { type: 'text', text: 'out' } }],
  })
  assert.ok(late?.type === 'tool_call')
  assert.equal(late.name, 'run_command')
  assert.equal(late.status, 'completed')
  assert.deepEqual(late.detail, {
    type: 'shell',
    command: 'ls',
    cwd: undefined,
    output: 'out',
  })
  assert.equal(t.isRunning(late.id), false)
  assert.deepEqual(t.terminalize('failed', 'boom'), [])
})

test('write card classification ignores paths or non-write tools containing "write"', () => {
  assert.equal(
    toolDetail({
      title: 'Read src/write_buffer.ts',
      kind: 'read',
      locations: [{ path: '/repo/src/write_buffer.ts' }],
    }).type,
    'read',
  )
  assert.deepEqual(
    toolDetail({
      title: 'Edit src/write_buffer.ts',
      kind: 'edit',
      rawInput: {
        Path: '/repo/src/write_buffer.ts',
        Target: 'old',
        Replacement: 'new',
      },
    }),
    {
      type: 'edit',
      filePath: '/repo/src/write_buffer.ts',
      oldString: 'old',
      newString: 'new',
    },
  )
  assert.equal(
    toolDetail({
      title: 'mcp__git__rewrite_history',
      rawInput: { path: '/repo/file.ts', content: 'note' },
    }).type,
    'plain_text',
  )
  assert.equal(
    toolDetail({
      title: 'save_memory',
      rawInput: { content: 'remember me' },
    }).type,
    'plain_text',
  )
  assert.equal(
    toolDetail({
      title: 'custom_replace',
      kind: 'edit',
      rawInput: {
        file_path: '/repo/a.ts',
        oldString: 'before',
        newString: 'after',
        content: 'unused',
      },
    }).type,
    'edit',
  )
})
