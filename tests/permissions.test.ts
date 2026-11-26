import { test } from 'node:test'
import assert from 'node:assert/strict'
import elicitationFixtures from './fixtures/elicitation-form.json' with { type: 'json' }
import {
  extractSecurityWarning,
  isPreapprovedMcpTool as preapprovedMcpCall,
  permissionKind,
  pickPermissionOption,
  RequestPermissionRequestSchema,
} from '../server/permissions.ts'
import {
  buildElicitationResponse,
  formatQuestionAnswersText,
  normalizeAskQuestionInput,
  normalizeElicitationQuestions,
  sameNormalizedQuestions,
  toQuestionPermissionInput,
} from '../server/questions.ts'

const options = [
  { optionId: 'always', kind: 'allow_always' },
  { optionId: 'yes', kind: 'allow_once' },
  { optionId: 'never', kind: 'reject_always' },
  { optionId: 'no', kind: 'reject_once' },
]

test('pickPermissionOption prefers one-shot options and honors selected actions', () => {
  assert.equal(pickPermissionOption(options, 'allow')?.optionId, 'yes')
  assert.equal(pickPermissionOption(options, 'deny')?.optionId, 'no')
  assert.equal(
    pickPermissionOption(options, 'allow', 'always')?.optionId,
    'always',
  )
  // A selected action must match the allow/deny intent.
  assert.equal(pickPermissionOption(options, 'deny', 'yes'), undefined)
  assert.equal(
    pickPermissionOption([{ optionId: 'a', kind: 'allow_always' }], 'allow')
      ?.optionId,
    'a',
  )
})

const binary = [{ kind: 'allow_once' }, { kind: 'reject_once' }]
const chooser = [{ kind: 'allow_once' }, { kind: 'allow_once' }]
const planDetail = { type: 'plan' as const, text: 'p' }
const textDetail = (label: string) => ({ type: 'plain_text' as const, label })

const classifyPermission = (
  toolCall: {
    toolCallId?: string
    name?: string
    title?: string
    kind?: unknown
  },
  detail: Parameters<typeof permissionKind>[1],
  requestOptions = binary,
) => permissionKind({ options: requestOptions, toolCall }, detail)

test('permissionKind classifies questions, mode switches, plans and tools', () => {
  assert.equal(
    classifyPermission(
      { title: 'run_command', kind: 'execute' },
      textDetail('run_command'),
      chooser,
    ),
    'question',
  )
  assert.equal(
    classifyPermission({ title: 'ask_user' }, textDetail('ask_user')),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'default_api:ask_question' },
      textDetail('default_api:ask_question'),
    ),
    'question',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run default_api:ask_question' },
      textDetail('Run default_api:ask_question'),
    ),
    'question',
  )
  assert.equal(
    classifyPermission(
      { title: 'exit_plan_mode' },
      textDetail('exit_plan_mode'),
    ),
    'mode',
  )
  assert.equal(
    classifyPermission(
      { title: 'enter_plan_mode' },
      textDetail('enter_plan_mode'),
    ),
    'mode',
  )
  assert.equal(
    classifyPermission({ title: 'contrib:exit_plan_mode' }, planDetail),
    'mode',
  )
  assert.equal(
    classifyPermission(
      { title: 'contrib:enter_plan_mode' },
      textDetail('contrib:enter_plan_mode'),
    ),
    'mode',
  )
  assert.equal(
    classifyPermission({ title: 'Run contrib:exit_plan_mode' }, planDetail),
    'mode',
  )
  assert.equal(
    classifyPermission({ title: 'Run default_api:exit_plan_mode' }, planDetail),
    'mode',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run enter_plan_mode' },
      textDetail('Run enter_plan_mode'),
    ),
    'mode',
  )
  assert.equal(
    classifyPermission(
      {
        name: 'contrib:exit_plan_mode',
        title: 'Exit plan mode',
        kind: 'other',
      },
      { type: 'plan', text: '# Plan' },
    ),
    'mode',
  )
  assert.equal(
    classifyPermission(
      {
        name: 'mcp__srv__other',
        title: 'exit_plan_mode',
      },
      textDetail('exit_plan_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      {
        name: 'mcp__srv__other',
        title: 'ask_question',
      },
      textDetail('ask_question'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission({ title: 'x', kind: 'switch_mode' }, textDetail('x')),
    'mode',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run mcp__srv__switch_mode', kind: 'switch_mode' },
      textDetail('Run mcp__srv__switch_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run default_api:mcp__srv__switch_mode', kind: 'switch_mode' },
      textDetail('Run default_api:mcp__srv__switch_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run mcp_srv_switch_mode', kind: 'switch_mode' },
      textDetail('Run mcp_srv_switch_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run default_api:mcp_srv_switch_mode', kind: 'switch_mode' },
      textDetail('Run default_api:mcp_srv_switch_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'srv:switch_mode', kind: 'switch_mode' },
      textDetail('srv:switch_mode'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'call_mcp_tool', kind: 'switch_mode' },
      textDetail('call_mcp_tool'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission(
      { title: 'Run default_api:call_mcp_tool', kind: 'switch_mode' },
      textDetail('Run default_api:call_mcp_tool'),
    ),
    'tool',
  )
  assert.equal(
    classifyPermission({ title: 'write_to_file', kind: 'edit' }, planDetail),
    'plan',
  )
  assert.equal(
    classifyPermission(
      { title: 'run_command', kind: 'execute' },
      textDetail('run_command'),
    ),
    'tool',
  )
})

const isPreapprovedMcpTool = (
  title: string,
  rawInput: Record<string, unknown>,
  policy: Parameters<typeof preapprovedMcpCall>[1],
  serverNames?: readonly string[],
) => preapprovedMcpCall({ title, rawInput }, policy, serverNames)

test('isPreapprovedMcpTool enforces policy presence, exact direct/lazy grant matching, and namespace trust', () => {
  const policy = {
    preapproved: [{ kind: 'mcp' as const, server: 'rea', tool: 'open' }],
  }
  assert.equal(
    isPreapprovedMcpTool('mcp_rea_open', {}, undefined, ['rea']),
    false,
  )
  assert.equal(isPreapprovedMcpTool('mcp_rea_open', {}, policy, ['rea']), true)
  assert.equal(
    isPreapprovedMcpTool('mcp_rea_close', {}, policy, ['rea']),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool('mcp__rea__open', {}, policy, ['rea']),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'open' },
      policy,
    ),
    true,
  )
  assert.equal(
    isPreapprovedMcpTool(
      'call_mcp_tool',
      { ServerName: 'rea', ToolName: 'close' },
      policy,
    ),
    false,
  )
  assert.equal(
    isPreapprovedMcpTool(
      'untrusted:call_mcp_tool',
      { ServerName: 'rea', ToolName: 'open' },
      policy,
    ),
    false,
  )
})

const agyForm = elicitationFixtures.foreground

test('elicitation forms become Paseo QuestionCard input', () => {
  assert.deepEqual(
    toQuestionPermissionInput(normalizeElicitationQuestions(agyForm)),
    {
      questions: [
        {
          header: 'Question 1',
          question: 'Which implementation path?',
          options: [
            { label: '(Recommended) Option A: Direct fix' },
            { label: 'Option B: Full refactor' },
          ],
          multiSelect: false,
          allowOther: true,
        },
        {
          header: 'Question 2',
          question: 'Select target platforms',
          options: [{ label: 'Linux, x86_64' }, { label: 'macOS' }],
          multiSelect: true,
          allowOther: true,
        },
      ],
    },
  )
  assert.deepEqual(
    toQuestionPermissionInput(
      normalizeElicitationQuestions({
        requestedSchema: {
          type: 'object',
          properties: {
            q1_other: { type: 'string', title: 'Any extra notes?' },
          },
        },
      }),
    ),
    {
      questions: [
        {
          header: 'Question 1',
          question: 'Any extra notes?',
          options: [],
          multiSelect: false,
          allowOther: true,
        },
      ],
    },
  )
  assert.deepEqual(
    normalizeElicitationQuestions({ message: '  Continue?  ' }).map(
      (q) => q.question,
    ),
    ['Continue?'],
  )
  assert.deepEqual(normalizeElicitationQuestions({}), [])
  const enumBased = normalizeElicitationQuestions({
    requestedSchema: {
      type: 'object',
      properties: {
        env: {
          type: 'string',
          title: 'Deploy target',
          enum: ['staging', 'prod'],
        },
      },
    },
  })
  assert.deepEqual(enumBased, [
    {
      id: 'env',
      header: 'Question 1',
      question: 'Deploy target',
      options: [
        { id: 'staging', label: 'staging' },
        { id: 'prod', label: 'prod' },
      ],
      multiSelect: false,
      allowOther: false,
    },
  ])
  assert.deepEqual(
    buildElicitationResponse(enumBased, {
      behavior: 'allow',
      updatedInput: { answers: { 'Question 1': 'canary' } },
    }),
    { action: 'accept', content: { env: 'canary' } },
  )
})

test('QuestionCard answers map back to schema values and Other fields', () => {
  const questions = normalizeElicitationQuestions(agyForm)
  // Paseo answers by header; multi-select joins labels and custom text with ", ".
  assert.deepEqual(
    buildElicitationResponse(questions, {
      behavior: 'allow',
      updatedInput: {
        answers: {
          'Question 1': '(Recommended) Option A: Direct fix',
          'Question 2': 'Linux, x86_64, Windows ARM64',
        },
      },
    }),
    {
      action: 'accept',
      content: {
        q1: '1',
        q2: ['1'],
        q2_other: 'Windows ARM64',
      },
    },
  )
  assert.deepEqual(
    buildElicitationResponse(questions, {
      behavior: 'allow',
      updatedInput: { answers: { 'Question 1': 'Custom hybrid approach' } },
    }),
    { action: 'accept', content: { q1_other: 'Custom hybrid approach' } },
  )
  assert.deepEqual(buildElicitationResponse(questions, { behavior: 'deny' }), {
    action: 'decline',
  })
})

test('answer text keeps labels and prefixes questions only when there are several', () => {
  const questions = normalizeElicitationQuestions(agyForm)
  const response = {
    behavior: 'allow' as const,
    updatedInput: {
      answers: {
        'Question 1': 'Option B: Full refactor',
        'Question 2': 'Linux, x86_64, macOS',
      },
    },
  }
  assert.equal(
    formatQuestionAnswersText(questions.slice(0, 1), response),
    'Option B: Full refactor',
  )
  assert.equal(
    formatQuestionAnswersText(questions, response),
    'Which implementation path?\nAnswer: Option B: Full refactor\n\nSelect target platforms\nAnswer: Linux, x86_64, macOS',
  )
  assert.equal(
    formatQuestionAnswersText(questions, { behavior: 'deny' }),
    'Declined',
  )
})

test('raw ask_question arguments normalize like agy writes them and disambiguate duplicate headers', () => {
  const [question] = normalizeAskQuestionInput({
    questions: [
      {
        question: 'Proceed with deployment?',
        options: ['(Recommended) Yes', 'No'],
        is_multi_select: false,
      },
    ],
  })
  assert.deepEqual(question, {
    id: 'q1',
    header: 'Question 1',
    question: 'Proceed with deployment?',
    options: [
      { id: '0', label: '(Recommended) Yes' },
      { id: '1', label: 'No' },
    ],
    multiSelect: false,
    allowOther: true,
  })
  assert.deepEqual(normalizeAskQuestionInput({ question: 'Name?' }), [
    {
      id: 'q1',
      header: 'Question 1',
      question: 'Name?',
      options: [],
      multiSelect: false,
      allowOther: true,
    },
  ])

  const dupHeaders = normalizeAskQuestionInput({
    questions: [
      {
        header: 'Target',
        question: 'Primary target?',
        options: ['Linux', 'macOS'],
      },
      {
        header: 'Target',
        question: 'Fallback target?',
        options: ['Linux', 'Windows'],
      },
    ],
  })
  assert.deepEqual(
    dupHeaders.map((q) => q.header),
    ['Target', 'Target (2)'],
  )
  const dupResponse = {
    behavior: 'allow' as const,
    updatedInput: {
      answers: {
        Target: 'Linux',
        'Target (2)': 'Windows',
      },
    },
  }
  assert.deepEqual(buildElicitationResponse(dupHeaders, dupResponse), {
    action: 'accept',
    content: { q1: '0', q2: '1' },
  })
  assert.equal(
    formatQuestionAnswersText(dupHeaders, dupResponse),
    'Primary target?\nAnswer: Linux\n\nFallback target?\nAnswer: Windows',
  )
})

test('an elicitation schema matches its Brain tool input by question and labels', () => {
  const wokenAsked = normalizeAskQuestionInput({
    questions: [
      {
        header: 'Choice',
        question: 'Background check finished. Which option?',
        options: ['(Recommended) Option 1', 'Option 2'],
        is_multi_select: false,
      },
    ],
  })
  const wokenReplayed = normalizeElicitationQuestions(
    elicitationFixtures.wokenReplay,
  )
  assert.equal(sameNormalizedQuestions(wokenAsked, wokenReplayed), true)
  const asked = normalizeAskQuestionInput({
    questions: [
      {
        header: 'Environment',
        question: 'Which implementation path?',
        options: [
          '(Recommended) Option A: Direct fix',
          'Option B: Full refactor',
        ],
      },
      {
        header: 'Platform',
        question: 'Select target platforms',
        is_multi_select: true,
        options: ['Linux, x86_64', 'macOS'],
      },
    ],
  })
  const replayed = normalizeElicitationQuestions(agyForm)
  assert.equal(sameNormalizedQuestions(asked, replayed), true)
  assert.equal(sameNormalizedQuestions(asked, wokenReplayed), false)
  assert.equal(sameNormalizedQuestions([], []), false)
})

test('RequestPermissionRequestSchema preserves _meta on request, toolCall, and options', () => {
  const parsed = RequestPermissionRequestSchema.safeParse({
    sessionId: 'sess-1',
    _meta: { 'agy.security.warning': 'Top-level caution', custom: true },
    toolCall: {
      toolCallId: 'interaction_1',
      title: 'Confirm choice',
      _meta: { 'agy.security.warning': { title: 'Tool', message: 'Caution' } },
    },
    options: [
      {
        optionId: 'allow',
        name: 'Allow',
        kind: 'allow_once',
        _meta: {
          'agy.security.warning': {
            title: 'Network',
            message: 'Sends outbound request',
          },
        },
      },
      {
        optionId: 'deny',
        name: 'Deny',
        kind: 'reject_once',
        _meta: null,
      },
    ],
  })
  assert.equal(parsed.success, true)
  assert.deepEqual(parsed.data?._meta, {
    'agy.security.warning': 'Top-level caution',
    custom: true,
  })
  assert.deepEqual(parsed.data?.toolCall._meta, {
    'agy.security.warning': { title: 'Tool', message: 'Caution' },
  })
  assert.deepEqual(parsed.data?.options[0]._meta, {
    'agy.security.warning': {
      title: 'Network',
      message: 'Sends outbound request',
    },
  })
  assert.equal(parsed.data?.options[1]._meta, null)
})

test('permissionKind protects interaction_* toolCallIds as questions', () => {
  assert.equal(
    classifyPermission(
      {
        toolCallId: 'interaction_42',
        title: 'run_command',
        kind: 'execute',
      },
      textDetail('run_command'),
    ),
    'question',
  )
  assert.equal(
    classifyPermission(
      {
        toolCallId: 'interaction_plan_confirm',
        title: 'exit_plan_mode',
        kind: 'switch_mode',
      },
      planDetail,
    ),
    'question',
  )
  assert.equal(
    classifyPermission(
      {
        toolCallId: 'call_interaction_42',
        title: 'run_command',
        kind: 'execute',
      },
      textDetail('run_command'),
    ),
    'tool',
  )
})

test('extractSecurityWarning formats, deduplicates, tracks warned options, and truncates safely by code points', () => {
  assert.equal(
    extractSecurityWarning({
      options: [{ optionId: 'allow' }, { optionId: 'deny', _meta: null }],
    }),
    undefined,
  )
  assert.equal(
    extractSecurityWarning({
      _meta: { 'agy.security.warning': '   ' },
      toolCall: {
        _meta: { 'agy.security.warning': { title: ' ', message: '' } },
      },
      options: [{ optionId: 'allow', _meta: { 'agy.security.warning': 123 } }],
    }),
    undefined,
  )

  const deduped = extractSecurityWarning({
    options: [
      {
        optionId: 'allow_once',
        _meta: {
          'agy.security.warning': {
            title: '  Destructive  ',
            message: '  Deletes files  ',
          },
        },
      },
      {
        optionId: 'allow_always',
        _meta: {
          'agy.security.warning': {
            title: 'Destructive',
            message: 'Deletes files',
          },
        },
      },
      {
        optionId: 'reject_once',
        _meta: {},
      },
    ],
  })
  assert.equal(deduped?.description, 'Destructive: Deletes files')
  assert.deepEqual(
    [...(deduped?.warnedOptionIds ?? [])],
    ['allow_once', 'allow_always'],
  )

  const layered = extractSecurityWarning({
    _meta: { 'agy.security.warning': 'Request warning' },
    toolCall: {
      _meta: { 'agy.security.warning': { title: 'Tool title only' } },
    },
    options: [
      {
        optionId: 'opt_msg',
        _meta: { 'agy.security.warning': { message: 'Option message only' } },
      },
    ],
  })
  assert.equal(
    layered?.description,
    'Request warning\nTool title only\nOption message only',
  )
  assert.deepEqual([...(layered?.warnedOptionIds ?? [])], ['opt_msg'])

  const requestOnly = extractSecurityWarning({
    _meta: { 'agy.security.warning': 'Global warning' },
    options: [{ optionId: 'allow' }],
  })
  assert.equal(requestOnly?.description, 'Global warning')
  assert.equal(requestOnly?.warnedOptionIds.size, 0)

  const exact512 = '🔥'.repeat(512)
  const exactResult = extractSecurityWarning({
    options: [{ optionId: 'a', _meta: { 'agy.security.warning': exact512 } }],
  })
  assert.equal(exactResult?.description, exact512)
  assert.equal([...(exactResult?.description ?? '')].length, 512)

  const over512 = '🔥'.repeat(513)
  const truncatedResult = extractSecurityWarning({
    options: [{ optionId: 'a', _meta: { 'agy.security.warning': over512 } }],
  })
  assert.equal(truncatedResult?.description, `${'🔥'.repeat(511)}…`)
  assert.equal([...(truncatedResult?.description ?? '')].length, 512)
})
