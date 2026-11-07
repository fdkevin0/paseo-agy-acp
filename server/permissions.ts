import { z } from 'zod'

/** Validate the permission fields consumed by the bridge before creating pending work. */
export const RequestPermissionRequestSchema = z.object({
  sessionId: z.string().min(1),
  toolCall: z
    .object({
      toolCallId: z.string().min(1),
      title: z.string().nullish(),
      rawInput: z.json().optional(),
    })
    .passthrough(),
  options: z
    .array(
      z.object({
        optionId: z.string().min(1),
        name: z.string(),
        kind: z.enum([
          'allow_once',
          'allow_always',
          'reject_once',
          'reject_always',
        ]),
      }),
    )
    .min(1),
})
