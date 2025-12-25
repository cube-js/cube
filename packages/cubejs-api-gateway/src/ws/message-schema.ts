import { z } from 'zod';

const messageId = z.union([z.string().max(16), z.int()]);
const requestId = z.string().max(64).optional();

export const authMessageSchema = z.object({
  authorization: z.string(),
}).strict();

export const unsubscribeMessageSchema = z.object({
  unsubscribe: messageId,
}).strict();

const queryParams = z.object({
  query: z.unknown(),
  queryType: z.string().optional(),
}).strict();

const queryOnlyParams = z.object({
  query: z.unknown(),
}).strict();

// Method-based messages using discriminatedUnion
export const methodMessageSchema = z.discriminatedUnion('method', [
  z.object({
    method: z.literal('load'),
    messageId,
    requestId,
    params: queryParams,
  }).strict(),
  z.object({
    method: z.literal('sql'),
    messageId,
    requestId,
    params: queryOnlyParams,
  }).strict(),
  z.object({
    method: z.literal('dry-run'),
    messageId,
    requestId,
    params: queryOnlyParams,
  }).strict(),
  z.object({
    method: z.literal('meta'),
    messageId,
    requestId,
    params: z.object({}).strict().optional(),
  }).strict(),
  z.object({
    method: z.literal('subscribe'),
    messageId,
    requestId,
    params: queryParams,
  }).strict(),
  z.object({
    method: z.literal('unsubscribe'),
    messageId,
    requestId,
    params: z.object({}).strict().optional(),
  }).strict(),
]);

// Export types
export type AuthMessage = z.infer<typeof authMessageSchema>;
export type UnsubscribeMessage = z.infer<typeof unsubscribeMessageSchema>;
export type MethodMessage = z.infer<typeof methodMessageSchema>;
export type WsMessage = AuthMessage | UnsubscribeMessage | MethodMessage;
