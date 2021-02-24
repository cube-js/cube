/**
 * This module export only type helpers for using it across Cube.js project
 */

export type ResolveAwait<T> = T extends {
  then(onfulfilled?: (value: infer U) => unknown): unknown;
} ? U : T;
