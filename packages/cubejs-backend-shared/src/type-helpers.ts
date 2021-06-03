/**
 * This module export only type helpers for using it across Cube.js project
 */

export type ResolveAwait<T> = T extends {
  then(onfulfilled?: (value: infer U) => unknown): unknown;
} ? U : T;

export type Constructor<T> = new (...args: any[]) => T;

// Make some fields required from, if they are optional
export type Required<T, K extends keyof T> = {
  [X in Exclude<keyof T, K>]?: T[X]
} & {
  [P in K]-?: T[P]
};

export type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>;
