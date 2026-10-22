import { readable, type Readable } from 'svelte/store';

import type { Source } from '../types';

export function isReadable<T>(value: Source<T>): value is Readable<T> {
  return Boolean(
    value && typeof (value as Readable<T>).subscribe === 'function'
  );
}
export function asReadable<T>(value: Source<T>): Readable<T> {
  return isReadable(value) ? value : readable(value as T);
}

export function isBrowser(): boolean {
  return typeof window !== 'undefined';
}
