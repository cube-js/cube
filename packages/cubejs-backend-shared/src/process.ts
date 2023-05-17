import { v1, v5 } from 'uuid';

/**
 * Unique process ID (aka 00000000-0000-0000-0000-000000000000).
 */
const processUid = v5(v1(), v1()).toString();

/**
 * Returns unique process ID.
 */
export function getProcessUid(): string {
  return processUid;
}

let i = 0;

/**
 * Returns unique for the process number.
 */
export function getNext(): number {
  return i++;
}

export function isCI(): boolean {
  if (process.env.GITHUB_EVENT_NAME) {
    return true;
  }

  return false;
}
