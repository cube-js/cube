export function extractRequestUUID(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');
  return idx !== -1 ? requestId.substring(0, idx) : requestId;
}

export const REQUEST_ID_MAX_LENGTH = 128;

export const REQUEST_ID_PATTERN = /^[A-Za-z0-9+/=._:@~-]+$/;

export const INVALID_REQUEST_ID_MESSAGE =
  `Request id must be at most ${REQUEST_ID_MAX_LENGTH} characters from A-Z, a-z, 0-9, '+', '/', '=', '.', '_', ':', '@', '~' and '-'`;

/**
 * Drivers forward the request id to databases as headers, labels and query ids.
 * Quotes, backslashes, ';' and other SQL metacharacters are excluded so that a
 * driver that ever inlines the id into SQL can't be injected through it.
 */
export function isValidRequestId(requestId: string): boolean {
  return requestId.length <= REQUEST_ID_MAX_LENGTH && REQUEST_ID_PATTERN.test(requestId);
}
