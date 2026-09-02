/**
 * Extracts the UUID prefix from a request ID by stripping the `-span-N` suffix.
 */
export function extractRequestUUID(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');
  return idx !== -1 ? requestId.substring(0, idx) : requestId;
}
