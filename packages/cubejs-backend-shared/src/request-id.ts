/**
 * A request ID carries a `-span-N` suffix per query span within the request. Consumers that
 * key off the request itself (queue dedup, query tagging) want the ID without that suffix.
 */
export function extractRequestUUID(requestId: string): string {
  const idx = requestId.lastIndexOf('-span-');
  return idx !== -1 ? requestId.substring(0, idx) : requestId;
}
