/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 */

/**
 * Node rejects with an `AggregateError` when a hostname resolves to several addresses and every
 * connection attempt fails; `${err}` on it renders as just "AggregateError" and drops every reason.
 *
 * Upstream only ever reads `message`/`stack` (QueryQueue logs, api-gateway response), never `cause`,
 * so the detail has to live in the string.
 */
export function formatError(e: unknown): string {
  if (e instanceof AggregateError) {
    // Node leaves `message` empty for the errors it raises itself
    const prefix = e.message ? `Aggregate error: ${e.message}` : 'Aggregate error';
    return `${prefix}; errors: ${e.errors.map((inner) => formatError(inner)).join('; ')}`;
  }

  return `${e}`;
}
