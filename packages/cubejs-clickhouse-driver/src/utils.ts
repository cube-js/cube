export function formatError(e: unknown): string {
  if (e instanceof AggregateError) {
    // Node leaves `message` empty for the errors it raises itself
    const prefix = e.message ? `Aggregate error: ${e.message}` : 'Aggregate error';
    return `${prefix}; errors: ${e.errors.map((inner) => formatError(inner)).join('; ')}`;
  }

  return `${e}`;
}
