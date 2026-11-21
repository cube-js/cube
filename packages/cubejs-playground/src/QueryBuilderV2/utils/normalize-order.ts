/**
 * Returns a copy of the query where `order` is always in the array form
 * (`[["Member", "asc"], ...]`), which is the canonical shape for the REST API.
 * The input query is not mutated.
 */
export function normalizeQueryOrder<T extends { order?: unknown }>(query: T): T {
  const order = query?.order;

  if (!order || Array.isArray(order) || typeof order !== 'object') {
    return query;
  }

  return { ...query, order: Object.entries(order as Record<string, unknown>) };
}
