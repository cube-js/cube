export function extractHashConfig<T extends Record<string, unknown>>(
  defaults?: T
): T {
  const hash = location.hash.slice(1);

  if (hash) {
    try {
      return {
        ...defaults,
        ...JSON.parse(atob(decodeURIComponent(hash))),
      };
    } catch (e) {
      console.error('Invalid hash', hash);
    }
  }

  return {
    ...defaults,
  } as T;
}
