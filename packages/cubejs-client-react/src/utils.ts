export function removeEmpty<T>(obj: T): T {
  if (Array.isArray(obj) || typeof obj !== 'object') {
    return obj;
  }

  return Object.fromEntries(
    Object.entries(obj as Record<string, any>)
      .filter(([, v]) => v != null)
      .map(([k, v]) => {
        if (Array.isArray(v)) {
          return [k, v.map(removeEmpty)];
        }

        return [k, typeof v === 'object' ? removeEmpty(v) : v];
      })
  ) as T;
}
