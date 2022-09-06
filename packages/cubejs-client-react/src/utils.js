export function removeEmpty(obj) {
  if (typeof obj !== 'object') {
    return obj;
  }

  return Object.fromEntries(
    Object.entries(obj)
      .filter(([, v]) => v != null)
      .map(([k, v]) => {
        if (Array.isArray(v)) {
          return [k, v.map(removeEmpty)];
        }

        return [k, typeof v === 'object' ? removeEmpty(v) : v];
      })
  );
}
