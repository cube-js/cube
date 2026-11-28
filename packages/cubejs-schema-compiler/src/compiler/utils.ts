import { camelize } from 'inflection';

/**
 * Caches a string transformation. Model compilation applies the same few to every member, and
 * inputs repeat: property keys are a small vocabulary, member names repeat across cubes. The
 * cache is dropped once it holds `limit` entries, so arbitrary input can't grow it without bound.
 */
export function memoizeString(fn: (s: string) => string, limit = 10000): (s: string) => string {
  const cache = new Map<string, string>();

  return (s: string) => {
    let res = cache.get(s);
    if (res === undefined) {
      if (cache.size >= limit) {
        cache.clear();
      }

      res = fn(s);
      cache.set(s, res);
    }

    return res;
  };
}

const camelizeKey = memoizeString((key) => camelize(key, true));

// It's a map where key - is a level and value - is a map of properties on this level to ignore camelization
const IGNORE_CAMELIZE: Record<number, Record<string, boolean>> = {
  1: {
    granularities: true,
  }
};

const DRILL_QUERIES_KEYS = new Set(['drill_queries', 'drillQueries']);

// `parentKey` is the key the object or list sits under; a list's items share it
function camelizeObjectPart(obj: unknown, camelizeKeys: boolean, level = 0, parentKey?: string): unknown {
  if (!obj) {
    return obj;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = camelizeObjectPart(obj[i], true, level + 1, parentKey);
    }
  } else if (typeof obj === 'object') {
    const record = obj as Record<string, unknown>;

    for (const key of Object.keys(record)) {
      if (!(level === 1 && key === 'meta')) {
        // A drill query's `order` is keyed by member names, which stay as written
        const isDrillQueryOrder = key === 'order' && parentKey !== undefined && DRILL_QUERIES_KEYS.has(parentKey);
        const camelizeChildKeys = !IGNORE_CAMELIZE[level]?.[key] && !isDrillQueryOrder;
        record[key] = camelizeObjectPart(record[key], camelizeChildKeys, level + 1, key);
      }

      if (camelizeKeys) {
        const camelizedKey = camelizeKey(key);
        if (camelizedKey !== key) {
          record[camelizedKey] = record[key];
          delete record[key];
        }
      }
    }
  }

  return obj;
}

export function camelizeCube(cube: any): unknown {
  for (const key of Object.keys(cube)) {
    const camelizedKey = camelizeKey(key);
    if (camelizedKey !== key) {
      cube[camelizedKey] = cube[key];
      delete cube[key];
    }
  }

  camelizeObjectPart(cube.measures, false);
  camelizeObjectPart(cube.dimensions, false);
  camelizeObjectPart(cube.preAggregations, false);
  camelizeObjectPart(cube.cubes, false);
  camelizeObjectPart(cube.accessPolicy, false);
  camelizeObjectPart(cube.folders, false);
  camelizeObjectPart(cube.memberSets, false);

  return cube;
}
