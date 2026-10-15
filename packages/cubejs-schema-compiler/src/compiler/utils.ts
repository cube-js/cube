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
const IGNORE_CAMELIZE = {
  1: {
    granularities: true,
  }
};

function camelizeObjectPart(obj: unknown, camelizeKeys: boolean, level = 0): unknown {
  if (!obj) {
    return obj;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = camelizeObjectPart(obj[i], true, level + 1);
    }
  } else if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (!(level === 1 && key === 'meta')) {
        obj[key] = camelizeObjectPart(obj[key], !IGNORE_CAMELIZE[level]?.[key], level + 1);
      }

      if (camelizeKeys) {
        const camelizedKey = camelizeKey(key);
        if (camelizedKey !== key) {
          obj[camelizedKey] = obj[key];
          delete obj[key];
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

  return cube;
}
