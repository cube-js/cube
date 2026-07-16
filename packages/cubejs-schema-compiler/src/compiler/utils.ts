import { camelize } from 'inflection';

// Map of level -> keys at that level whose children must not be camelized (they hold user-defined
// identifiers). Each entry can require a specific parent key so the guard is path-scoped rather than
// matching any same-named property elsewhere in the tree.
const IGNORE_CAMELIZE: Record<number, Record<string, { parent?: string }>> = {
  1: {
    granularities: {},
  },
  // Custom granularity names in the new dict form live at `granularities.custom.<name>`; scope the
  // guard to that path so an unrelated `custom` property elsewhere isn't affected.
  2: {
    custom: { parent: 'granularities' },
  }
};

function shouldIgnoreCamelize(level: number, key: string, parentKey: string | undefined): boolean {
  const entry = IGNORE_CAMELIZE[level]?.[key];
  if (!entry) {
    return false;
  }
  return entry.parent === undefined || entry.parent === parentKey;
}

function camelizeObjectPart(obj: unknown, camelizeKeys: boolean, level = 0, parentKey?: string): unknown {
  if (!obj) {
    return obj;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = camelizeObjectPart(obj[i], true, level + 1, parentKey);
    }
  } else if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (!(level === 1 && key === 'meta')) {
        obj[key] = camelizeObjectPart(obj[key], !shouldIgnoreCamelize(level, key, parentKey), level + 1, key);
      }

      if (camelizeKeys) {
        const camelizedKey = camelize(key, true);
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
    const camelizedKey = camelize(key, true);
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
