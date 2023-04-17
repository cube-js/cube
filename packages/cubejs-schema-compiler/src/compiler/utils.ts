import { camelize } from 'inflection';

function camelizeObjectPart(obj: unknown, deep: boolean, camelizeKeys: boolean, level = 0): unknown {
  if (!obj) {
    return obj;
  }

  if (!deep && !camelizeKeys) {
    return obj;
  }

  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      obj[i] = camelizeObjectPart(obj[i], true, true, level + 1);
    }
  } else if (typeof obj === 'object') {
    for (const key of Object.keys(obj)) {
      if (deep && !(level === 1 && key === 'meta')) {
        obj[key] = camelizeObjectPart(obj[key], deep, true, level + 1);
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
  camelizeObjectPart(cube, false, true);
  camelizeObjectPart(cube.measures, true, false);
  camelizeObjectPart(cube.dimensions, true, false);
  camelizeObjectPart(cube.preAggregations, true, false);

  return cube;
}
