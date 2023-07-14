import { camelize } from 'inflection';

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
        obj[key] = camelizeObjectPart(obj[key], true, level + 1);
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

  return cube;
}
