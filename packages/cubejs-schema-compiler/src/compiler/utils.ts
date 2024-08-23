import { camelize } from 'inflection';

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

export function isGranularityNaturalAligned(interval: string): boolean {
  const intParsed = interval.split(' ');

  if (intParsed.length !== 2) {
    return false;
  }

  const v = parseInt(intParsed[0], 10);
  const unit = intParsed[1];

  const validIntervals = {
    // Any number of years is valid
    year: () => true,
    // Only months divisible by a year with no remainder are valid
    month: () => 12 % v === 0,
    // Only quarters divisible by a year with no remainder are valid
    quarter: () => 4 % v === 0,
    // Only 1 day is valid
    day: () => v === 1,
    // Only hours divisible by a day with no remainder are valid
    hour: () => 24 % v === 0,
    // Only minutes divisible by an hour with no remainder are valid
    minute: () => 60 % v === 0,
    // Only seconds divisible by a minute with no remainder are valid
    second: () => 60 % v === 0,
  };

  return Object.keys(validIntervals).some(key => unit.includes(key) && validIntervals[key]());
}
