import path from 'path';
import fs from 'fs-extra';
import { Fixture } from '../types/Fixture';

function deepMerge(a: any, b: any): any {
  a = { ...a };
  for (const k of Object.keys(b)) {
    if (a[k] && typeof a[k] === 'object') {
      a[k] = deepMerge(a[k], b[k]);
    } else {
      a[k] = b[k];
    }
  }
  return a;
}

/**
 * Returns fixture by data source type.
 */
export function getFixtures(type: string, extendedEnv?: string): Fixture {
  const _path = path.resolve(process.cwd(), `./fixtures/${type}.json`);
  const _content = fs.readFileSync(_path, 'utf-8');

  let fixtures = JSON.parse(_content);

  if (extendedEnv) {
    if (!(extendedEnv in fixtures.extendedEnvs)) {
      throw new Error(`Fixtures for ${type} doesn't contain extended env for ${extendedEnv}`);
    }

    fixtures = deepMerge(fixtures, fixtures.extendedEnvs[extendedEnv]);
  }

  return fixtures;
}
