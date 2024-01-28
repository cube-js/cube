import path from 'path';
import fs from 'fs-extra';
import { Fixture } from '../types/Fixture';

/**
 * Returns fixture by data source type.
 */
export function getFixtures(type: string, extendedEnv?: string): Fixture {
  const _path = path.resolve(process.cwd(), `./fixtures/${type}.json`);
  const _content = fs.readFileSync(_path, 'utf-8');
  let fixtures = JSON.parse(_content);
  if (extendedEnv) {
    fixtures = deepMerge(fixtures, fixtures.extendedEnvs[extendedEnv]);
  }
  return fixtures;
}

function deepMerge(a: any, b: any): any {
  a = { ...a };
  for (let k of Object.keys(b)) {
    if (a[k] && typeof a[k] === 'object') {
      a[k] = deepMerge(a[k], b[k]);
    } else {
      a[k] = b[k];
    }
  }
  return a;
}