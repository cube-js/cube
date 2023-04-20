import path from 'path';
import fs from 'fs-extra';
import { Fixture } from '../types/Fixture';

let fixtures: Fixture;

/**
 * Returns fixture by data source type.
 */
export function getFixtures(type: string): Fixture {
  if (!fixtures) {
    const _path = path.resolve(process.cwd(), `./fixtures/${type}.json`);
    const _content = fs.readFileSync(_path, 'utf-8');
    fixtures = JSON.parse(_content);
  }
  return fixtures;
}
