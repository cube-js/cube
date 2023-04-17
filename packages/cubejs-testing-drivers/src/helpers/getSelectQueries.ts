import fs from 'fs-extra';
import path from 'path';

/**
 * Returns select sql queries.
 */
export function getSelectQueries(type: string): string[] {
  const schemas = JSON.parse(fs.readFileSync(
    path.resolve(process.cwd(), './fixtures/_schemas.json'),
    'utf-8'
  ));
  return schemas.cubes.map((cube: { sql: string }) => cube.sql);
}
