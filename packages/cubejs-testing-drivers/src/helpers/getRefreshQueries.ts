import { getFixtures } from './getFixtures';

/**
 * Returns `REFRESH TABLE` statements for the fixture tables.
 *
 * CrateDB is eventually consistent: rows written by `CREATE TABLE AS` / `INSERT`
 * are not guaranteed to be visible to the next `SELECT` until the table is
 * refreshed (the default table refresh interval is ~1s). Run these right after
 * seeding so the freshly loaded fixture data is readable immediately.
 *
 * See https://cratedb.com/docs/crate/reference/en/latest/general/dql/refresh.html
 */
export function getRefreshQueries(type: string, suf?: string): string[] {
  const { tables } = getFixtures(type);
  return Object
    .keys(tables)
    .map((key: string) => {
      const name = suf ? `${tables[key]}_${suf}` : tables[key];
      return `REFRESH TABLE ${name}`;
    });
}
