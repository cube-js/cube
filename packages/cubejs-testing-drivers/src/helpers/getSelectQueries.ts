import { getFixtures } from './getFixtures';

/**
 * Returns select sql queries.
 */
export function getSelectQueries(type: string, suf?: string): string[] {
  const { tables } = getFixtures(type);
  return Object
    .keys(tables)
    .map((key: string) => {
      let name = tables[key];
      name = suf ? `${name}_${suf}` : name;
      // CrateDB is sharded and returns rows in a non-deterministic order across
      // table (re)creations, which breaks the positional row snapshots in the raw
      // driver suite. Order by the first column (a unique key in every fixture
      // table) to make the result order stable.
      if (type === 'crate') {
        return `select * from ${name} order by 1`;
      }
      return `select * from ${name}`;
    });
}
