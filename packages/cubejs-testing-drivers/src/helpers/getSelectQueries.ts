import { getFixtures } from './getFixtures';

/**
 * Returns select sql queries.
 */
export function getSelectQueries(type: string, suf?: string): string[] {
  const { tables } = getFixtures(type);
  return Object
    .keys(tables)
    .map((key: string) => {
      let name = tables[<'products' | 'customers' | 'ecommerce'>key];
      name = suf ? `${name}_${suf}` : name;
      return `select * from ${name}`;
    });
}
