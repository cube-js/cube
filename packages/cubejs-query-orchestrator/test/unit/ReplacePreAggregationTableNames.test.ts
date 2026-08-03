import { QueryCache } from '../../src';
import type { PreAggTableToTempTableNames } from '../../src';

describe('QueryCache.replacePreAggregationTableNames', () => {
  test('replaces a single table name', () => {
    const result = QueryCache.replacePreAggregationTableNames(
      'SELECT * FROM dev_pre_aggregations.orders_rollup',
      [['dev_pre_aggregations.orders_rollup', { targetTableName: 'dev_pre_aggregations.orders_rollup_20250401_abc' }]],
    );
    expect(result).toBe('SELECT * FROM dev_pre_aggregations.orders_rollup_20250401_abc');
  });

  test('does not corrupt names that are prefixes of other names (name1 vs name10)', () => {
    const baseName = 'dev_pre_aggregations.orders_rollup';
    const entries: PreAggTableToTempTableNames[] = Array.from(
      { length: 12 },
      (_, i): PreAggTableToTempTableNames => [
        `${baseName}${i}`,
        { targetTableName: `(SELECT * FROM ${baseName}_20250401_part${i})` },
      ],
    );
    const query = entries
      .map(([tableName], i) => `SELECT * FROM ${tableName} AS "alias${i}"`)
      .join(' UNION ALL ');

    const result = QueryCache.replacePreAggregationTableNames(query, entries) as string;

    entries.forEach(([, { targetTableName }], i) => {
      expect(result).toContain(`${targetTableName} AS "alias${i}"`);
    });
    // No stray suffix digits left behind, e.g. `...)0 AS "alias10"`
    expect(result).not.toMatch(/\)\d+ AS/);
    expect(result).not.toContain(`${baseName}10`);
  });

  test('does not match source names inside already inserted target names', () => {
    // Real-world target shape: tableName + '_' + versions, so the target
    // of `rollup10` contains `rollup1` as a substring
    const entries: PreAggTableToTempTableNames[] = [
      ['pa.rollup1', { targetTableName: 'pa.rollup1_aaa_bbb_111' }],
      ['pa.rollup10', { targetTableName: 'pa.rollup10_ccc_ddd_222' }],
    ];
    const result = QueryCache.replacePreAggregationTableNames(
      'SELECT * FROM pa.rollup10 JOIN pa.rollup1',
      entries,
    );
    expect(result).toBe('SELECT * FROM pa.rollup10_ccc_ddd_222 JOIN pa.rollup1_aaa_bbb_111');
  });

  test('keeps params and query options for QueryWithParams input', () => {
    const result = QueryCache.replacePreAggregationTableNames(
      ['SELECT * FROM dev_pre_aggregations.orders_rollup WHERE id = ?', ['1'], { external: true }],
      [['dev_pre_aggregations.orders_rollup', { targetTableName: 'dev_pre_aggregations.orders_rollup_20250401_abc' }]],
    );
    expect(result).toEqual([
      'SELECT * FROM dev_pre_aggregations.orders_rollup_20250401_abc WHERE id = ?',
      ['1'],
      { external: true },
    ]);
  });

  test('returns query as is for empty replacements', () => {
    const result = QueryCache.replacePreAggregationTableNames('SELECT 1', []);
    expect(result).toBe('SELECT 1');
  });

  test('treats $ in target names literally', () => {
    const result = QueryCache.replacePreAggregationTableNames(
      'SELECT * FROM pa.rollup',
      [['pa.rollup', { targetTableName: 'pa.rollup_$&_$1' }]],
    );
    expect(result).toBe('SELECT * FROM pa.rollup_$&_$1');
  });

  test('does not mutate the incoming array order', () => {
    const entries: PreAggTableToTempTableNames[] = [
      ['name1', { targetTableName: 'target1' }],
      ['name10', { targetTableName: 'target10' }],
    ];
    QueryCache.replacePreAggregationTableNames('SELECT * FROM name1, name10', entries);
    expect(entries.map(([tableName]) => tableName)).toEqual(['name1', 'name10']);
  });
});
