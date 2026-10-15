import { CubeStoreDriver } from '../src/CubeStoreDriver';

function fixture() {
  const driver = Object.create(CubeStoreDriver.prototype) as CubeStoreDriver;
  const query = jest.fn(async (_sql: string, _values: unknown[], _options?: unknown) => []);
  Object.assign(driver, { query });
  return { driver, query };
}

describe('Parquet import SQL', () => {
  test('uses the standard parameterized path and preserves build options', async () => {
    const { driver, query } = fixture();
    const location = 'gs://bucket/it\'s-a-file.parquet';
    await driver.uploadTableWithIndexes('schema.table', [{ name: 'id', type: 'int' }], { parquetFile: [location] }, [], null,
      { buildRangeEnd: '2026-01-01T00:00:00.000' });
    const [sql, values] = query.mock.calls[0] as unknown as [string, string[]];
    expect(sql).toContain('input_format = \'parquet\'');
    expect(sql).toContain('LOCATION ?');
    expect(sql).not.toContain(location);
    expect(values).toEqual([location]);
    expect(sql).toContain('build_range_end');
  });

  test('empty exports create an empty table without LOCATION', async () => {
    const { driver, query } = fixture();
    await driver.uploadTableWithIndexes('schema.table', [{ name: 'id', type: 'int' }], { parquetFile: [] }, [], null);
    expect((query.mock.calls[0] as unknown as [string])[0]).not.toContain('LOCATION');
  });

  test('rejects invalid locations and missing columns before executing SQL', async () => {
    const { driver, query } = fixture();
    await expect(driver.uploadTableWithIndexes('schema.table', [], { parquetFile: ['gs://bucket/a'] }, [], null)).rejects.toThrow('empty columns');
    await expect(driver.uploadTableWithIndexes('schema.table', [{ name: 'id', type: 'int' }], { parquetFile: [null] }, [], null)).rejects.toThrow('non-empty strings');
    expect(query).not.toHaveBeenCalled();
  });
});
