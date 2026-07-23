import { BaseDriver, detectTypesFromTabular } from '../../src';

class BaseDriverImplementedMock extends BaseDriver {
  public constructor(protected readonly response: any) {
    super();
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public async testConnection(): Promise<void> {}

  public async query(_query: string, _values: unknown[]) {
    return this.response;
  }
}

describe('BaseDriver', () => {
  test('downloadQueryResults - test type detection', async () => {
    const rows = [{
      bigint: 21474836479,
      bigint_because_int_max: 2147483648,
      bigint_because_int_min: -2147483649,
      bigint_str_because_int_max: '2147483648',
      bigint_str_because_int_min: '-2147483649',
      int: 1,
      int_zero: 0,
      int_as_str: '1',
      int_as_str_zero: '0',
      int_as_str_negative: '-1',
      decimal_as_str: '1.000000000001',
      decimal_as_str_zero: '0.0000000',
      decimal_as_str_negative: '-1.000000000001',
      decimal_because_bigint_max: '9223372036854775808',
      decimal_because_bigint_min: '-9223372036854775809',
      string: 'str',
    }];

    const driver = new BaseDriverImplementedMock(rows);

    // @ts-expect-error redundant test case
    expect((await driver.downloadQueryResults()).types).toEqual([
      { name: 'bigint', type: 'bigint' },
      { name: 'bigint_because_int_max', type: 'bigint' },
      { name: 'bigint_because_int_min', type: 'bigint' },
      { name: 'bigint_str_because_int_max', type: 'bigint' },
      { name: 'bigint_str_because_int_min', type: 'bigint' },
      { name: 'int', type: 'int' },
      { name: 'int_zero', type: 'int' },
      { name: 'int_as_str', type: 'int' },
      { name: 'int_as_str_zero', type: 'int' },
      { name: 'int_as_str_negative', type: 'int' },
      { name: 'decimal_as_str', type: 'decimal' },
      { name: 'decimal_as_str_zero', type: 'decimal' },
      { name: 'decimal_as_str_negative', type: 'decimal' },
      { name: 'decimal_because_bigint_max', type: 'decimal' },
      { name: 'decimal_because_bigint_min', type: 'decimal' },
      { name: 'string', type: 'string' }
    ]);
  });
  
  test('downloadQueryResults - type detection ignores NULL values (#11094)', async () => {
    const rows = [
      {
        decimal_with_null: '1.000000000001',
        int_with_null: 1,
        bigint_with_null: 21474836479,
        timestamp_with_null: '2020-01-01T00:00:00.000',
        string_with_null: 'str',
        boolean_with_null: true,
        all_null: null,
      },
      {
        decimal_with_null: null,
        int_with_null: null,
        bigint_with_null: null,
        timestamp_with_null: null,
        string_with_null: null,
        boolean_with_null: null,
        all_null: null,
      },
    ];

    const driver = new BaseDriverImplementedMock(rows);

    // @ts-expect-error redundant test case
    expect((await driver.downloadQueryResults()).types).toEqual([
      { name: 'decimal_with_null', type: 'decimal' },
      { name: 'int_with_null', type: 'int' },
      { name: 'bigint_with_null', type: 'bigint' },
      { name: 'timestamp_with_null', type: 'timestamp' },
      { name: 'string_with_null', type: 'string' },
      { name: 'boolean_with_null', type: 'boolean' },
      // A column that holds only NULLs has no detectable type
      { name: 'all_null', type: 'text' },
    ]);
  });

  test('detectTypesFromTabular - throws on empty tabular data', () => {
    expect(() => detectTypesFromTabular([])).toThrow('Unable to detect column types');
  });

  test('detectTypesFromTabular - infers type from a later row when the first is NULL (#11094)', () => {
    const rows = [
      { decimal: null, int: null, string: null },
      { decimal: '1.5', int: 1, string: 'str' },
    ];

    expect(detectTypesFromTabular(rows)).toEqual([
      { name: 'decimal', type: 'decimal' },
      { name: 'int', type: 'int' },
      { name: 'string', type: 'string' },
    ]);
  });

  test('detectTypesFromTabular - a column that is NULL across all rows falls back to text (#11094)', () => {
    const rows = [
      { known: 1, unknown: null },
      { known: 2, unknown: null },
    ];

    expect(detectTypesFromTabular(rows)).toEqual([
      { name: 'known', type: 'int' },
      { name: 'unknown', type: 'text' },
    ]);
  });

  test('tablesSchema() never marks primary key columns, even when the table has a real primary key (#11270)', async () => {
    // Mirrors what a real driver's informationSchemaQuery() returns for a table
    // whose primary key column is not literally named "id" - e.g. the Chinook
    // sample database's `album.album_id`, `track.track_id`, etc. These rows
    // carry no key information because informationSchemaQuery() never selects
    // it (see BaseDriver#informationSchemaQuery / #informationColumnsSchemaReducer).
    const rows = [
      { column_name: 'album_id', table_name: 'album', table_schema: 'public', data_type: 'integer' },
      { column_name: 'title', table_name: 'album', table_schema: 'public', data_type: 'character varying' },
      { column_name: 'artist_id', table_name: 'album', table_schema: 'public', data_type: 'integer' },
    ];

    const driver = new BaseDriverImplementedMock(rows);
    const schema = await driver.tablesSchema();
    const albumIdColumn: any = schema.public.album.find((c: any) => c.name === 'album_id');

    // album_id is the table's actual primary key, so it should be reported
    // the same way BaseDriver#tablesSchemaV2 reports it - otherwise
    // ScaffoldingSchema (which only falls back to a literal column name of
    // "id") has no way to know it's a primary key, and generated cubes for
    // tables like this end up with no `primary_key: true` dimension at all.
    expect(albumIdColumn?.attributes).toEqual(['primaryKey']);
  });

  test('wrapQueryWithLimit wraps the query with a limit', () => {
    const driver = new BaseDriverImplementedMock({});
    const query = { query: 'SELECT * FROM users', limit: 10 };
    driver.wrapQueryWithLimit(query);
    expect(query).toEqual({
      query: 'SELECT * FROM (SELECT * FROM users) AS t LIMIT 10',
      limit: 10,
    });
  });
});
