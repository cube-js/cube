import { BaseDriver } from '../../src';

class BaseDriverImplementedMock extends BaseDriver {
  public constructor(protected readonly response: any) {
    super();
  }

  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public async testConnection(): Promise<void> {}

  public async query(_query, _values) {
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
});
