import { BaseDriver } from '../../src';

class BaseDriverImplementedMock extends BaseDriver {
  public constructor(protected readonly response: any) {
    super();
  }

  public async query(query, values) {
    return this.response;
  }
}

describe('BaseDriver', () => {
  test('downloadQueryResults - test type detection', async () => {
    const rows = [{
      bigint: 21474836479,
      int: 1,
      int_as_str: '1',
      int_as_str_zero: '0',
      int_as_str_negative: '-1',
      decimal_as_str: '1.000000000001',
      decimal_as_str_zero: '0.0000000',
      decimal_as_str_negative: '-1.000000000001',
      string: 'str',
    }];

    const driver = new BaseDriverImplementedMock(rows);

    expect((await driver.downloadQueryResults()).types).toEqual([
      { name: 'bigint', type: 'bigint' },
      { name: 'int', type: 'int' },
      { name: 'int_as_str', type: 'int' },
      { name: 'int_as_str_zero', type: 'int' },
      { name: 'int_as_str_negative', type: 'int' },
      { name: 'decimal_as_str', type: 'decimal' },
      { name: 'decimal_as_str_zero', type: 'decimal' },
      { name: 'decimal_as_str_negative', type: 'decimal' },
      { name: 'string', type: 'string' }
    ]);
  });
});
