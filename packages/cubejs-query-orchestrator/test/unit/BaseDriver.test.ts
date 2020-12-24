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
      intstr: '1',
      string: 'str',
    }];

    const driver = new BaseDriverImplementedMock(rows);

    expect((await driver.downloadQueryResults()).types).toEqual([
      { name: 'bigint', type: 'bigint' },
      { name: 'int', type: 'int' },
      { name: 'intstr', type: 'int' },
      { name: 'string', type: 'string' }
    ]);
  });
});
