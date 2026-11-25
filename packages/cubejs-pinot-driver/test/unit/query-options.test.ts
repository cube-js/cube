import { PinotDriver } from '../../src/PinotDriver';

class TestPinotDriver extends PinotDriver {
  public override queryOptions(requestId?: string) {
    return super.queryOptions(requestId);
  }
}

describe('PinotDriver query options', () => {
  const baseOptions = 'useMultistageEngine=true;enableNullHandling=false;timeoutMs=30000';
  const uuid = '22cf5b37-2351-4eab-a501-617fcf8baac6';

  let driver: TestPinotDriver;

  beforeAll(() => {
    driver = new TestPinotDriver({
      host: 'localhost',
      port: '8099',
      dataSource: 'default',
      nullHandling: false,
      queryTimeout: 30,
    });
  });

  it('sends only the built-in options without a request id', () => {
    expect(driver.queryOptions()).toBe(baseOptions);
    expect(driver.queryOptions('')).toBe(baseOptions);
  });

  it('sends the request id as clientQueryId', () => {
    expect(driver.queryOptions(uuid)).toBe(`${baseOptions};clientQueryId=${uuid}`);
  });

  it('strips the span suffix from the request id', () => {
    expect(driver.queryOptions(`${uuid}-span-1`)).toBe(`${baseOptions};clientQueryId=${uuid}`);
  });

  it('skips a request id that would break the options string', () => {
    expect(driver.queryOptions('abc=;timeoutMs=1')).toBe(baseOptions);
    expect(driver.queryOptions('YWJj=')).toBe(baseOptions);
    expect(driver.queryOptions('abc;timeoutMs')).toBe(baseOptions);
  });

  it('skips an invalid request id', () => {
    expect(driver.queryOptions('abc def')).toBe(baseOptions);
    expect(driver.queryOptions('a'.repeat(129))).toBe(baseOptions);
  });

  describe('applicationName and workloadName', () => {
    const config = {
      host: 'localhost',
      port: '8099',
      dataSource: 'default',
      nullHandling: false,
      queryTimeout: 30,
    };

    it('sends applicationName and workloadName when configured', () => {
      const tagged = new TestPinotDriver({ ...config, applicationName: 'cube', workloadName: 'bi' });

      expect(tagged.queryOptions()).toBe(`${baseOptions};applicationName=cube;workloadName=bi`);
      expect(tagged.queryOptions(uuid)).toBe(
        `${baseOptions};applicationName=cube;workloadName=bi;clientQueryId=${uuid}`
      );
    });

    it('sends each option independently', () => {
      expect(new TestPinotDriver({ ...config, applicationName: 'cube' }).queryOptions())
        .toBe(`${baseOptions};applicationName=cube`);
      expect(new TestPinotDriver({ ...config, workloadName: 'bi' }).queryOptions())
        .toBe(`${baseOptions};workloadName=bi`);
    });

    it('rejects values that would break the options string', () => {
      expect(() => new TestPinotDriver({ ...config, applicationName: 'cube;timeoutMs=1' })).toThrow(/applicationName/);
      expect(() => new TestPinotDriver({ ...config, applicationName: 'a=b' })).toThrow(/applicationName/);
      expect(() => new TestPinotDriver({ ...config, workloadName: 'bi;trace=true' })).toThrow(/workloadName/);
      expect(() => new TestPinotDriver({ ...config, workloadName: ' ' })).toThrow(/workloadName/);
    });
  });
});
