import { PrestoDriver } from '../../src/PrestoDriver';

describe('PrestoDriver prepareQueryWithParams', () => {
  let driver: PrestoDriver;

  beforeEach(() => {
    driver = new PrestoDriver({
      host: 'localhost',
      port: '8080',
      catalog: 'test',
      schema: 'default',
    });
  });

  it('formats simple queries without parameters', () => {
    const formatted = driver.prepareQueryWithParams('SELECT * FROM users', []);
    expect(formatted).toBe('SELECT * FROM users');
  });

  it('formats parameters with standard SQL escaping for single quotes', () => {
    const formatted = driver.prepareQueryWithParams(
      'SELECT * FROM users WHERE name = ? AND city = ?',
      ["maya'k", "O'Reilly"]
    );
    expect(formatted).toBe("SELECT * FROM users WHERE name = 'maya''k' AND city = 'O''Reilly'");
  });

  it('does not escape backslashes like MySQL', () => {
    const formatted = driver.prepareQueryWithParams(
      'SELECT * FROM users WHERE path = ?',
      ['C:\\Program Files\\App']
    );
    expect(formatted).toBe("SELECT * FROM users WHERE path = 'C:\\Program Files\\App'");
  });

  it('keeps wildcards and their escapes in LIKE queries', () => {
    const formatted = driver.prepareQueryWithParams(
      'SELECT * FROM users WHERE name LIKE ? ESCAPE ?',
      ['%foo\\_bar%', '\\']
    );
    expect(formatted).toBe("SELECT * FROM users WHERE name LIKE '%foo\\_bar%' ESCAPE '\\'");
  });
});
