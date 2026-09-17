import {
  findJdbcUrlParam,
  formatJdbcUrl,
  parseJdbcUrl,
  removeJdbcUrlParams,
} from '../../src/jdbc-url';

const url = 'jdbc:databricks://host.example.net:443;httpPath=/sql/1.0/warehouses/abc;UID=token;PWD=secret';

describe('JDBC URL', () => {
  it('splits the base off the parameters', () => {
    expect(parseJdbcUrl(url).base).toBe('jdbc:databricks://host.example.net:443');
    expect(parseJdbcUrl(url).params.map(({ key }) => key)).toEqual(['httpPath', 'UID', 'PWD']);
  });

  it('splits an entry on its first "=" only', () => {
    const { params } = parseJdbcUrl('jdbc:databricks://host;httpPath=/sql/1.0/endpoints/x?o=123');

    expect(params[0]).toMatchObject({ key: 'httpPath', value: '/sql/1.0/endpoints/x?o=123' });
  });

  it('treats an entry without "=" as an empty value', () => {
    expect(parseJdbcUrl('jdbc:databricks://host;SSL').params[0]).toMatchObject({ key: 'SSL', value: '' });
  });

  it('round-trips an untouched URL byte for byte', () => {
    expect(formatJdbcUrl(parseJdbcUrl(url))).toBe(url);
  });

  it('matches parameter names case-insensitively', () => {
    expect(findJdbcUrlParam(parseJdbcUrl(url), 'httppath')?.value).toBe('/sql/1.0/warehouses/abc');
    expect(findJdbcUrlParam(parseJdbcUrl(url), 'missing')).toBeUndefined();
  });

  // A padded name is a different (unknown) parameter to the driver, so trimming here would reject
  // URLs it happily accepts.
  it('does not trim whitespace around the name or value', () => {
    const { params } = parseJdbcUrl('jdbc:databricks://host;UID = token');

    expect(params[0]).toMatchObject({ key: 'UID ', value: ' token' });
    expect(findJdbcUrlParam(parseJdbcUrl('jdbc:databricks://host;UID = token'), 'UID')).toBeUndefined();
  });

  it('matches a name only as a whole entry, not as a substring', () => {
    const parsed = parseJdbcUrl('jdbc:databricks://host;MyUID=x;httpPath=/a');

    expect(findJdbcUrlParam(parsed, 'UID')).toBeUndefined();
    expect(formatJdbcUrl(removeJdbcUrlParams(parsed, ['UID']))).toBe('jdbc:databricks://host;MyUID=x;httpPath=/a');
  });

  it('removes every occurrence of the named parameters', () => {
    const parsed = parseJdbcUrl('jdbc:databricks://host;UID=a;httpPath=/a;uid=b;PWD=c');

    expect(formatJdbcUrl(removeJdbcUrlParams(parsed, ['UID', 'PWD']))).toBe('jdbc:databricks://host;httpPath=/a');
  });
});
