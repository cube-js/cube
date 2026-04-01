import { CubeSymbols } from '../../src/compiler/CubeSymbols';

describe('CubeSymbols.contextSymbolsProxyFrom', () => {
  const allocateParam = (param: unknown) => `__param(${JSON.stringify(param)})`;

  it('unsafeValue returns leaf primitive value, not parent object', () => {
    const symbols = { cubeCloud: { groups: 'admin' } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.groups.unsafeValue()).toBe('admin');
  });

  it('unsafeValue returns leaf array value, not parent object', () => {
    const symbols = { cubeCloud: { groups: ['admin', 'user'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.groups.unsafeValue()).toEqual(['admin', 'user']);
  });

  it('unsafeValue returns intermediate object at each level', () => {
    const symbols = { cubeCloud: { groups: ['admin'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.unsafeValue()).toEqual({ groups: ['admin'] });
    expect(proxy.unsafeValue()).toEqual(symbols);
  });

  it('unsafeValue returns undefined for missing properties', () => {
    const symbols = { cubeCloud: { groups: ['admin'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.nonExistent.unsafeValue()).toBeUndefined();
  });

  it('unsafeValue returns numeric zero correctly', () => {
    const symbols = { tenant: { id: 0 } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.tenant.id.unsafeValue()).toBe(0);
  });

  it('filter works on nested primitive values', () => {
    const symbols = { cubeCloud: { tenantId: 'abc' } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.tenantId.filter('col')).toContain('col');
  });

  it('filter returns 1=1 for missing values', () => {
    const symbols = { cubeCloud: {} };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.tenantId.filter('col')).toBe('1 = 1');
  });

  it('filter with nested array passes allocated params to function for IN clause', () => {
    const symbols = { cubeCloud: { groups: ['admin', 'user'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = proxy.cubeCloud.groups.filter(
      (groups) => `col IN (${groups.join(', ')})`
    );
    expect(result).toBe('col IN (__param("admin"), __param("user"))');
  });

  it('filter with nested array and string column produces IN clause', () => {
    const symbols = { cubeCloud: { groups: ['admin', 'user'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = proxy.cubeCloud.groups.filter('col');
    expect(result).toBe('col IN (__param("admin"), __param("user"))');
  });

  it('filter with nested primitive and string column produces equality', () => {
    const symbols = { cubeCloud: { tenantId: 'abc' } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = proxy.cubeCloud.tenantId.filter('col');
    expect(result).toBe('col = __param("abc")');
  });

  it('toString on nested primitive allocates param for interpolation', () => {
    const symbols = { cubeCloud: { tenantId: 'abc' } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = `${proxy.cubeCloud.tenantId}`;
    expect(result).toBe('__param("abc")');
  });

  it('toString on nested array allocates each element as param', () => {
    const symbols = { cubeCloud: { groups: ['admin', 'user'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = `${proxy.cubeCloud.groups}`;
    expect(result).toBe('__param("admin"),__param("user")');
  });

  it('toString on proxy wrapping array allocates params via toPrimitive', () => {
    const symbols = { cubeCloud: { groups: ['admin', 'user'] } };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = String(proxy.cubeCloud.groups);
    expect(result).toBe('__param("admin"),__param("user")');
  });

  it('toString on missing property returns empty string', () => {
    const symbols = { cubeCloud: {} };
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    const result = `${proxy.cubeCloud.tenantId}`;
    expect(result).toBe('');
  });

  it('deeply nested access on empty context chains without error', () => {
    const symbols = {};
    const proxy = CubeSymbols.contextSymbolsProxyFrom(symbols, allocateParam) as any;

    expect(proxy.cubeCloud.tenantId.filter('col')).toBe('1 = 1');
    expect(proxy.cubeCloud.tenantId.unsafeValue()).toBeUndefined();
    expect(`${proxy.cubeCloud.tenantId}`).toBe('');
    expect(proxy.a.b.c.d.e.filter('col')).toBe('1 = 1');
  });
});
