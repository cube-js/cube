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
});
