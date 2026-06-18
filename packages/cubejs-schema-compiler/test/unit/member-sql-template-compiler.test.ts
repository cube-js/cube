// eslint-disable-next-line @typescript-eslint/no-var-requires
const { compileMemberSql, uniqueInsertPath } = require('../../src/adapter/MemberSqlTemplateCompiler');

describe('MemberSqlTemplateCompiler — member reference path', () => {
  it('records a single member reference via string coercion', () => {
    const res = compileMemberSql((orders) => `${orders.amount}`, ['orders']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['orders', 'amount']]);
    expect(res.filterParams).toEqual([]);
  });

  it('records nested member paths', () => {
    const res = compileMemberSql((cube) => `${cube.a.b.c}`, ['cube']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['cube', 'a', 'b', 'c']]);
  });

  it('records the .sql() function call form with the __sql_fn suffix', () => {
    const res = compileMemberSql((orders) => `${orders.amount.sql()}`, ['orders']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['orders', 'amount', '__sql_fn']]);
  });

  it('dedups repeated identical references to the same {arg:N}', () => {
    const res = compileMemberSql((o) => `${o.amount} + ${o.amount}`, ['o']);
    expect(res.template).toBe('{arg:0} + {arg:0}');
    expect(res.symbolPaths).toEqual([['o', 'amount']]);
  });

  it('assigns distinct indices to distinct references', () => {
    const res = compileMemberSql((o) => `${o.a} - ${o.b}`, ['o']);
    expect(res.template).toBe('{arg:0} - {arg:1}');
    expect(res.symbolPaths).toEqual([['o', 'a'], ['o', 'b']]);
  });

  it('handles multiple cube args', () => {
    const res = compileMemberSql((a, b) => `${a.x} = ${b.y}`, ['a', 'b']);
    expect(res.template).toBe('{arg:0} = {arg:1}');
    expect(res.symbolPaths).toEqual([['a', 'x'], ['b', 'y']]);
  });

  it('supports an array template result (member sql returning an array)', () => {
    const res = compileMemberSql((o) => [o.a, o.b], ['o']);
    expect(res.template).toEqual(['{arg:0}', '{arg:1}']);
    expect(res.symbolPaths).toEqual([['o', 'a'], ['o', 'b']]);
  });
});

describe('MemberSqlTemplateCompiler — FILTER_PARAMS / FILTER_GROUP', () => {
  it('records a filter param with a string column and yields {fp:0}', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.status.filter('t.status')}`,
      ['FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0}');
    expect(res.filterParams).toEqual([{ cube_name: 'orders', name: 'status', column: 't.status' }]);
  });

  it('keeps the column callback as a function (deferred) and records {fp:N}', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.status.filter((c) => `${c} > 0`)}`,
      ['FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0}');
    expect(res.filterParams).toHaveLength(1);
    expect(res.filterParams[0].cube_name).toBe('orders');
    expect(res.filterParams[0].name).toBe('status');
    expect(typeof res.filterParams[0].column).toBe('function');
    const captured = res.filterParams[0].column;
    expect(captured('X')).toBe('X > 0');
  });

  it('records a filter group from filter-param args and yields {fg:0}', () => {
    const res = compileMemberSql(
      (FILTER_GROUP, FILTER_PARAMS) => `${FILTER_GROUP(
        FILTER_PARAMS.orders.a.filter('a'),
        FILTER_PARAMS.orders.b.filter('b')
      )}`,
      ['FILTER_GROUP', 'FILTER_PARAMS']
    );
    expect(res.template).toBe('{fg:0}');
    expect(res.filterGroups).toHaveLength(1);
    expect(res.filterGroups[0].filterParams.map((p) => p.name)).toEqual(['a', 'b']);
  });
});

describe('MemberSqlTemplateCompiler — SECURITY_CONTEXT', () => {
  it('filter() with string column records the value and emits col = {sv:0}', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.tenantId.filter('t.id')}`,
      ['SECURITY_CONTEXT'],
      { tenantId: 'acme' }
    );
    expect(res.template).toBe('t.id = {sv:0}');
    expect(res.securityContextValues).toEqual(['acme']);
  });

  it('filter() with a callback passes the {sv:N} placeholder into the callback', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.tenantId.filter((c) => `${c} IN (sub)`)}`,
      ['SECURITY_CONTEXT'],
      { tenantId: 'acme' }
    );
    expect(res.template).toBe('{sv:0} IN (sub)');
    expect(res.securityContextValues).toEqual(['acme']);
  });

  it('array value emits IN (...) with one {sv:N} per element', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.roles.filter('r')}`,
      ['SECURITY_CONTEXT'],
      { roles: ['a', 'b'] }
    );
    expect(res.template).toBe('r IN ({sv:0}, {sv:1})');
    expect(res.securityContextValues).toEqual(['a', 'b']);
  });

  it('missing value: filter() emits 1 = 1, requiredFilter() throws', () => {
    const ok = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.missing.filter('x')}`,
      ['SECURITY_CONTEXT'],
      {}
    );
    expect(ok.template).toBe('1 = 1');
    expect(ok.securityContextValues).toEqual([]);

    expect(() => compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.missing.requiredFilter('x')}`,
      ['SECURITY_CONTEXT'],
      {}
    )).toThrow();
  });

  it('toString coercion records the value as {sv:0}; unsafeValue returns raw', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.tenantId}|${SECURITY_CONTEXT.tenantId.unsafeValue()}`,
      ['SECURITY_CONTEXT'],
      { tenantId: 'acme' }
    );
    expect(res.template).toBe('{sv:0}|acme');
    expect(res.securityContextValues).toEqual(['acme']);
  });
});

describe('MemberSqlTemplateCompiler — SQL_UTILS', () => {
  it('passes the provided sqlUtils through to the template', () => {
    const res = compileMemberSql(
      (SQL_UTILS) => `${SQL_UTILS.convertTz('x')}`,
      ['SQL_UTILS'],
      undefined,
      { convertTz: (c) => `TZ(${c})` }
    );
    expect(res.template).toBe('TZ(x)');
  });
});

describe('uniqueInsertPath', () => {
  it('returns existing index for an equal path and appends new ones', () => {
    const paths = [];
    expect(uniqueInsertPath(paths, ['a', 'b'])).toBe(0);
    expect(uniqueInsertPath(paths, ['a', 'c'])).toBe(1);
    expect(uniqueInsertPath(paths, ['a', 'b'])).toBe(0);
    expect(paths).toEqual([['a', 'b'], ['a', 'c']]);
  });
});
