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

  it('captures a reference via String() coercion', () => {
    const res = compileMemberSql((o) => String(o.amount), ['o']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['o', 'amount']]);
  });

  it('captures a reference via valueOf (concatenation) coercion', () => {
    // eslint-disable-next-line prefer-template
    const res = compileMemberSql((o) => o.amount + '', ['o']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['o', 'amount']]);
  });

  it('captures a direct (non-template) member return', () => {
    const res = compileMemberSql((o) => o.amount, ['o']);
    expect(res.template).toBe('{arg:0}');
    expect(res.symbolPaths).toEqual([['o', 'amount']]);
  });

  it('keeps .sql() and a plain member ref as distinct paths', () => {
    const res = compileMemberSql((o) => `${o.amount.sql()} ${o.amount}`, ['o']);
    expect(res.template).toBe('{arg:0} {arg:1}');
    expect(res.symbolPaths).toEqual([['o', 'amount', '__sql_fn'], ['o', 'amount']]);
  });

  it('keeps different leaves under a shared prefix distinct', () => {
    const res = compileMemberSql((o) => `${o.a.x} ${o.a.y}`, ['o']);
    expect(res.template).toBe('{arg:0} {arg:1}');
    expect(res.symbolPaths).toEqual([['o', 'a', 'x'], ['o', 'a', 'y']]);
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

  it('does not dedup distinct filter params', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter('a')} ${FILTER_PARAMS.line.b.filter('b')}`,
      ['FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0} {fp:1}');
    expect(res.filterParams.map((p) => [p.cube_name, p.name])).toEqual([['orders', 'a'], ['line', 'b']]);
  });

  it('does not dedup identical filter params', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter('a')} ${FILTER_PARAMS.orders.a.filter('a')}`,
      ['FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0} {fp:1}');
    expect(res.filterParams).toHaveLength(2);
  });

  it('produces an empty group when FILTER_GROUP() is called with no args', () => {
    const res = compileMemberSql((FILTER_GROUP) => `${FILTER_GROUP()}`, ['FILTER_GROUP']);
    expect(res.template).toBe('{fg:0}');
    expect(res.filterGroups).toEqual([{ filterParams: [] }]);
  });

  it('throws when FILTER_GROUP receives a non-FILTER_PARAMS arg', () => {
    expect(() => compileMemberSql(
      (FILTER_GROUP) => `${FILTER_GROUP('x')}`,
      ['FILTER_GROUP']
    )).toThrow();
  });

  it('coexists with a top-level FILTER_PARAMS reference in the same template', () => {
    const res = compileMemberSql(
      (FILTER_GROUP, FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter('a')} ${FILTER_GROUP(
        FILTER_PARAMS.orders.b.filter('b')
      )}`,
      ['FILTER_GROUP', 'FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0} {fg:0}');
    expect(res.filterParams).toHaveLength(1);
    expect(res.filterGroups[0].filterParams.map((p) => p.name)).toEqual(['b']);
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

  it('stringifies each element of a numeric array into IN (...)', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.ids.filter('i')}`,
      ['SECURITY_CONTEXT'],
      { ids: [1, 2] }
    );
    expect(res.template).toBe('i IN ({sv:0}, {sv:1})');
    expect(res.securityContextValues).toEqual(['1', '2']);
  });

  it('emits 1 = 0 for an empty array with a string column', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.ids.filter('i')}`,
      ['SECURITY_CONTEXT'],
      { ids: [] }
    );
    expect(res.template).toBe('1 = 0');
    expect(res.securityContextValues).toEqual([]);
  });

  it('passes an empty array to a callback column', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.ids.filter((vs) => `len=${vs.length}`)}`,
      ['SECURITY_CONTEXT'],
      { ids: [] }
    );
    expect(res.template).toBe('len=0');
  });

  it('formats an integer without a decimal point and a non-integer with one', () => {
    const int = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.n.filter('c')}`,
      ['SECURITY_CONTEXT'],
      { n: 42 }
    );
    const dec = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.n.filter('c')}`,
      ['SECURITY_CONTEXT'],
      { n: 1.5 }
    );
    expect(int.securityContextValues).toEqual(['42']);
    expect(dec.securityContextValues).toEqual(['1.5']);
  });

  it('formats a truthy boolean as the string "true"', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.flag.filter('c')}`,
      ['SECURITY_CONTEXT'],
      { flag: true }
    );
    expect(res.template).toBe('c = {sv:0}');
    expect(res.securityContextValues).toEqual(['true']);
  });

  it('rejects an unsupported value type', () => {
    expect(() => compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.obj.filter('c')}`,
      ['SECURITY_CONTEXT'],
      { obj: { nested: 1 } }
    )).toThrow();
  });

  it('navigates nested struct values through the recursive proxy', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.a.b.filter('c')}`,
      ['SECURITY_CONTEXT'],
      { a: { b: 'v' } }
    );
    expect(res.template).toBe('c = {sv:0}');
    expect(res.securityContextValues).toEqual(['v']);
  });

  it('does not crash on a missing deep leaf path', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `x=${SECURITY_CONTEXT.a.b.c}`,
      ['SECURITY_CONTEXT'],
      {}
    );
    expect(res.template).toBe('x=');
    expect(res.securityContextValues).toEqual([]);
  });

  it('renders a scalar leaf used directly as a single placeholder', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `tenant = ${SECURITY_CONTEXT.cubeCloud.tenantId}`,
      ['SECURITY_CONTEXT'],
      { cubeCloud: { tenantId: '123' } }
    );
    expect(res.template).toBe('tenant = {sv:0}');
    expect(res.securityContextValues).toEqual(['123']);
  });

  it('renders an array leaf directly as comma-joined placeholders', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `${SECURITY_CONTEXT.groups}`,
      ['SECURITY_CONTEXT'],
      { groups: ['a', 'b'] }
    );
    expect(res.template).toBe('{sv:0},{sv:1}');
    expect(res.securityContextValues).toEqual(['a', 'b']);
  });

  it('renders an empty array leaf directly as an empty string', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => `[${SECURITY_CONTEXT.groups}]`,
      ['SECURITY_CONTEXT'],
      { groups: [] }
    );
    expect(res.template).toBe('[]');
    expect(res.securityContextValues).toEqual([]);
  });

  it('dedups identical security values across repeated coercions', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT) => {
        const t = SECURITY_CONTEXT.tenant;
        return `${t} | ${t}`;
      },
      ['SECURITY_CONTEXT'],
      { tenant: 'acme' }
    );
    expect(res.template).toBe('{sv:0} | {sv:0}');
    expect(res.securityContextValues).toEqual(['acme']);
  });

  it('accepts camelCase and snake_case security-context arg names', () => {
    const camel = compileMemberSql(
      (ctx) => `${ctx.tenant.filter('c')}`,
      ['securityContext'],
      { tenant: 'acme' }
    );
    const snake = compileMemberSql(
      (ctx) => `${ctx.tenant.filter('c')}`,
      ['security_context'],
      { tenant: 'acme' }
    );
    expect(camel.template).toBe('c = {sv:0}');
    expect(snake.template).toBe('c = {sv:0}');
  });
});

describe('MemberSqlTemplateCompiler — result coercion', () => {
  it('coerces a number return to its string form', () => {
    expect(compileMemberSql(() => 42, []).template).toBe('42');
    expect(compileMemberSql(() => 1.5, []).template).toBe('1.5');
  });

  it('coerces a boolean return to its string form', () => {
    expect(compileMemberSql(() => true, []).template).toBe('true');
    expect(compileMemberSql(() => false, []).template).toBe('false');
  });

  it('returns an empty string for a null or undefined return', () => {
    expect(compileMemberSql(() => null, []).template).toBe('');
    expect(compileMemberSql(() => undefined, []).template).toBe('');
  });

  it('returns a constant template with no recorded paths for a string-literal return', () => {
    const res = compileMemberSql(() => 'CONST', []);
    expect(res.template).toBe('CONST');
    expect(res.symbolPaths).toEqual([]);
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
