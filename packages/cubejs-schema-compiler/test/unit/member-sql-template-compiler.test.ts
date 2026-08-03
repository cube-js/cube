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

  it('compiles a column callback into a template of its own and records {fp:N}', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.status.filter((c) => `${c} > 0`)}`,
      ['FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0}');
    expect(res.filterParams).toHaveLength(1);
    expect(res.filterParams[0].cube_name).toBe('orders');
    expect(res.filterParams[0].name).toBe('status');
    // The filter value it takes becomes a placeholder of its own.
    expect(res.filterParams[0].column.template).toBe('{fpv:0} > 0');
    expect(res.filterParams[0].column.valueParamsCount).toBe(1);
    expect(res.filterParams[0].column.symbolPaths).toEqual([]);
  });

  // What a callback references belongs to the callback, not to the member whose
  // sql declared it: the placeholders it emits index its own dependency list.
  it('records a callback reference into the callback, not the enclosing member', () => {
    const res = compileMemberSql(
      (CUBE, FILTER_PARAMS) => `${FILTER_PARAMS.orders.createdAt.filter((from, to) => `${CUBE.createdAt} >= ${from} AND ${CUBE.createdAt} < ${to}`)}`,
      ['CUBE', 'FILTER_PARAMS']
    );
    expect(res.template).toBe('{fp:0}');
    expect(res.symbolPaths).toEqual([]);
    expect(res.filterParams[0].column.template)
      .toBe('{arg:0} >= {fpv:0} AND {arg:0} < {fpv:1}');
    expect(res.filterParams[0].column.symbolPaths).toEqual([['CUBE', 'createdAt']]);
  });

  it('numbers callback references separately from the enclosing template', () => {
    const res = compileMemberSql(
      (CUBE, FILTER_PARAMS) => `${CUBE.a} AND ${FILTER_PARAMS.orders.b.filter((v) => `${CUBE.b} = ${v}`)}`,
      ['CUBE', 'FILTER_PARAMS']
    );
    expect(res.template).toBe('{arg:0} AND {fp:0}');
    expect(res.symbolPaths).toEqual([['CUBE', 'a']]);
    expect(res.filterParams[0].column.template).toBe('{arg:0} = {fpv:0}');
    expect(res.filterParams[0].column.symbolPaths).toEqual([['CUBE', 'b']]);
  });

  it('counts a defaulted parameter as a filter value', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter((from, to = 'x') => `d BETWEEN ${from} AND ${to}`)}`,
      ['FILTER_PARAMS']
    );
    expect(res.filterParams[0].column.template).toBe('d BETWEEN {fpv:0} AND {fpv:1}');
  });

  // A rest parameter takes as many values as the query supplies, which a fixed
  // set of placeholders cannot express.
  it('leaves a rest-parameter column callback uncompiled', () => {
    const res = compileMemberSql(
      (CUBE, FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter((...vals) => vals.map(v => `${CUBE.a} = ${v}`).join(' OR '))}`,
      ['CUBE', 'FILTER_PARAMS']
    );
    expect(typeof res.filterParams[0].column).toBe('function');
  });

  // Too few placeholders would render the missing values as `undefined`, so a
  // parameter list that cannot be read in full is left to render time.
  it.each([
    // eslint-disable-next-line no-extra-bind
    ['a bound callback', ((from, to) => `d >= ${from} AND d < ${to}`).bind(null)],
    ['a comment closing the parameter list', (from /* ) */, to) => `d >= ${from} AND d < ${to}`],
    ['a default containing a paren', (from, to = '(') => `d >= ${from} AND d < ${to}`],
    // `Function.length` is 0 from the first defaulted parameter on, so it can no
    // longer vouch for the parse — and a `)` in a string default breaks it.
    ['a first parameter defaulted to a paren', (from = ')', to) => `d >= ${from} AND d < ${to}`],
  ])('leaves %s uncompiled', (_name, column) => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter(column)}`,
      ['FILTER_PARAMS']
    );
    expect(typeof res.filterParams[0].column).toBe('function');
  });

  // A defaulted parameter only costs `Function.length` its witness; a list with
  // nothing for the scan to trip over is still read in full.
  it('compiles a callback whose first parameter has a plain default', () => {
    const res = compileMemberSql(
      (FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter((from = 1, to) => `d >= ${from} AND d < ${to}`)}`,
      ['FILTER_PARAMS']
    );
    expect(res.filterParams[0].column.template).toBe('d >= {fpv:0} AND d < {fpv:1}');
  });

  it('records a security context value referenced from a column callback into the callback', () => {
    const res = compileMemberSql(
      (SECURITY_CONTEXT, FILTER_PARAMS) => `${FILTER_PARAMS.orders.a.filter((v) => `t = ${SECURITY_CONTEXT.tenantId} AND a = ${v}`)}`,
      ['SECURITY_CONTEXT', 'FILTER_PARAMS'],
      { tenantId: 'acme' }
    );
    expect(res.securityContextValues).toEqual([]);
    expect(res.filterParams[0].column.template).toBe('t = {sv:0} AND a = {fpv:0}');
    expect(res.filterParams[0].column.securityContextValues).toEqual(['acme']);
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
