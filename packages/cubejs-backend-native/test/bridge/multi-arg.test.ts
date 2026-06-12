import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: multi-arg dispatch', () => {
  it('wires CUBE, FILTER_PARAMS, and SECURITY_CONTEXT independently in one function', () => {
    const result = compileMemberSql(
      (CUBE: any, FILTER_PARAMS: any, SECURITY_CONTEXT: any) => `SUM(${CUBE.amount}) WHERE ` +
        `${FILTER_PARAMS.orders.status.filter('col')} AND ` +
        `${SECURITY_CONTEXT.tenant.filter('t')}`,
      { tenant: 'acme' }
    );

    expect(result.template).toBe(
      'SUM({arg:0}) WHERE {fp:0} AND t = {sv:0}'
    );
    expect(result.args.symbol_paths).toEqual([['CUBE', 'amount']]);
    expect(result.args.filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 'col' },
    ]);
    expect(result.args.security_context.values).toEqual(['acme']);
  });

  it('throws an internal error from the StubBaseTools when SQL_UTILS is referenced', () => {
    // The bridge harness deliberately does not provide a real BaseTools.
    // Tests that exercise SQL_UTILS need a richer stub; this test pins
    // current behavior so we notice if/when we add one.
    expect(() => compileMemberSql(
      (SQL_UTILS: any) => `${SQL_UTILS.someHelper()}`
    )).toThrow(/StubBaseTools::sql_utils_for_rust/);
  });
});
