import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: FILTER_PARAMS', () => {
  it('records cube_name, name, and string column for a single filter', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any) => FILTER_PARAMS.orders.status.filter('col')
    );

    expect(result.template).toBe('{fp:0}');
    expect(result.args.filter_params).toHaveLength(1);
    expect(result.args.filter_params[0]).toEqual({
      cube_name: 'orders',
      name: 'status',
      column: 'col',
    });
  });

  it('exposes a callback column as a JS function and forwards the prepared arg to it', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any) => FILTER_PARAMS.orders.status.filter((c: string) => `${c} IN (1,2)`)
    );

    expect(result.template).toBe('{fp:0}');
    expect(result.args.filter_params).toHaveLength(1);

    const fp = result.args.filter_params[0];
    expect(fp.cube_name).toBe('orders');
    expect(fp.name).toBe('status');
    expect(typeof fp.column).toBe('function');
    expect((fp.column as Function)('orders.status')).toBe(
      'orders.status IN (1,2)'
    );
  });

  it('records distinct filter params for different cubes/members without dedup', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any) => `${FILTER_PARAMS.orders.status.filter('a')} AND ` +
        `${FILTER_PARAMS.orders.region.filter('b')} AND ` +
        `${FILTER_PARAMS.users.tier.filter('c')}`
    );

    expect(result.template).toBe('{fp:0} AND {fp:1} AND {fp:2}');
    expect(result.args.filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 'a' },
      { cube_name: 'orders', name: 'region', column: 'b' },
      { cube_name: 'users', name: 'tier', column: 'c' },
    ]);
  });

  it('does NOT dedup identical filter params — each .filter() call adds a new entry', () => {
    // Capturing this on purpose: filter_params uses Vec::push, not
    // unique_insert, unlike symbol_paths. If this changes, the test will
    // fail and force a deliberate decision.
    const result = compileMemberSql(
      (FILTER_PARAMS: any) => `${FILTER_PARAMS.orders.status.filter('col')} OR ` +
        `${FILTER_PARAMS.orders.status.filter('col')}`
    );

    expect(result.template).toBe('{fp:0} OR {fp:1}');
    expect(result.args.filter_params).toHaveLength(2);
    expect(result.args.filter_params[0]).toEqual({
      cube_name: 'orders',
      name: 'status',
      column: 'col',
    });
    expect(result.args.filter_params[1]).toEqual({
      cube_name: 'orders',
      name: 'status',
      column: 'col',
    });
  });
});
