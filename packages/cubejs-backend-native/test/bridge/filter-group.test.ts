import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: FILTER_GROUP', () => {
  it('wraps a single FILTER_PARAMS arg into a group', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any, FILTER_GROUP: any) => FILTER_GROUP(FILTER_PARAMS.orders.status.filter('col'))
    );

    expect(result.template).toBe('{fg:0}');
    expect(result.args.filter_groups).toHaveLength(1);
    expect(result.args.filter_groups[0].filter_params).toHaveLength(1);
    expect(result.args.filter_groups[0].filter_params[0]).toEqual({
      cube_name: 'orders',
      name: 'status',
      column: 'col',
    });
    // Filter params used only inside FILTER_GROUP are not promoted to the
    // top-level filter_params list.
    expect(result.args.filter_params).toEqual([]);
  });

  it('preserves member identity across multiple grouped FILTER_PARAMS', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any, FILTER_GROUP: any) => FILTER_GROUP(
        FILTER_PARAMS.orders.status.filter('a'),
        FILTER_PARAMS.users.tier.filter('b')
      )
    );

    expect(result.template).toBe('{fg:0}');
    expect(result.args.filter_groups[0].filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 'a' },
      { cube_name: 'users', name: 'tier', column: 'b' },
    ]);
  });

  it('coexists with a top-level FILTER_PARAMS reference in the same template', () => {
    const result = compileMemberSql(
      (FILTER_PARAMS: any, FILTER_GROUP: any) => `${FILTER_PARAMS.orders.region.filter('r')} AND ${FILTER_GROUP(
        FILTER_PARAMS.orders.status.filter('s')
      )}`
    );

    expect(result.template).toBe('{fp:0} AND {fg:0}');
    expect(result.args.filter_params).toHaveLength(1);
    expect(result.args.filter_params[0]).toEqual({
      cube_name: 'orders',
      name: 'region',
      column: 'r',
    });
    expect(result.args.filter_groups[0].filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 's' },
    ]);
  });

  it('throws a user error when FILTER_GROUP receives a non-FILTER_PARAMS arg', () => {
    expect(() => compileMemberSql((FILTER_GROUP: any) => FILTER_GROUP('not a filter'))).toThrow(/FILTER_GROUP expects FILTER_PARAMS args to be passed/);
  });

  it('produces an empty group with no filter_params when FILTER_GROUP() is called with no args', () => {
    const result = compileMemberSql((FILTER_GROUP: any) => FILTER_GROUP());

    expect(result.template).toBe('{fg:0}');
    expect(result.args.filter_groups).toHaveLength(1);
    expect(result.args.filter_groups[0].filter_params).toEqual([]);
  });

  it('does NOT dedup the same FILTER_PARAMS across two FILTER_GROUP() calls', () => {
    // Mirrors filter-params anti-dedup: each FILTER_GROUP gets its own copy
    // of the items via push, no UniqueVector.
    const result = compileMemberSql(
      (FILTER_PARAMS: any, FILTER_GROUP: any) => `${FILTER_GROUP(FILTER_PARAMS.orders.status.filter('s'))} OR ` +
        `${FILTER_GROUP(FILTER_PARAMS.orders.status.filter('s'))}`
    );

    expect(result.template).toBe('{fg:0} OR {fg:1}');
    expect(result.args.filter_groups).toHaveLength(2);
    expect(result.args.filter_groups[0].filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 's' },
    ]);
    expect(result.args.filter_groups[1].filter_params).toEqual([
      { cube_name: 'orders', name: 'status', column: 's' },
    ]);
  });
});
