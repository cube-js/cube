import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: compile_template_sql', () => {
  describe('symbol paths', () => {
    it('captures a simple cube ref', () => {
      const result = compileMemberSql((CUBE: any) => `${CUBE.amount}`);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'amount']]);
      expect(result.args.filter_params).toEqual([]);
      expect(result.args.filter_groups).toEqual([]);
      expect(result.args.security_context.values).toEqual([]);
    });

    it('dedups identical paths in template', () => {
      const result = compileMemberSql((CUBE: any) => `${CUBE.x} + ${CUBE.x}`);

      expect(result.template).toBe('{arg:0} + {arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'x']]);
    });

    it('captures nested paths', () => {
      const result = compileMemberSql((users: any) => `${users.address.city}`);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['users', 'address', 'city']]);
    });
  });

  describe('FILTER_PARAMS', () => {
    it('records cube_name, name, and string column', () => {
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
  });

  describe('SECURITY_CONTEXT', () => {
    it('records string filter value (leaf proxy eagerly registers a duplicate)', () => {
      // SECURITY_CONTEXT.tenant returns a leaf proxy. Constructing the leaf
      // proxy eagerly runs security_context_to_string_fn, which pushes 'acme'
      // into security_context.values even though we never coerce the leaf to
      // string. The subsequent .filter('col') call pushes 'acme' again. So
      // the resulting placeholder index is 1, not 0, and values has two
      // entries. Capturing this here so any future change to the eagerness
      // shows up as an explicit test diff.
      const result = compileMemberSql(
        (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
        { tenant: 'acme' }
      );

      expect(result.template).toBe('col = {sv:1}');
      expect(result.args.security_context.values).toEqual(['acme', 'acme']);
    });

    it('throws TesseractUserError when requiredFilter field is missing', () => {
      expect(() => compileMemberSql(
        (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.requiredFilter('col')}`,
        {}
      )).toThrow(/Filter for col is required/);
    });
  });
});
