import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: symbol paths via property_proxy', () => {
  describe('basic capture', () => {
    it('captures a simple cube ref via toString interception', () => {
      const result = compileMemberSql((CUBE: any) => `${CUBE.amount}`);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'amount']]);
    });

    it('captures a nested path', () => {
      const result = compileMemberSql((users: any) => `${users.address.city}`);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['users', 'address', 'city']]);
    });

    it('handles direct return without template literal (convert_to_string fallback)', () => {
      const result = compileMemberSql((CUBE: any) => CUBE.amount);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'amount']]);
    });

    it('exposes a constant template when the function returns a string literal', () => {
      const result = compileMemberSql(() => 'NOW()');

      expect(result.template).toBe('NOW()');
      expect(result.args.symbol_paths).toEqual([]);
    });
  });

  describe('coercion paths', () => {
    it('captures via String(x) explicit coercion', () => {
      const result = compileMemberSql((CUBE: any) => `value=${String(CUBE.x)}`);

      expect(result.template).toBe('value={arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'x']]);
    });

    it('captures via + "" concatenation (valueOf path)', () => {
      // eslint-disable-next-line prefer-template
      const result = compileMemberSql((CUBE: any) => `${'' + CUBE.x}`);

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'x']]);
    });
  });

  describe('.sql() accessor', () => {
    // Canonical user-facing usage: `<cube>.sql()` for subquery references —
    // e.g. `select v.* from ${visitors.sql()} as v` (see prod schemas in
    // packages/cubejs-schema-compiler/test/integration/postgres/...).
    // .sql is itself a function — JS does NOT auto-invoke it like
    // toString/valueOf, so user code must call it explicitly.

    it('records {arg:N} with __sql_fn suffix for cube.sql() at the cube root', () => {
      const result = compileMemberSql(
        (visitors: any) => `select v.* from ${visitors.sql()} v where v.source = 'google'`
      );

      expect(result.template).toBe(
        'select v.* from {arg:0} v where v.source = \'google\''
      );
      expect(result.args.symbol_paths).toEqual([['visitors', '__sql_fn']]);
    });

    it('keeps cube.sql() and a member ref as distinct paths', () => {
      const result = compileMemberSql(
        (orders: any) => `SELECT * FROM ${orders.sql()} WHERE created = ${orders.createdAt}`
      );

      expect(result.template).toBe(
        'SELECT * FROM {arg:0} WHERE created = {arg:1}'
      );
      expect(result.args.symbol_paths).toEqual([
        ['orders', '__sql_fn'],
        ['orders', 'createdAt'],
      ]);
    });
  });

  describe('dedup behavior', () => {
    it('dedups identical paths in the same template', () => {
      const result = compileMemberSql((CUBE: any) => `${CUBE.x} + ${CUBE.x}`);

      expect(result.template).toBe('{arg:0} + {arg:0}');
      expect(result.args.symbol_paths).toEqual([['CUBE', 'x']]);
    });

    it('dedups same path across different surrounding SQL contexts', () => {
      // Bare reference and the same reference wrapped in ceil() share one
      // {arg:N}. Parenthesizing/safety is handled downstream by SqlCall;
      // the bridge only records the path once via UniqueVector.
      const result = compileMemberSql(
        (cube: any) => `${cube.a} + ceil(${cube.a})`
      );

      expect(result.template).toBe('{arg:0} + ceil({arg:0})');
      expect(result.args.symbol_paths).toEqual([['cube', 'a']]);
    });

    it('treats different leaves under the same top-level as distinct paths', () => {
      const result = compileMemberSql(
        (CUBE: any) => `${CUBE.a} + ${CUBE.b}`
      );

      expect(result.template).toBe('{arg:0} + {arg:1}');
      expect(result.args.symbol_paths).toEqual([
        ['CUBE', 'a'],
        ['CUBE', 'b'],
      ]);
    });
  });

  describe('view chains', () => {
    it('captures sibling view chains as distinct paths sharing a top-level', () => {
      const result = compileMemberSql(
        (view: any) => `${view.v1.field} + ${view.v2.field}`
      );

      expect(result.template).toBe('{arg:0} + {arg:1}');
      expect(result.args.symbol_paths).toEqual([
        ['view', 'v1', 'field'],
        ['view', 'v2', 'field'],
      ]);
    });

    it('dedups identical view chains', () => {
      const result = compileMemberSql(
        (view: any) => `${view.v1.field} + ${view.v1.field}`
      );

      expect(result.template).toBe('{arg:0} + {arg:0}');
      expect(result.args.symbol_paths).toEqual([['view', 'v1', 'field']]);
    });

    it('keeps different leaves under a shared chain prefix as distinct', () => {
      const result = compileMemberSql(
        (view: any) => `${view.v1.a} + ${view.v1.b}`
      );

      expect(result.template).toBe('{arg:0} + {arg:1}');
      expect(result.args.symbol_paths).toEqual([
        ['view', 'v1', 'a'],
        ['view', 'v1', 'b'],
      ]);
    });

    it('captures deeper view chains (4+ levels)', () => {
      const result = compileMemberSql(
        (view: any) => `${view.v1.sub.field}`
      );

      expect(result.template).toBe('{arg:0}');
      expect(result.args.symbol_paths).toEqual([
        ['view', 'v1', 'sub', 'field'],
      ]);
    });

    it('handles mixed-depth view chains in one template', () => {
      const result = compileMemberSql(
        (view: any) => `${view.v1.field} + ${view.v2.sub.field}`
      );

      expect(result.template).toBe('{arg:0} + {arg:1}');
      expect(result.args.symbol_paths).toEqual([
        ['view', 'v1', 'field'],
        ['view', 'v2', 'sub', 'field'],
      ]);
    });
  });
});
