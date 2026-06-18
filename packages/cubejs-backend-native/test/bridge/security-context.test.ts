import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: SECURITY_CONTEXT — filter input shapes', () => {
  it('handles a string filter value as col = {sv:0}', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      { tenant: 'acme' }
    );

    expect(result.template).toBe('col = {sv:0}');
    expect(result.args.security_context.values).toEqual(['acme']);
  });

  it('handles a string array as col IN (sv0, sv1, sv2)', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter('col')}`,
      { groups: ['a', 'b', 'c'] }
    );

    expect(result.template).toBe('col IN ({sv:0}, {sv:1}, {sv:2})');
    expect(result.args.security_context.values).toEqual(['a', 'b', 'c']);
  });

  it('handles a numeric array by stringifying each element', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.ids.filter('id')}`,
      { ids: [1, 2, 3] }
    );

    expect(result.template).toBe('id IN ({sv:0}, {sv:1}, {sv:2})');
    expect(result.args.security_context.values).toEqual(['1', '2', '3']);
  });

  it('renders an empty string array as 1 = 0 with no values registered for the filter', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter('col')}`,
      { groups: [] }
    );

    expect(result.template).toBe('1 = 0');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('passes an empty array to a callback column for the empty-array case', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter(
        (vals: string[]) => `received:${vals.length}`
      )}`,
      { groups: [] }
    );

    expect(result.template).toBe('received:0');
  });

  it('formats an integer-valued number without a decimal point', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.user_id.filter('uid')}`,
      { user_id: 42 }
    );

    expect(result.template).toBe('uid = {sv:0}');
    expect(result.args.security_context.values).toEqual(['42']);
  });

  it('formats a non-integer number as a decimal string', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.factor.filter('f')}`,
      { factor: 1.5 }
    );

    expect(result.template).toBe('f = {sv:0}');
    expect(result.args.security_context.values).toEqual(['1.5']);
  });

  it('formats a truthy boolean as the string "true"', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.flag.filter('f')}`,
      { flag: true }
    );

    expect(result.template).toBe('f = {sv:0}');
    expect(result.args.security_context.values).toEqual(['true']);
  });

  it.each([
    ['missing', undefined],
    ['null', null],
    ['empty string', ''],
    ['zero', 0],
    ['false', false],
  ])('returns 1 = 1 when filter value is %s', (_, value) => {
    const ctx = value === undefined ? {} : { tenant: value };
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      ctx
    );

    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it.each([
    ['missing', undefined],
    ['null', null],
    ['empty string', ''],
    ['zero', 0],
    ['false', false],
  ])('throws when requiredFilter value is %s', (_, value) => {
    const ctx = value === undefined ? {} : { tenant: value };

    expect(() => compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.requiredFilter('col')}`,
      ctx
    )).toThrow(/Filter for col is required/);
  });

  it('rejects an unsupported value type with a user error', () => {
    expect(() => compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.bad.filter('col')}`,
      { bad: { nested: 'object' } }
    )).toThrow(/Invalid param for security context/);
  });
});

describeBridge('bridge: SECURITY_CONTEXT — proxy structure', () => {
  it('navigates nested struct values through the recursive proxy', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.id.filter('col')}`,
      { tenant: { id: '123' } }
    );

    expect(result.template).toBe('col = {sv:0}');
    expect(result.args.security_context.values).toEqual(['123']);
  });

  it('does not crash on a deep leaf-proxy path that does not exist in the context', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.a.b.c.filter('col')}`,
      {}
    );

    // Filter on a fully-undefined leaf path: the value resolves to None and
    // the filter falls back to "1 = 1" (the path is treated as an absent
    // optional filter, not an error).
    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('exposes unsafeValue() that returns the raw value without registering a placeholder', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `prefix-${SECURITY_CONTEXT.tenant.unsafeValue()}-suffix`,
      { tenant: 'acme' }
    );

    expect(result.template).toBe('prefix-acme-suffix');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('lets the user branch the template at compile time via unsafeValue()', () => {
    // Real prod pattern: unsafeValue() returns the actual JS value, so a
    // ternary in the template literal picks one branch at compile time
    // and the resulting template is just the picked literal — no
    // placeholders registered.
    const adminResult = compileMemberSql(
      (SECURITY_CONTEXT: any) => `SELECT * FROM ${
        SECURITY_CONTEXT.cubeCloud.groups.unsafeValue() === 'admin'
          ? 'admin_orders'
          : 'public_orders'
      }`,
      { cubeCloud: { groups: 'admin' } }
    );
    const viewerResult = compileMemberSql(
      (SECURITY_CONTEXT: any) => `SELECT * FROM ${
        SECURITY_CONTEXT.cubeCloud.groups.unsafeValue() === 'admin'
          ? 'admin_orders'
          : 'public_orders'
      }`,
      { cubeCloud: { groups: 'viewer' } }
    );

    expect(adminResult.template).toBe('SELECT * FROM admin_orders');
    expect(viewerResult.template).toBe('SELECT * FROM public_orders');
    expect(adminResult.args.security_context.values).toEqual([]);
    expect(viewerResult.args.security_context.values).toEqual([]);
  });

  it('renders a scalar leaf used directly in a template as a single placeholder', () => {
    // `tenant_id = ${SECURITY_CONTEXT.cubeCloud.tenantId}` — common in prod.
    // Coerce-time toString fires once and registers a single placeholder.
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `tenant_id = ${SECURITY_CONTEXT.cubeCloud.tenantId}`,
      { cubeCloud: { tenantId: '123' } }
    );

    expect(result.template).toBe('tenant_id = {sv:0}');
    expect(result.args.security_context.values).toEqual(['123']);
  });

  it('renders an array leaf directly in a template as comma-joined placeholders', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups}`,
      { groups: ['a', 'b'] }
    );

    expect(result.template).toBe('{sv:0},{sv:1}');
    expect(result.args.security_context.values).toEqual(['a', 'b']);
  });

  it('renders an empty array leaf directly in a template as an empty string', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `[${SECURITY_CONTEXT.groups}]`,
      { groups: [] }
    );

    expect(result.template).toBe('[]');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('allocates a fresh placeholder on every coercion of the same leaf proxy', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => {
        const t = SECURITY_CONTEXT.tenant;
        return `${t} | ${t}`;
      },
      { tenant: 'acme' }
    );

    expect(result.template).toBe('{sv:0} | {sv:1}');
    expect(result.args.security_context.values).toEqual(['acme', 'acme']);
  });

  it('supports the canonical array-filter callback pattern with groups.join(...)', () => {
    // Canonical prod pattern. The callback receives the prepared
    // placeholder strings; join glues them into the SQL fragment.
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.cubeCloud.groups.filter(
        (groups: string[]) => `source IN (${groups.join(',')})`
      )}`,
      { cubeCloud: { groups: ['a', 'b'] } }
    );

    expect(result.template).toBe('source IN ({sv:0},{sv:1})');
    expect(result.args.security_context.values).toEqual(['a', 'b']);
  });

  it('accepts both camelCase securityContext and snake_case security_context arg names', () => {
    const camel = compileMemberSql(
      (securityContext: any) => `${securityContext.tenant.filter('col')}`,
      { tenant: 'acme' }
    );
    const snake = compileMemberSql(
      // eslint-disable-next-line camelcase
      (security_context: any) => `${security_context.tenant.filter('col')}`,
      { tenant: 'acme' }
    );

    expect(camel.template).toBe('col = {sv:0}');
    expect(snake.template).toBe('col = {sv:0}');
  });
});
