import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: SECURITY_CONTEXT — filter input shapes', () => {
  it('handles a string filter value as col = {sv:N}', () => {
    // Pin current Rust behavior. Eager double-registration is a known
    // divergence vs JS — see skipped 'JS-ref: .filter pushes value once'.
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      { tenant: 'acme' }
    );

    expect(result.template).toBe('col = {sv:1}');
    expect(result.args.security_context.values).toEqual(['acme', 'acme']);
  });

  it('handles a string array as col IN (sv0, sv1, ...)', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter('col')}`,
      { groups: ['a', 'b', 'c'] }
    );

    // Eager double-registration: 3 from leaf-proxy construction + 3 from
    // .filter call. See skipped 'JS-ref: .filter pushes value once'.
    expect(result.template).toBe('col IN ({sv:3}, {sv:4}, {sv:5})');
    expect(result.args.security_context.values).toEqual([
      'a',
      'b',
      'c',
      'a',
      'b',
      'c',
    ]);
  });

  it('renders an empty string array as 1 = 0 with no values registered for the filter', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter('col')}`,
      { groups: [] }
    );

    expect(result.template).toBe('1 = 0');
    // Leaf proxy still constructed; an empty array contributes no eager
    // registrations and the filter callback also produces none.
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

    expect(result.template).toBe('uid = {sv:1}');
    expect(result.args.security_context.values).toEqual(['42', '42']);
  });

  it('formats a non-integer number as a decimal string', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.factor.filter('f')}`,
      { factor: 1.5 }
    );

    expect(result.template).toBe('f = {sv:1}');
    expect(result.args.security_context.values).toEqual(['1.5', '1.5']);
  });

  it('formats a boolean as the string true/false', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.flag.filter('f')}`,
      { flag: true }
    );

    expect(result.template).toBe('f = {sv:1}');
    expect(result.args.security_context.values).toEqual(['true', 'true']);
  });

  it('returns 1 = 1 when an optional filter field is missing', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      {}
    );

    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('throws a user error when requiredFilter field is missing', () => {
    expect(() => compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.requiredFilter('col')}`,
      {}
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

    expect(result.template).toBe('col = {sv:1}');
    expect(result.args.security_context.values).toEqual(['123', '123']);
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
  });

  it('exposes unsafeValue() that returns the raw value without registering a placeholder', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `prefix-${SECURITY_CONTEXT.tenant.unsafeValue()}-suffix`,
      { tenant: 'acme' }
    );

    expect(result.template).toBe('prefix-acme-suffix');
    // Eager toString registration still pushes once; unsafeValue itself
    // does not register anything.
    expect(result.args.security_context.values).toEqual(['acme']);
  });

  it('lets the user branch the template at compile time via unsafeValue()', () => {
    // Real prod pattern: unsafeValue() returns the actual JS value, so a
    // ternary in the template literal picks one branch at compile time
    // and the resulting template is just the picked literal.
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
    // unsafeValue() itself does not push to values, BUT just accessing
    // .groups constructs a leaf proxy whose toString function eagerly
    // pushes the leaf value. So the bridge state still records the leaf
    // even though the rendered template never references {sv:N}.
    expect(adminResult.args.security_context.values).toEqual(['admin']);
    expect(viewerResult.args.security_context.values).toEqual(['viewer']);
  });

  it('renders a leaf used directly in a template (no filter call) without duplicating values', () => {
    // tenant_id = ${SECURITY_CONTEXT.cubeCloud.tenantId} — common in prod.
    // Here the eager to_string_fn baked in during leaf-proxy construction
    // returns the placeholder when JS coerces; nothing else pushes.
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `tenant_id = ${SECURITY_CONTEXT.cubeCloud.tenantId}`,
      { cubeCloud: { tenantId: '123' } }
    );

    expect(result.template).toBe('tenant_id = {sv:0}');
    expect(result.args.security_context.values).toEqual(['123']);
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

    // 2 from eager toString registration + 2 from .filter() — the last two
    // ({sv:2}, {sv:3}) are passed to the callback.
    expect(result.template).toBe('source IN ({sv:2},{sv:3})');
    expect(result.args.security_context.values).toEqual(['a', 'b', 'a', 'b']);
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

    expect(camel.template).toBe('col = {sv:1}');
    expect(snake.template).toBe('col = {sv:1}');
  });
});

// Each skipped test below asserts what the JS reference proxy
// (`contextSymbolsProxyFrom` in schema-compiler) does today. The Rust
// bridge diverges; unskip together with a fix.
describeBridge('bridge: SECURITY_CONTEXT — known divergences from JS reference', () => {
  // Bug: bridge treats falsy non-null values as real values and emits a
  // bind. JS short-circuits truthy on the param: false / 0 / '' return
  // "1 = 1". Rust cascades through String/f64/bool deserialization and
  // pushes the value.
  it.skip('JS-ref: .filter on false returns 1 = 1', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.flag.filter('f')}`,
      { flag: false }
    );
    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it.skip('JS-ref: .filter on 0 returns 1 = 1', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.user_id.filter('uid')}`,
      { user_id: 0 }
    );
    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it.skip('JS-ref: .filter on \'\' returns 1 = 1', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      { tenant: '' }
    );
    expect(result.template).toBe('1 = 1');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('JS-ref: .requiredFilter on 0 throws', () => {
    expect(() => compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.requiredFilter('col')}`,
      { tenant: 0 }
    )).toThrow(/Filter for col is required/);
  });

  it('JS-ref: .requiredFilter on false throws', () => {
    expect(() => compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.flag.requiredFilter('f')}`,
      { flag: false }
    )).toThrow(/Filter for f is required/);
  });

  it('JS-ref: .filter on number[] emits IN clause', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.ids.filter('id')}`,
      { ids: [1, 2, 3] }
    );
    expect(result.template).toMatch(
      /^id IN \(\{sv:\d+\}, \{sv:\d+\}, \{sv:\d+\}\)$/
    );
  });

  // Bug: leaf proxy eagerly allocates the value at construction time, so
  // .filter pushes a duplicate and {sv:N} starts at 1. JS allocates
  // lazily — each .filter call pushes exactly once. Rust pre-bakes the
  // toString output when the leaf proxy is built.
  it.skip('JS-ref: .filter pushes value once at {sv:0}', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.tenant.filter('col')}`,
      { tenant: 'acme' }
    );
    expect(result.template).toBe('col = {sv:0}');
    expect(result.args.security_context.values).toEqual(['acme']);
  });

  it.skip('JS-ref: .filter on string[] uses indices 0..N once', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups.filter('col')}`,
      { groups: ['a', 'b'] }
    );
    expect(result.template).toBe('col IN ({sv:0}, {sv:1})');
    expect(result.args.security_context.values).toEqual(['a', 'b']);
  });

  it.skip('JS-ref: unsafeValue() does not register a placeholder', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `prefix-${SECURITY_CONTEXT.tenant.unsafeValue()}-suffix`,
      { tenant: 'acme' }
    );
    expect(result.template).toBe('prefix-acme-suffix');
    expect(result.args.security_context.values).toEqual([]);
  });

  it('JS-ref: array toString joins without a space', () => {
    const result = compileMemberSql(
      (SECURITY_CONTEXT: any) => `${SECURITY_CONTEXT.groups}`,
      { groups: ['a', 'b'] }
    );
    expect(result.template).toBe('{sv:0},{sv:1}');
  });
});
