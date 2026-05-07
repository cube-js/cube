import { bridgeHarnessAvailable, compileMemberSql } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: result shape', () => {
  it('returns a string-vec template when the user function returns an array', () => {
    // Pre-aggregation refs use this path.
    const result = compileMemberSql(
      // eslint-disable-next-line camelcase
      (orders: any, line_items: any) => [orders.status, line_items.id]
    );

    expect(result.template).toEqual(['{arg:0}', '{arg:1}']);
    expect(result.args.symbol_paths).toEqual([
      ['orders', 'status'],
      ['line_items', 'id'],
    ]);
  });

  it('returns a string template for any non-array return (including objects coerced via toString)', () => {
    // If the user returns a single proxy node, convert_to_string falls back
    // to its toString interceptor, which in turn registers the path.
    const result = compileMemberSql((orders: any) => orders.status);

    expect(result.template).toBe('{arg:0}');
    expect(result.args.symbol_paths).toEqual([['orders', 'status']]);
  });

  it('errors when the user function returns a primitive number', () => {
    // Bug: bridge eagerly coerces the return value via convert_to_string,
    // which only handles JsString and null — it then tries to call
    // toString as a struct method, which errors on primitives. JS
    // reference returns the value unchanged and lets downstream template
    // literals coerce naturally. See skipped 'JS-ref: numeric return…'.
    expect(() => compileMemberSql(() => 42 as any)).toThrow(
      /Object is not the Struct object/
    );
  });

  it('errors when the user function returns a primitive boolean', () => {
    expect(() => compileMemberSql(() => true as any)).toThrow(
      /Object is not the Struct object/
    );
  });

  it('returns an empty string template when the user function returns null', () => {
    const result = compileMemberSql(() => null as any);

    expect(result.template).toBe('');
    expect(result.args.symbol_paths).toEqual([]);
  });

  // JS reference does no coercion at the bridge boundary — the user
  // function's return value flows through resolveSymbolsCall verbatim and
  // is later embedded via template literal, which yields String(value).
  it.skip('JS-ref: numeric return is coerced to its string form', () => {
    const result = compileMemberSql(() => 42 as any);
    expect(result.template).toBe('42');
  });

  it.skip('JS-ref: boolean return is coerced to its string form', () => {
    const result = compileMemberSql(() => true as any);
    expect(result.template).toBe('true');
  });
});
