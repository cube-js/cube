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

  it('coerces a numeric return to its string form', () => {
    const integerResult = compileMemberSql(() => 42 as any);
    const decimalResult = compileMemberSql(() => 1.5 as any);

    expect(integerResult.template).toBe('42');
    expect(decimalResult.template).toBe('1.5');
  });

  it('coerces a boolean return to its string form', () => {
    const trueResult = compileMemberSql(() => true as any);
    const falseResult = compileMemberSql(() => false as any);

    expect(trueResult.template).toBe('true');
    expect(falseResult.template).toBe('false');
  });

  it('returns an empty string template when the user function returns null', () => {
    const result = compileMemberSql(() => null as any);

    expect(result.template).toBe('');
    expect(result.args.symbol_paths).toEqual([]);
  });
});
