import {
  bridgeHarnessAvailable,
  invokeFilterParamsCallback,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: FilterParamsCallback', () => {
  // The bridge spreads the Vec<String> of filter values into positional
  // arguments of the JS callback (matching how typed_filter.rs invokes
  // it). A callback for an equality filter takes one arg; a between
  // filter takes two; a callback that wants the array form should use
  // rest params.

  it('passes a single placeholder as one positional arg', () => {
    const result = invokeFilterParamsCallback(
      (a: string) => `= ${a}`,
      ['{sv:0}']
    );

    expect(result).toBe('= {sv:0}');
  });

  it('spreads multiple placeholders as positional args', () => {
    const result = invokeFilterParamsCallback(
      (from: string, to: string) => `BETWEEN ${from} AND ${to}`,
      ['{sv:0}', '{sv:1}']
    );

    expect(result).toBe('BETWEEN {sv:0} AND {sv:1}');
  });

  it('makes the placeholder array reachable via rest params', () => {
    const result = invokeFilterParamsCallback(
      (...vals: string[]) => `IN (${vals.join(',')})`,
      ['{sv:0}', '{sv:1}', '{sv:2}']
    );

    expect(result).toBe('IN ({sv:0},{sv:1},{sv:2})');
  });

  it('passes no args when the filter values are empty', () => {
    const result = invokeFilterParamsCallback(
      (...rest: string[]) => `count=${rest.length}`,
      []
    );

    expect(result).toBe('count=0');
  });

  it('returns a user error when the callback returns a non-string', () => {
    expect(() => invokeFilterParamsCallback(() => 42 as any, [])).toThrow(/Callback for FILTER_PARAMS should return string/);
  });

  it('returns a user error when the callback returns undefined', () => {
    expect(() => invokeFilterParamsCallback(() => undefined as any, [])).toThrow(/Callback for FILTER_PARAMS should return string/);
  });

  it('propagates a JS-thrown error', () => {
    expect(() => invokeFilterParamsCallback(() => {
      throw new Error('boom');
    }, [])).toThrow(/boom/);
  });
});
