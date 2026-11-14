import { memoizeByObject } from '../src';

describe('memoizeByObject', () => {
  test('calls the function once per object', () => {
    const fn = jest.fn((context: { id: string }) => `app_${context.id}`);
    const memoized = memoizeByObject(fn);
    const first = { id: 'a' };
    const second = { id: 'a' };

    expect(memoized(first)).toBe('app_a');
    expect(memoized(first)).toBe('app_a');
    expect(memoized(second)).toBe('app_a');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  test('shares one pending promise between concurrent calls', async () => {
    const fn = jest.fn(async () => ['basic']);
    const memoized = memoizeByObject(fn);
    const context = {};

    const [a, b] = await Promise.all([memoized(context), memoized(context)]);

    expect(a).toBe(b);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  test('runs again after a rejection', async () => {
    const fn = jest.fn()
      .mockRejectedValueOnce(new Error('unavailable'))
      .mockResolvedValueOnce('app_a');
    const memoized = memoizeByObject(fn);
    const context = {};

    await expect(memoized(context)).rejects.toThrow('unavailable');
    await expect(memoized(context)).resolves.toBe('app_a');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  test('does not cache a synchronous throw', () => {
    const fn = jest.fn()
      .mockImplementationOnce(() => {
        throw new Error('bad context');
      })
      .mockImplementationOnce(() => 'app_a');
    const memoized = memoizeByObject(fn);
    const context = {};

    expect(() => memoized(context)).toThrow('bad context');
    expect(memoized(context)).toBe('app_a');
  });

  test('does not cache calls without an object', () => {
    const fn = jest.fn((context?: object) => (context ? 'app' : 'default'));
    const memoized = memoizeByObject(fn);

    memoized(undefined);
    memoized(undefined);

    expect(fn).toHaveBeenCalledTimes(2);
  });
});
