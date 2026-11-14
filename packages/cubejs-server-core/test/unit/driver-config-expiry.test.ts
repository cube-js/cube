import { CreateOptions, CubejsServerCore } from '../../src';
import { parseDriverExpiry, withoutDriverExpiry } from '../../src/core/driver-config-expiry';

describe('parseDriverExpiry', () => {
  const iso = '2026-08-17T22:29:31.136Z';
  const epochMs = Date.parse(iso);

  test('reads an ISO 8601 string, which is how a credential usually carries it', () => {
    expect(parseDriverExpiry(iso)).toBe(epochMs);
    expect(parseDriverExpiry('2026-08-17T22:29:31.136+00:00')).toBe(epochMs);
    expect(parseDriverExpiry(`  ${iso}  `)).toBe(epochMs);
  });

  test('reads a Date', () => {
    expect(parseDriverExpiry(new Date(epochMs))).toBe(epochMs);
  });

  test('reads epoch milliseconds and epoch seconds alike', () => {
    expect(parseDriverExpiry(epochMs)).toBe(epochMs);
    // What `time.time() + 3600` in a Python config produces. Read as seconds it
    // is 2026; read as milliseconds it would be 1970, and every driver built
    // from it would be born expired.
    expect(parseDriverExpiry(Math.floor(epochMs / 1000))).toBe(Math.floor(epochMs / 1000) * 1000);
  });

  test('reads a stringified timestamp, which Date.parse would take for a year', () => {
    expect(parseDriverExpiry(String(epochMs))).toBe(epochMs);
    expect(parseDriverExpiry(String(Math.floor(epochMs / 1000)))).toBe(
      Math.floor(epochMs / 1000) * 1000,
    );
  });

  test('drops anything that is not a moment in time', () => {
    // Dropped rather than thrown: the lifetime is an optimisation over
    // comparing configurations, and failing a deployment's queries over a
    // malformed one would be worse than the behaviour it had before the field
    // existed.
    expect(parseDriverExpiry(undefined)).toBeUndefined();
    expect(parseDriverExpiry(null)).toBeUndefined();
    expect(parseDriverExpiry('')).toBeUndefined();
    expect(parseDriverExpiry('   ')).toBeUndefined();
    expect(parseDriverExpiry('whenever')).toBeUndefined();
    expect(parseDriverExpiry(new Date('nonsense'))).toBeUndefined();
    expect(parseDriverExpiry(0)).toBeUndefined();
    expect(parseDriverExpiry(-1)).toBeUndefined();
    expect(parseDriverExpiry(NaN)).toBeUndefined();
    expect(parseDriverExpiry(Infinity)).toBeUndefined();
    expect(parseDriverExpiry(true)).toBeUndefined();
    expect(parseDriverExpiry({ expiresAt: iso })).toBeUndefined();
    expect(parseDriverExpiry([iso])).toBeUndefined();
  });
});

describe('withoutDriverExpiry', () => {
  test('strips the lifetime without mutating the input', () => {
    const config = <any>{ type: 'postgres', password: 'secret', expiresAt: 1 };

    expect(withoutDriverExpiry(config)).toEqual({ type: 'postgres', password: 'secret' });
    expect(config.expiresAt).toBe(1);
  });

  test('returns the same object when there is nothing to strip', () => {
    const config = <any>{ type: 'postgres' };

    expect(withoutDriverExpiry(config)).toBe(config);
  });
});

/**
 * Every key of a `DriverConfig` other than `type` is passed to the driver's own
 * constructor, so a lifetime left in would arrive as a connection option — and,
 * for a configuration that names nothing else, would also displace the pool
 * defaults that an otherwise-empty configuration is meant to get.
 */
describe('driver construction', () => {
  class ExposedCore extends CubejsServerCore {
    public build(val: any, context: any) {
      return this.createDriverFromFactoryResult(val, context);
    }
  }

  let created: jest.SpyInstance;

  beforeAll(() => {
    process.env.CUBEJS_API_SECRET = 'api-secret';
  });

  beforeEach(() => {
    created = jest.spyOn(<any>CubejsServerCore, 'createDriver').mockReturnValue(<any>{});
  });

  afterEach(() => {
    created.mockRestore();
  });

  function core() {
    return new ExposedCore(<CreateOptions>{
      driverFactory: () => (<any>{ type: 'postgres' }),
      logger: jest.fn(),
    });
  }

  test('does not pass the lifetime to the driver', async () => {
    await core().build(
      { type: 'postgres', password: 'secret', expiresAt: '2026-08-17T22:29:31.136Z' },
      { dataSource: 'default' },
    );

    expect(created).toHaveBeenCalledWith('postgres', expect.not.objectContaining({
      expiresAt: expect.anything(),
    }));
    expect(created).toHaveBeenCalledWith('postgres', expect.objectContaining({
      password: 'secret',
      dataSource: 'default',
    }));
  });

  test('a configuration carrying only a lifetime still gets the pool defaults', async () => {
    await core().build(
      { type: 'postgres', expiresAt: '2026-08-17T22:29:31.136Z' },
      { dataSource: 'default' },
    );

    const [, opts] = created.mock.calls[0];

    expect(opts).not.toHaveProperty('expiresAt');
    expect(opts).toHaveProperty('maxPoolSize');
  });
});
