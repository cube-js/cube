import crypto from 'crypto';

type HashModule = typeof import('../src/hash');

// The algorithm is resolved once per process, so each case loads a fresh copy of the module.
function loadHashModule(): HashModule {
  let hashModule: HashModule | undefined;

  jest.isolateModules(() => {
    // eslint-disable-next-line global-require
    hashModule = require('../src/hash');
  });

  return hashModule!;
}

describe('createKeyHash', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('uses MD5 where the runtime provides it, so existing keys stay the same', () => {
    const { createKeyHash, getKeyHashAlgorithm } = loadHashModule();

    expect(getKeyHashAlgorithm()).toBe('md5');
    // MD5 of "cube"
    expect(createKeyHash().update('cube').digest('hex')).toBe('18a64f7251cfb123aa7c358135d00d10');
  });

  test('falls back to SHA-256 where MD5 is disabled (FIPS)', () => {
    const createHash = crypto.createHash.bind(crypto);

    jest.spyOn(crypto, 'createHash').mockImplementation((algorithm, options) => {
      if (algorithm === 'md5') {
        throw new Error('error:0308010C:digital envelope routines::unsupported');
      }

      return createHash(algorithm, options);
    });

    const { createKeyHash, getKeyHashAlgorithm } = loadHashModule();

    expect(getKeyHashAlgorithm()).toBe('sha256');
    // SHA-256 of "cube"
    expect(createKeyHash().update('cube').digest('hex')).toBe(
      '4f3c4172a4fe308cebc840da665171f534849b1fca101eaf635d68fb453393db'
    );
  });

  test('probes MD5 once', () => {
    const spy = jest.spyOn(crypto, 'createHash');
    const { createKeyHash } = loadHashModule();

    createKeyHash();
    createKeyHash();

    // 1 probe + 2 hashes
    expect(spy.mock.calls.filter(([algorithm]) => algorithm === 'md5')).toHaveLength(3);
  });
});
