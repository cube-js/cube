import crypto from 'crypto';

import { createKeyHash } from '../src/hash';

describe('createKeyHash', () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  test('uses MD5 outside FIPS mode, so existing keys stay the same', () => {
    jest.spyOn(crypto, 'getFips').mockReturnValue(0);

    // MD5 of "cube"
    expect(createKeyHash().update('cube').digest('hex')).toBe('18a64f7251cfb123aa7c358135d00d10');
  });

  test('uses SHA-256 in FIPS mode, where MD5 is not available', () => {
    jest.spyOn(crypto, 'getFips').mockReturnValue(1);
    const createHash = jest.spyOn(crypto, 'createHash');

    // SHA-256 of "cube"
    expect(createKeyHash().update('cube').digest('hex')).toBe(
      '4f3c4172a4fe308cebc840da665171f534849b1fca101eaf635d68fb453393db'
    );
    expect(createHash).not.toHaveBeenCalledWith('md5');
  });
});
