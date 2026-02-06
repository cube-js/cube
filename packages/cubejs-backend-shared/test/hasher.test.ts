import { defaultHasher } from '../src';

describe('defaultHasher', () => {
  test('should create a hasher instance', () => {
    const hasher = defaultHasher();
    expect(hasher).toBeDefined();
    expect(typeof hasher.update).toBe('function');
    expect(typeof hasher.digest).toBe('function');
  });

  test('should return consistent hex hash for the same input', () => {
    const input = 'test data';
    const hash1 = defaultHasher().update(input).digest('hex');
    const hash2 = defaultHasher().update(input).digest('hex');

    expect(hash1).toBe(hash2);
    expect(typeof hash1).toBe('string');
    expect(hash1.length).toBeGreaterThan(0);
  });

  test('should return different hashes for different inputs', () => {
    const hash1 = defaultHasher().update('input1').digest('hex');
    const hash2 = defaultHasher().update('input2').digest('hex');

    expect(hash1).not.toBe(hash2);
  });

  test('should support chaining update calls', () => {
    const hash1 = defaultHasher()
      .update('part1')
      .update('part2')
      .digest('hex');

    const hash2 = defaultHasher()
      .update('part1part2')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer inputs', () => {
    const buffer = Buffer.from('test data');
    const hash = defaultHasher().update(buffer).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should return Buffer when digest is called without encoding', () => {
    const hash = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16); // 128 bits = 16 bytes
  });

  test('should handle JSON stringified objects', () => {
    const obj = { key: 'value', nested: { prop: 123 } };
    const hash1 = defaultHasher().update(JSON.stringify(obj)).digest('hex');
    const hash2 = defaultHasher().update(JSON.stringify(obj)).digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle empty strings', () => {
    const hash = defaultHasher().update('').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle large inputs', () => {
    const largeString = 'x'.repeat(10000);
    const hash = defaultHasher().update(largeString).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle unicode characters', () => {
    const unicode = '你好世界 🌍 مرحبا';
    const hash = defaultHasher().update(unicode).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should produce consistent hashes for mixed string and Buffer updates', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    const hash2 = defaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });
});

describe('Hasher interface compatibility', () => {
  test('should be compatible with crypto.createHash API pattern', () => {
    // This tests that the API matches the pattern used to replace crypto.createHash('md5')
    const data = JSON.stringify({ test: 'data' });

    // Old pattern: crypto.createHash('md5').update(data).digest('hex')
    // New pattern: defaultHasher().update(data).digest('hex')
    const hash = defaultHasher().update(data).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should support digest() without encoding for Buffer result', () => {
    // Old pattern: crypto.createHash('md5').update(data).digest()
    // New pattern: defaultHasher().update(data).digest()
    const data = JSON.stringify({ test: 'data' });
    const digestBuffer = defaultHasher().update(data).digest();

    expect(Buffer.isBuffer(digestBuffer)).toBe(true);
    expect(digestBuffer.length).toBe(16);
  });

  test('should handle the version() function pattern from PreAggregations', () => {
    // Testing the pattern: defaultHasher().update(JSON.stringify(cacheKey)).digest()
    const cacheKey = ['2024', '01', 'users'];
    const digestBuffer = defaultHasher().update(JSON.stringify(cacheKey)).digest();

    expect(Buffer.isBuffer(digestBuffer)).toBe(true);

    // Should be able to read bytes from the buffer like the old code did
    const firstByte = digestBuffer.readUInt8(0);
    expect(typeof firstByte).toBe('number');
    expect(firstByte).toBeGreaterThanOrEqual(0);
    expect(firstByte).toBeLessThanOrEqual(255);
  });
});

describe('Hash consistency across different data types', () => {
  test('string vs Buffer with same content should produce same hash', () => {
    const str = 'test content';
    const buf = Buffer.from(str);

    const hashFromString = defaultHasher().update(str).digest('hex');
    const hashFromBuffer = defaultHasher().update(buf).digest('hex');

    expect(hashFromString).toBe(hashFromBuffer);
  });

  test('Buffer digest should be consistent', () => {
    const input = 'consistent test';
    const digest1 = defaultHasher().update(input).digest();
    const digest2 = defaultHasher().update(input).digest();

    expect(digest1.equals(digest2)).toBe(true);
  });
});
describe('MD5 hasher (default)', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  beforeEach(() => {
    delete process.env.CUBEJS_HASHER_ALGORITHM;
  });

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('should use MD5 by default', () => {
    const input = 'test data';
    const hash = defaultHasher().update(input).digest('hex');

    // Known MD5 hash for 'test data'
    expect(hash).toBe('eb733a00c0c9d336e65691a37ab54293');
  });

  test('should return 16-byte Buffer for MD5 digest', () => {
    const hash = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16);
  });

  test('should handle chaining with MD5', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    const hash2 = defaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with MD5', () => {
    const buffer = Buffer.from('test buffer');
    const hash = defaultHasher().update(buffer).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(32); // MD5 hex is 32 characters
  });
});

describe('xxHash implementation', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  beforeEach(() => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'xxhash';
  });

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('should use xxHash when CUBEJS_HASHER_ALGORITHM=xxhash', () => {
    const input = 'test data';
    const hash = defaultHasher().update(input).digest('hex');

    // xxHash will produce a different hash than MD5
    expect(hash).not.toBe('eb733a00c0c9d336e65691a37ab54293');
    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should return 16-byte Buffer for xxHash digest', () => {
    const hash = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16);
  });

  test('should be consistent with xxHash', () => {
    const input = 'consistency test';
    const hash1 = defaultHasher().update(input).digest('hex');
    const hash2 = defaultHasher().update(input).digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with xxHash', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    const hash2 = defaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle string input with xxHash', () => {
    const str = 'test string';
    const hash = defaultHasher().update(str).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle Buffer input with xxHash', () => {
    const buffer = Buffer.from('test buffer');
    const hash = defaultHasher().update(buffer).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle mixed string and Buffer updates with xxHash', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    const hash2 = defaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should return Buffer digest with xxHash', () => {
    const digest = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(digest)).toBe(true);
    expect(digest.length).toBe(16);

    // Verify it can be read as bytes
    const firstByte = digest.readUInt8(0);
    expect(typeof firstByte).toBe('number');
  });

  test('should handle empty strings with xxHash', () => {
    const hash = defaultHasher().update('').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'XXHASH';
    const hash = defaultHasher().update('test').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle XxHash algorithm name', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'XxHash';
    const hash = defaultHasher().update('test').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });
});

describe('Feature flag behavior', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('should default to MD5 when env var is not set', () => {
    delete process.env.CUBEJS_HASHER_ALGORITHM;
    const hash = defaultHasher().update('test').digest('hex');

    // MD5 hash of 'test'
    expect(hash).toBe('098f6bcd4621d373cade4e832627b4f6');
  });

  test('should default to MD5 when env var is empty string', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = '';
    const hash = defaultHasher().update('test').digest('hex');

    // MD5 hash of 'test' (empty string falls back to default 'md5')
    expect(hash).toBe('098f6bcd4621d373cade4e832627b4f6');
  });

  test('should throw error for unknown algorithm', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'blake2b';

    expect(() => defaultHasher()).toThrow('Value "blake2b" is not valid for CUBEJS_HASHER_ALGORITHM');
  });

  test('MD5 and xxHash should produce different results', () => {
    delete process.env.CUBEJS_HASHER_ALGORITHM;
    const md5Hash = defaultHasher().update('test').digest('hex');

    process.env.CUBEJS_HASHER_ALGORITHM = 'xxhash';
    const xxHash = defaultHasher().update('test').digest('hex');

    expect(md5Hash).not.toBe(xxHash);
  });
});

describe('SHA256 hasher', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  beforeEach(() => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'sha256';
  });

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('should use SHA256 when CUBEJS_HASHER_ALGORITHM=sha256', () => {
    const input = 'test';
    const hash = defaultHasher().update(input).digest('hex');

    // Known SHA256 hash for 'test'
    expect(hash).toBe('9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08');
  });

  test('should return 32-byte Buffer for SHA256 digest', () => {
    const hash = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(32); // SHA256 produces 32 bytes
  });

  test('should be consistent with SHA256', () => {
    const input = 'consistency test';
    const hash1 = defaultHasher().update(input).digest('hex');
    const hash2 = defaultHasher().update(input).digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with SHA256', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    const hash2 = defaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with SHA256', () => {
    const buffer = Buffer.from('test buffer');
    const hash = defaultHasher().update(buffer).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(64); // SHA256 hex is 64 characters
  });

  test('should handle empty strings with SHA256', () => {
    const hash = defaultHasher().update('').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'SHA256';
    const hash = defaultHasher().update('test').digest('hex');

    expect(hash).toBe('9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08');
  });

  test('should handle mixed string and Buffer updates with SHA256', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    const hash2 = defaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });
});

describe('SHA512 hasher', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  beforeEach(() => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'sha512';
  });

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('should use SHA512 when CUBEJS_HASHER_ALGORITHM=sha512', () => {
    const input = 'test';
    const hash = defaultHasher().update(input).digest('hex');

    // Known SHA512 hash for 'test'
    expect(hash).toBe('ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff');
  });

  test('should return 64-byte Buffer for SHA512 digest', () => {
    const hash = defaultHasher().update('test').digest();

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(64); // SHA512 produces 64 bytes
  });

  test('should be consistent with SHA512', () => {
    const input = 'consistency test';
    const hash1 = defaultHasher().update(input).digest('hex');
    const hash2 = defaultHasher().update(input).digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with SHA512', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    const hash2 = defaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with SHA512', () => {
    const buffer = Buffer.from('test buffer');
    const hash = defaultHasher().update(buffer).digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(128); // SHA512 hex is 128 characters
  });

  test('should handle empty strings with SHA512', () => {
    const hash = defaultHasher().update('').digest('hex');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    process.env.CUBEJS_HASHER_ALGORITHM = 'SHA512';
    const hash = defaultHasher().update('test').digest('hex');

    expect(hash).toBe('ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff');
  });

  test('should handle mixed string and Buffer updates with SHA512', () => {
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    const hash2 = defaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle JSON stringified objects with SHA512', () => {
    const obj = { key: 'value', nested: { prop: 123 } };
    const hash1 = defaultHasher().update(JSON.stringify(obj)).digest('hex');
    const hash2 = defaultHasher().update(JSON.stringify(obj)).digest('hex');

    expect(hash1).toBe(hash2);
  });
});

describe('Algorithm comparison', () => {
  const originalEnv = process.env.CUBEJS_HASHER_ALGORITHM;

  afterEach(() => {
    if (originalEnv !== undefined) {
      process.env.CUBEJS_HASHER_ALGORITHM = originalEnv;
    } else {
      delete process.env.CUBEJS_HASHER_ALGORITHM;
    }
  });

  test('all algorithms should produce different results', () => {
    const input = 'test';

    delete process.env.CUBEJS_HASHER_ALGORITHM;
    const md5Hash = defaultHasher().update(input).digest('hex');

    process.env.CUBEJS_HASHER_ALGORITHM = 'sha256';
    const sha256Hash = defaultHasher().update(input).digest('hex');

    process.env.CUBEJS_HASHER_ALGORITHM = 'sha512';
    const sha512Hash = defaultHasher().update(input).digest('hex');

    process.env.CUBEJS_HASHER_ALGORITHM = 'xxhash';
    const xxHash = defaultHasher().update(input).digest('hex');

    // All hashes should be different
    expect(md5Hash).not.toBe(sha256Hash);
    expect(md5Hash).not.toBe(sha512Hash);
    expect(md5Hash).not.toBe(xxHash);
    expect(sha256Hash).not.toBe(sha512Hash);
    expect(sha256Hash).not.toBe(xxHash);
    expect(sha512Hash).not.toBe(xxHash);
  });

  test('different algorithms produce different buffer lengths', () => {
    const input = 'test';

    delete process.env.CUBEJS_HASHER_ALGORITHM;
    const md5Buffer = defaultHasher().update(input).digest();

    process.env.CUBEJS_HASHER_ALGORITHM = 'sha256';
    const sha256Buffer = defaultHasher().update(input).digest();

    process.env.CUBEJS_HASHER_ALGORITHM = 'sha512';
    const sha512Buffer = defaultHasher().update(input).digest();

    process.env.CUBEJS_HASHER_ALGORITHM = 'xxhash';
    const xxHashBuffer = defaultHasher().update(input).digest();

    expect(md5Buffer.length).toBe(16);
    expect(sha256Buffer.length).toBe(32);
    expect(sha512Buffer.length).toBe(64);
    expect(xxHashBuffer.length).toBe(16);
  });
});
