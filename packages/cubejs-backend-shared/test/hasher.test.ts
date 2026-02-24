type Hasher = {
  update(data: string | Buffer): Hasher;
  digest(encoding: 'hex'): string;
  digest(): Buffer;
};

type DefaultHasher = () => Hasher;

const ORIGINAL_ALGORITHM = process.env.CUBEJS_HASHER_ALGORITHM;

function setAlgorithm(algorithm?: string) {
  if (algorithm === undefined) {
    delete process.env.CUBEJS_HASHER_ALGORITHM;
  } else {
    process.env.CUBEJS_HASHER_ALGORITHM = algorithm;
  }
}

function loadDefaultHasher(): DefaultHasher {
  jest.resetModules();
  const mod = require('../src') as { defaultHasher: DefaultHasher };
  return mod.defaultHasher;
}

function hashHex(input: string | Buffer, algorithm?: string): string {
  setAlgorithm(algorithm);
  const defaultHasher = loadDefaultHasher();
  return defaultHasher().update(input).digest('hex');
}

function hashBuffer(input: string | Buffer, algorithm?: string): Buffer {
  setAlgorithm(algorithm);
  const defaultHasher = loadDefaultHasher();
  return defaultHasher().update(input).digest();
}

afterAll(() => {
  if (ORIGINAL_ALGORITHM === undefined) {
    delete process.env.CUBEJS_HASHER_ALGORITHM;
  } else {
    process.env.CUBEJS_HASHER_ALGORITHM = ORIGINAL_ALGORITHM;
  }
  jest.resetModules();
});

describe('defaultHasher', () => {
  test('should create a hasher instance', () => {
    setAlgorithm();
    const defaultHasher = loadDefaultHasher();
    const hasher = defaultHasher();

    expect(hasher).toBeDefined();
    expect(typeof hasher.update).toBe('function');
    expect(typeof hasher.digest).toBe('function');
  });

  test('should return consistent hex hash for the same input', () => {
    const input = 'test data';
    const hash1 = hashHex(input);
    const hash2 = hashHex(input);

    expect(hash1).toBe(hash2);
    expect(typeof hash1).toBe('string');
    expect(hash1.length).toBeGreaterThan(0);
  });

  test('should return different hashes for different inputs', () => {
    const hash1 = hashHex('input1');
    const hash2 = hashHex('input2');

    expect(hash1).not.toBe(hash2);
  });

  test('should support chaining update calls', () => {
    setAlgorithm();
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('part1')
      .update('part2')
      .digest('hex');

    setAlgorithm();
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update('part1part2')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer inputs', () => {
    const hash = hashHex(Buffer.from('test data'));

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should return Buffer when digest is called without encoding', () => {
    const hash = hashBuffer('test');

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16);
  });

  test('should handle JSON stringified objects', () => {
    const obj = { key: 'value', nested: { prop: 123 } };
    const hash1 = hashHex(JSON.stringify(obj));
    const hash2 = hashHex(JSON.stringify(obj));

    expect(hash1).toBe(hash2);
  });

  test('should handle empty strings', () => {
    const hash = hashHex('');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle large inputs', () => {
    const hash = hashHex('x'.repeat(10000));

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle unicode characters', () => {
    const hash = hashHex('你好世界 🌍 مرحبا');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should produce consistent hashes for mixed string and Buffer updates', () => {
    setAlgorithm();
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    setAlgorithm();
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });
});

describe('Hasher interface compatibility', () => {
  test('should be compatible with crypto.createHash API pattern', () => {
    const hash = hashHex(JSON.stringify({ test: 'data' }));

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should support digest() without encoding for Buffer result', () => {
    const digestBuffer = hashBuffer(JSON.stringify({ test: 'data' }));

    expect(Buffer.isBuffer(digestBuffer)).toBe(true);
    expect(digestBuffer.length).toBe(16);
  });

  test('should handle the version() function pattern from PreAggregations', () => {
    const digestBuffer = hashBuffer(JSON.stringify(['2024', '01', 'users']));

    expect(Buffer.isBuffer(digestBuffer)).toBe(true);

    const firstByte = digestBuffer.readUInt8(0);
    expect(typeof firstByte).toBe('number');
    expect(firstByte).toBeGreaterThanOrEqual(0);
    expect(firstByte).toBeLessThanOrEqual(255);
  });
});

describe('Hash consistency across different data types', () => {
  test('string vs Buffer with same content should produce same hash', () => {
    const str = 'test content';
    const hashFromString = hashHex(str);
    const hashFromBuffer = hashHex(Buffer.from(str));

    expect(hashFromString).toBe(hashFromBuffer);
  });

  test('Buffer digest should be consistent', () => {
    const digest1 = hashBuffer('consistent test');
    const digest2 = hashBuffer('consistent test');

    expect(digest1.equals(digest2)).toBe(true);
  });
});

describe('MD5 hasher (default)', () => {
  test('should use MD5 by default', () => {
    const hash = hashHex('test data');
    expect(hash).toBe('eb733a00c0c9d336e65691a37ab54293');
  });

  test('should return 16-byte Buffer for MD5 digest', () => {
    const hash = hashBuffer('test');

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16);
  });

  test('should handle chaining with MD5', () => {
    setAlgorithm();
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    setAlgorithm();
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with MD5', () => {
    const hash = hashHex(Buffer.from('test buffer'));

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(32);
  });
});

describe('xxHash implementation', () => {
  test('should use xxHash when CUBEJS_HASHER_ALGORITHM=xxhash', () => {
    const hash = hashHex('test data', 'xxhash');

    expect(hash).not.toBe('eb733a00c0c9d336e65691a37ab54293');
    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should return 16-byte Buffer for xxHash digest', () => {
    const hash = hashBuffer('test', 'xxhash');

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(16);
  });

  test('should be consistent with xxHash', () => {
    const hash1 = hashHex('consistency test', 'xxhash');
    const hash2 = hashHex('consistency test', 'xxhash');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with xxHash', () => {
    setAlgorithm('xxhash');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    setAlgorithm('xxhash');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle string input with xxHash', () => {
    const hash = hashHex('test string', 'xxhash');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle Buffer input with xxHash', () => {
    const hash = hashHex(Buffer.from('test buffer'), 'xxhash');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle mixed string and Buffer updates with xxHash', () => {
    setAlgorithm('xxhash');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    setAlgorithm('xxhash');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should return Buffer digest with xxHash', () => {
    const digest = hashBuffer('test', 'xxhash');

    expect(Buffer.isBuffer(digest)).toBe(true);
    expect(digest.length).toBe(16);

    const firstByte = digest.readUInt8(0);
    expect(typeof firstByte).toBe('number');
  });

  test('should handle empty strings with xxHash', () => {
    const hash = hashHex('', 'xxhash');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    const upperCaseHash = hashHex('test', 'XXHASH');
    const mixedCaseHash = hashHex('test', 'XxHash');
    const lowerCaseHash = hashHex('test', 'xxhash');

    expect(upperCaseHash).toBe(lowerCaseHash);
    expect(mixedCaseHash).toBe(lowerCaseHash);
  });
});

describe('Feature flag behavior', () => {
  test('should default to MD5 when env var is not set', () => {
    const hash = hashHex('test');
    expect(hash).toBe('098f6bcd4621d373cade4e832627b4f6');
  });

  test('should default to MD5 when env var is empty string', () => {
    const hash = hashHex('test', '');
    expect(hash).toBe('098f6bcd4621d373cade4e832627b4f6');
  });

  test('should throw error for unknown algorithm', () => {
    setAlgorithm('blake2b');
    expect(() => loadDefaultHasher()).toThrow(/not valid/i);
  });

  test('MD5 and xxHash should produce different results', () => {
    const md5Hash = hashHex('test');
    const xxHash = hashHex('test', 'xxhash');

    expect(md5Hash).not.toBe(xxHash);
  });
});

describe('SHA256 hasher', () => {
  test('should use SHA256 when CUBEJS_HASHER_ALGORITHM=sha256', () => {
    const hash = hashHex('test', 'sha256');
    expect(hash).toBe('9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08');
  });

  test('should return 32-byte Buffer for SHA256 digest', () => {
    const hash = hashBuffer('test', 'sha256');

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(32);
  });

  test('should be consistent with SHA256', () => {
    const hash1 = hashHex('consistency test', 'sha256');
    const hash2 = hashHex('consistency test', 'sha256');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with SHA256', () => {
    setAlgorithm('sha256');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    setAlgorithm('sha256');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with SHA256', () => {
    const hash = hashHex(Buffer.from('test buffer'), 'sha256');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(64);
  });

  test('should handle empty strings with SHA256', () => {
    const hash = hashHex('', 'sha256');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    const hash = hashHex('test', 'SHA256');
    expect(hash).toBe('9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08');
  });

  test('should handle mixed string and Buffer updates with SHA256', () => {
    setAlgorithm('sha256');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    setAlgorithm('sha256');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });
});

describe('SHA512 hasher', () => {
  test('should use SHA512 when CUBEJS_HASHER_ALGORITHM=sha512', () => {
    const hash = hashHex('test', 'sha512');
    expect(hash).toBe('ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff');
  });

  test('should return 64-byte Buffer for SHA512 digest', () => {
    const hash = hashBuffer('test', 'sha512');

    expect(Buffer.isBuffer(hash)).toBe(true);
    expect(hash.length).toBe(64);
  });

  test('should be consistent with SHA512', () => {
    const hash1 = hashHex('consistency test', 'sha512');
    const hash2 = hashHex('consistency test', 'sha512');

    expect(hash1).toBe(hash2);
  });

  test('should handle chaining with SHA512', () => {
    setAlgorithm('sha512');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(' ')
      .update('world')
      .digest('hex');

    setAlgorithm('sha512');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update('hello world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle Buffer input with SHA512', () => {
    const hash = hashHex(Buffer.from('test buffer'), 'sha512');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
    expect(hash.length).toBe(128);
  });

  test('should handle empty strings with SHA512', () => {
    const hash = hashHex('', 'sha512');

    expect(hash).toBeDefined();
    expect(typeof hash).toBe('string');
  });

  test('should handle case-insensitive algorithm name', () => {
    const hash = hashHex('test', 'SHA512');
    expect(hash).toBe('ee26b0dd4af7e749aa1a8ee3c10ae9923f618980772e473f8819a5d4940e0db27ac185f8a0e1d5f84f88bc887fd67b143732c304cc5fa9ad8e6f57f50028a8ff');
  });

  test('should handle mixed string and Buffer updates with SHA512', () => {
    setAlgorithm('sha512');
    const defaultHasher = loadDefaultHasher();
    const hash1 = defaultHasher()
      .update('hello')
      .update(Buffer.from('world'))
      .digest('hex');

    setAlgorithm('sha512');
    const secondDefaultHasher = loadDefaultHasher();
    const hash2 = secondDefaultHasher()
      .update(Buffer.from('hello'))
      .update('world')
      .digest('hex');

    expect(hash1).toBe(hash2);
  });

  test('should handle JSON stringified objects with SHA512', () => {
    const obj = { key: 'value', nested: { prop: 123 } };
    const hash1 = hashHex(JSON.stringify(obj), 'sha512');
    const hash2 = hashHex(JSON.stringify(obj), 'sha512');

    expect(hash1).toBe(hash2);
  });
});

describe('Algorithm comparison', () => {
  test('all algorithms should produce different results', () => {
    const md5Hash = hashHex('test');
    const sha256Hash = hashHex('test', 'sha256');
    const sha512Hash = hashHex('test', 'sha512');
    const xxHash = hashHex('test', 'xxhash');

    expect(md5Hash).not.toBe(sha256Hash);
    expect(md5Hash).not.toBe(sha512Hash);
    expect(md5Hash).not.toBe(xxHash);
    expect(sha256Hash).not.toBe(sha512Hash);
    expect(sha256Hash).not.toBe(xxHash);
    expect(sha512Hash).not.toBe(xxHash);
  });

  test('different algorithms produce different buffer lengths', () => {
    const md5Buffer = hashBuffer('test');
    const sha256Buffer = hashBuffer('test', 'sha256');
    const sha512Buffer = hashBuffer('test', 'sha512');
    const xxHashBuffer = hashBuffer('test', 'xxhash');

    expect(md5Buffer.length).toBe(16);
    expect(sha256Buffer.length).toBe(32);
    expect(sha512Buffer.length).toBe(64);
    expect(xxHashBuffer.length).toBe(16);
  });
});
