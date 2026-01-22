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
