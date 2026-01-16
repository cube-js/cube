import { xxh3 } from '@node-rs/xxhash';

export interface Hasher {
  /**
   * @param data - The data to hash (string or Buffer)
   */
  update(data: string | Buffer): this;

  /**
   * @returns The hash value in hex format
   */
  digest(encoding: 'hex'): string;

  /**
   * @returns The hash value as a Buffer
   */
  digest(): Buffer;
}

class XxHasher implements Hasher {
  private data: Buffer[] = [];

  public update(data: string | Buffer): this {
    if (typeof data === 'string') {
      this.data.push(Buffer.from(data));
    } else {
      this.data.push(data);
    }
    return this;
  }

  public digest(): Buffer;

  public digest(encoding: 'hex'): string;

  public digest(encoding?: 'hex'): Buffer | string {
    const combined = Buffer.concat(this.data);
    const hash = xxh3.xxh128(combined);

    if (encoding === 'hex') {
      return hash.toString(16);
    }

    /*
     * This ensures the Buffer format matches what the old MD5 implementation
     * would have returned, maintaining compatibility with code that reads the
     * digest as a Buffer.
     */
    const buffer = Buffer.alloc(16);
    const hashBigInt = BigInt(hash);
    // eslint-disable-next-line no-bitwise
    buffer.writeBigUInt64BE(hashBigInt >> BigInt(64), 0);
    // eslint-disable-next-line no-bitwise
    buffer.writeBigUInt64BE(hashBigInt & BigInt('0xFFFFFFFFFFFFFFFF'), 8);
    return buffer;
  }
}

/**
 * Creates a new default hasher instance.
 *
 * This follows Rust's DefaultHasher pattern and provides a consistent
 * hashing interface throughout the Cube.js codebase. The implementation
 * uses xxHash (xxh128) for fast, non-cryptographic hashing.
 *
 * The hasher can be used as a drop-in replacement for crypto.createHash()
 * in non-cryptographic contexts.
 *
 * @example
 * ```typescript
 * const hash = defaultHasher().update('data').digest('hex');
 * ```
 *
 * @example
 * ```typescript
 * const buffer = defaultHasher().update(JSON.stringify(obj)).digest();
 * ```
 *
 * @returns A new Hasher instance
 */
export function defaultHasher(): Hasher {
  // Future: could check environment variable here to switch implementations
  // e.g., process.env.CUBEJS_HASHER_ALGORITHM
  return new XxHasher();
}
