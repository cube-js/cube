import crypto from 'crypto';
import { xxh3 } from '@node-rs/xxhash';
import { getEnv } from './env';

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

class Md5Hasher implements Hasher {
  private hash: crypto.Hash;

  public constructor() {
    this.hash = crypto.createHash('md5');
  }

  public update(data: string | Buffer): this {
    this.hash.update(data);
    return this;
  }

  public digest(): Buffer;

  public digest(encoding: 'hex'): string;

  public digest(encoding?: 'hex'): Buffer | string {
    if (encoding === 'hex') {
      return this.hash.digest('hex');
    }
    return this.hash.digest();
  }
}

class Sha256Hasher implements Hasher {
  private hash: crypto.Hash;

  public constructor() {
    this.hash = crypto.createHash('sha256');
  }

  public update(data: string | Buffer): this {
    this.hash.update(data);
    return this;
  }

  public digest(): Buffer;

  public digest(encoding: 'hex'): string;

  public digest(encoding?: 'hex'): Buffer | string {
    if (encoding === 'hex') {
      return this.hash.digest('hex');
    }
    return this.hash.digest();
  }
}

class Sha512Hasher implements Hasher {
  private hash: crypto.Hash;

  public constructor() {
    this.hash = crypto.createHash('sha512');
  }

  public update(data: string | Buffer): this {
    this.hash.update(data);
    return this;
  }

  public digest(): Buffer;

  public digest(encoding: 'hex'): string;

  public digest(encoding?: 'hex'): Buffer | string {
    if (encoding === 'hex') {
      return this.hash.digest('hex');
    }
    return this.hash.digest();
  }
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
 * hashing interface throughout the Cube.js codebase.
 *
 * The hasher can be used as a drop-in replacement for crypto.createHash()
 * in non-cryptographic contexts.
 *
 * By default, this uses MD5 hashing for backward compatibility. You can
 * choose different algorithms by setting the CUBEJS_HASHER_ALGORITHM
 * environment variable to: 'md5', 'sha256', 'sha512', or 'xxhash'.
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
  const algorithm = getEnv('hasherAlgorithm');

  if (algorithm) {
    const alg = algorithm.toLowerCase();

    if (alg === 'xxhash') {
      return new XxHasher();
    }

    if (alg === 'sha256') {
      return new Sha256Hasher();
    }

    if (alg === 'sha512') {
      return new Sha512Hasher();
    }
  }

  // Default to MD5 for backward compatibility
  return new Md5Hasher();
}
