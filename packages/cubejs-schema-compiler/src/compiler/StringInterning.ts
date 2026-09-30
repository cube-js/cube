import { getEnv } from '@cubejs-backend/shared';

/**
 * String interning for compiled data models (CUBEJS_COMPILER_MULTI_TENANT_SHARING).
 *
 * A process that keeps many compiled apps holds the same member names, titles and descriptions
 * once per app, and strings built with `+` or template literals stay as trees of ConsString
 * nodes. Using a string as a key of a dictionary-mode object makes V8 internalize it: the key
 * is a flat copy in V8's string table, and every equal string maps to the same one. The string
 * table is weak, so the "pool" costs nothing to keep and never outlives the models using it.
 */

const stats = {
  calls: 0,
  chars: 0,
  nanos: BigInt(0),
};

// Canonical array indices ("0", "42") become elements, not named keys, so they aren't
// internalized as names. They are short: leave them as they are.
const ARRAY_INDEX = /^(?:0|[1-9]\d{0,9})$/;

export function internString(value: string): string {
  if (value.length === 0 || ARRAY_INDEX.test(value)) {
    return value;
  }

  const holder = Object.create(null);
  holder[value] = true;

  // eslint-disable-next-line no-restricted-syntax, guard-for-in
  for (const key in holder) {
    return key;
  }
  return value;
}

// Model files run in their own vm context, so their objects inherit from that realm's
// Object.prototype: accept any direct child of a root prototype, not only this realm's.
// Class instances, Maps, Dates and the like have a longer prototype chain.
const isPlainObject = (value: object): boolean => {
  const proto = Object.getPrototypeOf(value);

  return proto === null || Object.getPrototypeOf(proto) === null;
};

/**
 * Replaces, in place, every string held by the plain objects and arrays reachable from `root`
 * with its interned copy. Equal values are replaced by equal values, so nothing a reader sees
 * changes. Only own data properties are touched: accessors (and whatever they would compute),
 * functions, class instances, non-writable properties and frozen objects are left alone.
 */
export function internStringsDeep<T>(root: T): T {
  if (root === null || typeof root !== 'object') {
    return root;
  }

  const started = process.hrtime.bigint();
  const visited = new WeakSet<object>();
  const stack: object[] = [root as unknown as object];

  const visitValue = (value: unknown): unknown => {
    if (typeof value === 'string') {
      stats.calls++;
      stats.chars += value.length;
      return internString(value);
    }
    if (value !== null && typeof value === 'object' && !visited.has(value)) {
      stack.push(value);
    }
    return value;
  };

  const visitObject = (obj: object) => {
    const frozen = Object.isFrozen(obj);

    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        const value = obj[i];
        const interned = visitValue(value);

        if (interned !== value && !frozen) {
          obj[i] = interned;
        }
      }
    } else if (isPlainObject(obj)) {
      for (const key of Object.keys(obj)) {
        const descriptor = Object.getOwnPropertyDescriptor(obj, key);

        if (descriptor && 'value' in descriptor) {
          const interned = visitValue(descriptor.value);

          if (typeof descriptor.value === 'string' && !frozen && descriptor.writable) {
            obj[key] = interned;
          }
        }
      }
    }
  };

  while (stack.length) {
    const obj = stack.pop()!;

    if (!visited.has(obj)) {
      visited.add(obj);

      try {
        visitObject(obj);
      } catch {
        // A proxy whose traps throw (a guarded COMPILE_CONTEXT, say): leave it as it is
      }
    }
  }

  stats.nanos += process.hrtime.bigint() - started;
  return root;
}

/**
 * `internStringsDeep` when CUBEJS_COMPILER_MULTI_TENANT_SHARING is on, a no-op otherwise.
 */
export function internCompiledStrings<T>(root: T): T {
  return getEnv('compilerMultiTenantSharing') ? internStringsDeep(root) : root;
}

/**
 * What interning has cost this process so far: strings passed through the pool, their total
 * length, and the time spent walking models and interning.
 */
export function internedStringsStats(): { calls: number; chars: number; ms: number } {
  return {
    calls: stats.calls,
    chars: stats.chars,
    ms: Math.round(Number(stats.nanos) / 1e6),
  };
}
