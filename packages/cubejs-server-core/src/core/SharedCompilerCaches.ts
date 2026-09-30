import type vm from 'vm';
import { LRUCache } from 'lru-cache';

/**
 * Process-wide script and YAML caches, whose values depend on their key alone. The Jinja render
 * cache is not shared: its output depends on the app's COMPILE_CONTEXT and Python globals.
 */
export type SharedCompilerCaches = {
  compiledScriptCache: LRUCache<string, vm.Script>;
  compiledYamlCache: LRUCache<string, string>;
};

export type SharedCompilerCachesOptions = {
  max?: number;
  ttl?: number;
  updateAgeOnGet?: boolean;
};

// Sized for the distinct files of all apps: a JS file takes an entry per compile phase
export const SHARED_COMPILER_CACHES_MAX = 10000;

let sharedCaches: SharedCompilerCaches | null = null;
let purgeInterval: NodeJS.Timeout | null = null;

/**
 * The process-wide caches, created on first use. The options of the first caller win: every
 * CompilerApi of a server is created with the same keep-alive settings.
 */
export function getSharedCompilerCaches(options: SharedCompilerCachesOptions = {}): SharedCompilerCaches {
  if (!sharedCaches) {
    const cacheOptions = {
      max: options.max || SHARED_COMPILER_CACHES_MAX,
      ttl: options.ttl,
      updateAgeOnGet: options.updateAgeOnGet,
    };
    sharedCaches = {
      compiledScriptCache: new LRUCache<string, vm.Script>(cacheOptions),
      compiledYamlCache: new LRUCache<string, string>(cacheOptions),
    };

    if (options.ttl) {
      const caches = sharedCaches;
      purgeInterval = setInterval(() => {
        caches.compiledScriptCache.purgeStale();
        caches.compiledYamlCache.purgeStale();
      }, options.ttl);
      // Nothing to wait for: an entry that outlives its TTL is dropped on its next get anyway
      purgeInterval.unref();
    }
  }

  return sharedCaches;
}

/**
 * Drops the process-wide caches (tests, or a server shutting down): the next CompilerApi
 * starts new ones. CompilerApis already holding the old ones keep using them.
 */
export function resetSharedCompilerCaches(): void {
  if (purgeInterval) {
    clearInterval(purgeInterval);
    purgeInterval = null;
  }
  sharedCaches = null;
}
