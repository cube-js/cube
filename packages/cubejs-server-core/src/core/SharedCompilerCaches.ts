import type vm from 'vm';
import { LRUCache } from 'lru-cache';

/**
 * Process-wide compile caches (CUBEJS_COMPILER_MULTI_TENANT_SHARING), handed to every CompilerApi
 * instead of one set per app.
 *
 * Only caches whose value is a function of the key alone are shared:
 * - compiledScriptCache: key = file name + transpiled JS source (the shared-realm scripts, with
 *   their `with` wrapper, under their own prefix); a vm.Script doesn't depend on the context
 *   it later runs in.
 * - compiledYamlCache: key = YAML (or rendered Jinja) content; YAML is transpiled before any
 *   cube symbols exist, so the output depends on the content only.
 *
 * The Jinja render cache is NOT shared: its key is the template (and macros), but the output
 * depends on the app's COMPILE_CONTEXT (security context) and its Python globals, so sharing
 * it would hand one tenant's rendered model to another.
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

/**
 * Entries of each shared cache: sized for the distinct files of all apps, as a JS model file
 * takes up to one compiled-script entry per compile phase.
 */
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
