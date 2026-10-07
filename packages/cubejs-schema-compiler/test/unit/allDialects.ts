import fs from 'fs';
import path from 'path';

// Tests run from `dist`, so this resolves to the compiled adapters.
const ADAPTER_DIR = path.join(__dirname, '..', '..', 'src', 'adapter');

// The adapter directory holds far more than this; the floor is only here so a scan that
// silently matched nothing fails loudly rather than passing every invariant vacuously.
const MIN_DIALECTS = 10;

/**
 * Every dialect in the schema-compiler adapter directory, discovered rather than listed
 * by hand. Driver packages carry Query subclasses outside this scan; for a given template
 * invariant the ones that matter are those redefining or deleting that template.
 *
 * Paired with its name, which is what a caller needs to report which dialect failed.
 */
export function allDialects(): [string, any][] {
  const classes = fs.readdirSync(ADAPTER_DIR)
    // `.ts` keeps this working if the tests are ever run from source.
    .map(file => file.match(/^(\w+Query)\.(?:ts|js)$/)?.[1])
    .filter((name): name is string => !!name && name !== 'BaseQuery')
    // eslint-disable-next-line global-require, import/no-dynamic-require
    .map(name => [name, require(path.join(ADAPTER_DIR, name))[name]] as [string, any]);

  if (classes.length < MIN_DIALECTS) {
    throw new Error(
      `Expected the adapter directory to hold at least ${MIN_DIALECTS} dialects, found ${classes.length}`
    );
  }

  return classes;
}

export function dialect(name: string): any {
  // eslint-disable-next-line global-require, import/no-dynamic-require
  return require(path.join(ADAPTER_DIR, name))[name];
}
