import fs from 'fs';
import path from 'path';

// Tests run from `dist`, so this resolves to the compiled adapters.
const ADAPTER_DIR = path.join(__dirname, '..', '..', 'src', 'adapter');

/**
 * Every dialect, read off the adapter directory rather than listed by hand, so a dialect
 * added later cannot quietly escape an invariant asserted over all of them.
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

  if (classes.length < 10) {
    throw new Error(`Expected the adapter directory to hold more dialects than ${classes.length}`);
  }

  return classes;
}

export function dialect(name: string): any {
  // eslint-disable-next-line global-require, import/no-dynamic-require
  return require(path.join(ADAPTER_DIR, name))[name];
}
