#!/usr/bin/env node
/**
 * `jest.base.config.js` declares no `preset` and no `transform`, so a package
 * extending it cannot execute TypeScript: its suites must come from `dist/test`,
 * and its config must say so. Without that, jest's default `testMatch` collects
 * the untransformable sources alongside the compiled output and a bare `jest`
 * dies on `import` — leaving the path argument in the `unit` script as the only
 * thing keeping the suite runnable.
 */

const fs = require('fs');
const path = require('path');

const REPO_ROOT = path.join(__dirname, '..');
const PACKAGES_DIR = path.join(REPO_ROOT, 'packages');

/**
 * Constraining collection to `dist/` has two spellings in this repo, and both
 * are correct: most packages use `testMatch`, `cubejs-backend-native` uses
 * `roots: ['<rootDir>/dist/test/']`. A check keyed on only one of them fails
 * forever on a package that is already right, which is how a guard like this
 * gets deleted instead of fixed.
 */
const CONSTRAINT_KEYS = ['testMatch', 'roots'];

/**
 * Presence of the key is not the property that matters — where it points is.
 * `testMatch: ['<rootDir>/test/**\/*.test.ts']` is a declared `testMatch` that
 * still aims at the untransformable sources, so checking only that the key
 * exists would wave through exactly the drift this guard is for. Every entry
 * must target `dist/`.
 */
function targetsDist(value) {
  const entries = (Array.isArray(value) ? value : [value])
    .filter(entry => typeof entry === 'string')
    // A `!`-negated entry subtracts from the set rather than widening it, so it
    // cannot break the constraint and should not have to name `dist/`.
    .filter(entry => !entry.startsWith('!'));

  return entries.length > 0 && entries.every(entry =>
    // Normalized so a Windows-style separator in a hand-written config reads the
    // same as a posix one.
    /(^|\/)dist(\/|$)/.test(entry.replace(/\\/g, '/').replace('<rootDir>/', '')));
}

/**
 * Whether the config can actually compile a TypeScript test — not merely
 * whether it mentions a transform. `transform: {}` is jest's idiom for turning
 * transformation *off*, and a `'^.+\\.js$': 'babel-jest'` entry covers no `.ts`
 * at all; both are truthy, so a presence check would exempt exactly the
 * packages that still break. The patterns are tested against a representative
 * filename, which is how jest itself decides.
 */
function transformsTypeScript(config) {
  if (typeof config.preset === 'string' && /ts-jest|typescript/i.test(config.preset)) return true;

  return Object.keys(config.transform || {}).some(pattern => {
    try {
      return new RegExp(pattern).test('example.test.ts');
    } catch (error) {
      // An unparseable pattern is jest's problem to report, not this check's
      // reason to exempt a package.
      return false;
    }
  });
}

const IGNORED_DIRS = new Set(['node_modules', 'dist', 'coverage', '__snapshots__']);

/**
 * TypeScript test sources anywhere in the package — `.tsx` as well as `.ts`,
 * and `.spec.` as well as `.test.`, since jest's default collects all of them
 * and every one fails the same way without a transform.
 *
 * The walk covers the whole package rather than just `test/`: a suite added
 * under `src/` is collected by jest's default just as readily, and would
 * otherwise be a test the guard cannot see.
 */
function typescriptTestSources(dir) {
  if (!fs.existsSync(dir)) return [];

  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return IGNORED_DIRS.has(entry.name) ? [] : typescriptTestSources(full);
    }
    return /\.(test|spec)\.tsx?$/.test(entry.name) ? [full] : [];
  });
}

/**
 * Jest reads its config from a `jest.config.*` file or from a `jest` key in
 * package.json, and the drift this guards against is indifferent to which. A
 * check that only opened `jest.config.js` would miss the driver packages that
 * carry a package.json block instead — several of which have TypeScript tests
 * kept alive purely by the path argument in their `unit` script.
 *
 * The extensions jest resolves beyond `.js` are listed so an unreadable one is
 * reported rather than silently skipped: being unable to inspect a config is a
 * different thing from a config being fine.
 */
const CONFIG_EXTENSIONS = ['js', 'cjs', 'mjs', 'ts', 'json'];

function resolveConfig(pkg) {
  const configFile = CONFIG_EXTENSIONS
    .map(extension => path.join(pkg, `jest.config.${extension}`))
    .find(candidate => fs.existsSync(candidate));

  if (configFile) {
    if (!configFile.endsWith('.js') && !configFile.endsWith('.cjs') && !configFile.endsWith('.json')) {
      // `require` cannot load these without a loader, and guessing would be
      // worse than saying so.
      throw new Error(
        `${path.relative(REPO_ROOT, pkg)}: ${path.basename(configFile)} cannot be inspected by this check`
      );
    }

    try {
      // eslint-disable-next-line import/no-dynamic-require, global-require
      return require(configFile);
    } catch (error) {
      // A config that cannot even be loaded is a worse problem than the one
      // this guard is for, and jest would fail on it too — so say which
      // package it is rather than dying on a bare stack trace.
      throw new Error(
        `${path.relative(REPO_ROOT, pkg)}: ${path.basename(configFile)} could not be loaded — ${error.message}`
      );
    }
  }

  const packageJson = path.join(pkg, 'package.json');
  if (!fs.existsSync(packageJson)) return null;

  // eslint-disable-next-line import/no-dynamic-require, global-require
  return require(packageJson).jest || null;
}

/**
 * Reads the resolved config rather than the file's `require` target. That
 * distinction is what keeps `jest.base-ts.config.js` out of scope: it extends
 * the base config too, but adds `preset: 'ts-jest'` and a `transform`, so its
 * consumers run from source deliberately. Asking the resolved object whether a
 * transform exists answers "can this execute TypeScript" directly, instead of
 * inferring it from which base file was required.
 */
function violations(inspected) {
  return fs.readdirSync(PACKAGES_DIR, { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .map(entry => path.join(PACKAGES_DIR, entry.name))
    .flatMap(pkg => {
      const config = resolveConfig(pkg);

      // No jest config at all: the package does not run jest, so there is no
      // collection to constrain.
      if (!config) return [];

      inspected.push(pkg);

      // A package that can actually compile TypeScript may test its sources
      // deliberately, so the dist-only convention does not apply to it.
      if (transformsTypeScript(config)) return [];

      // With no TypeScript test sources there is nothing untransformable for
      // jest's default to pick up — a package testing plain `.js`, or with no
      // tests at all, is collectible exactly as it stands.
      if (typescriptTestSources(pkg).length === 0) return [];

      const constrained = CONSTRAINT_KEYS
        .filter(key => config[key] !== undefined)
        .some(key => targetsDist(config[key]));

      if (constrained) return [];

      return [path.relative(REPO_ROOT, pkg)];
    });
}

function main() {
  const inspected = [];
  const offenders = violations(inspected);

  // Guards the guard. Counting the configs actually read, rather than the
  // directories walked, is what makes this meaningful: if jest configs ever
  // move to a filename this check does not resolve, the walk still finds a
  // healthy number of package directories while inspecting none of them, and
  // the run would pass vacuously.
  if (inspected.length === 0) {
    console.error('check-dist-test-target: inspected no jest configs — the walk is broken, not the tree.');
    process.exit(1);
  }

  if (offenders.length > 0) {
    console.error(
      'These packages have TypeScript tests and a jest config that cannot transform them,\n' +
      'but do not confine collection to dist/. A bare `jest` in them collects the\n' +
      'untransformable sources and fails on `import`:\n'
    );
    offenders.forEach(pkg => console.error(`  ${pkg}`));
    console.error(
      "\nAdd one of the following to the package's jest.config.js:\n" +
      "  testMatch: ['<rootDir>/dist/test/**/*.{test,spec}.{ts,js}']\n" +
      "  roots: ['<rootDir>/dist/test/']"
    );
    process.exit(1);
  }

  console.log(`check-dist-test-target: ${inspected.length} jest configs checked, no drift.`);
}

try {
  main();
} catch (error) {
  console.error(`check-dist-test-target: ${error.message}`);
  process.exit(1);
}
