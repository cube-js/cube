#!/usr/bin/env node
/**
 * `jest.base.config.js` declares no `preset` and no `transform`, so a package
 * extending it cannot execute TypeScript: its suites must come from `dist/test`,
 * and its config must say so. Without that, jest's default `testMatch` collects
 * the untransformable sources alongside the compiled output and a bare `jest`
 * dies on `import` — leaving the path argument in the `unit` script as the only
 * thing keeping the suite runnable.
 *
 * Sixteen packages had drifted that way before anything checked. This is the
 * check.
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
  const entries = Array.isArray(value) ? value : [value];

  return entries.length > 0 && entries.every(entry => {
    if (typeof entry !== 'string') return false;
    // Normalized so a Windows-style separator in a hand-written config reads the
    // same as a posix one.
    return /(^|\/)dist(\/|$)/.test(entry.replace(/\\/g, '/').replace('<rootDir>/', ''));
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
 * Reads the resolved config rather than the file's `require` target. That
 * distinction is what keeps `jest.base-ts.config.js` out of scope: it extends
 * the base config too, but adds `preset: 'ts-jest'` and a `transform`, so its
 * consumers run from source deliberately. Asking the resolved object whether a
 * transform exists answers "can this execute TypeScript" directly, instead of
 * inferring it from which base file was required.
 */
function violations() {
  return fs.readdirSync(PACKAGES_DIR, { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .map(entry => path.join(PACKAGES_DIR, entry.name))
    .filter(pkg => fs.existsSync(path.join(pkg, 'jest.config.js')))
    .flatMap(pkg => {
      // eslint-disable-next-line import/no-dynamic-require, global-require
      const config = require(path.join(pkg, 'jest.config.js'));

      // A package that brings its own transform (or a preset that implies one)
      // can execute its sources, so the dist-only convention does not apply.
      if (config.transform || config.preset) return [];

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
  const packages = fs.readdirSync(PACKAGES_DIR, { withFileTypes: true })
    .filter(entry => entry.isDirectory()).length;

  // Guards the guard: a moved directory or a bad glob would otherwise make
  // every assertion below vacuous and keep passing forever.
  if (packages === 0) {
    console.error('check-dist-test-target: found no packages — the walk is broken, not the tree.');
    process.exit(1);
  }

  const offenders = violations();

  if (offenders.length > 0) {
    console.error(
      'These packages extend jest.base.config.js and compile TypeScript tests into dist/,\n' +
      'but constrain collection to neither testMatch nor roots. A bare `jest` in them\n' +
      'collects the untransformable sources and fails on `import`:\n'
    );
    offenders.forEach(pkg => console.error(`  ${pkg}`));
    console.error(
      "\nAdd one of the following to the package's jest.config.js:\n" +
      "  testMatch: ['<rootDir>/dist/test/**/*.{test,spec}.{ts,js}']\n" +
      "  roots: ['<rootDir>/dist/test/']"
    );
    process.exit(1);
  }

  console.log(`check-dist-test-target: ${packages} packages checked, no drift.`);
}

main();
