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

/**
 * `packages/` is where the drift happened, but it is not the only place a
 * workspace runs jest: `rust/cubestore` does too. Read off the root manifest
 * rather than transcribed, so a workspace added there is covered without
 * anyone remembering to widen this.
 *
 * Losing a tree here is the one failure the `inspected` guard below cannot
 * catch — the walk would still resolve plenty of configs from the trees it does
 * visit — so the pattern handling errs toward keeping a tree, and refuses
 * outright rather than returning a set it is not sure of.
 */
function workspaceDirs(
  // eslint-disable-next-line import/no-dynamic-require, global-require
  workspaces = require(path.join(REPO_ROOT, 'package.json')).workspaces
) {
  // Both spellings are valid: a bare array, or an object with `packages` (which
  // is what this repo uses, only because it also needs `nohoist`).
  const patterns = Array.isArray(workspaces) ? workspaces : (workspaces || {}).packages;

  if (!Array.isArray(patterns) || patterns.length === 0) {
    throw new Error('root package.json declares no workspaces — nothing to check');
  }

  // Everything up to the first glob segment, so `packages/**` keeps its tree
  // rather than being dropped for not ending in a literal `/*`.
  const dirs = patterns.map(pattern => {
    const segments = pattern.split('/');
    const glob = segments.findIndex(segment => segment.includes('*'));
    return path.join(REPO_ROOT, ...(glob === -1 ? segments : segments.slice(0, glob)));
  });

  return [...new Set(dirs)];
}

/**
 * `<workspace>/<package>` — the same label whether the walk root is this repo's
 * `packages/` or a fixture tree, so the checker can be exercised against
 * fixtures without its reporting reading as paths outside the repo.
 */
function label(pkg) {
  return path.join(path.basename(path.dirname(pkg)), path.basename(pkg));
}

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
function transformsTypeScript(config, sources = []) {
  if (typeof config.preset === 'string' && /ts-jest|typescript/i.test(config.preset)) return true;

  // Probed against the extensions this package's suites actually use. A
  // `\.ts$`-only transform beside a `.tsx` suite covers nothing that is really
  // there, so the exemption has to mean "every test here can run", not "some
  // test could have". With no sources yet known, probe both.
  const extensions = sources.length > 0
    ? [...new Set(sources.map(file => path.extname(file).slice(1)))]
    : ['ts', 'tsx'];

  return Object.keys(config.transform || {}).some(pattern => {
    try {
      const matcher = new RegExp(pattern);
      return extensions.every(extension => matcher.test(`example.test.${extension}`));
    } catch (error) {
      // An unparseable pattern is jest's problem to report, not this check's
      // reason to exempt a package.
      return false;
    }
  });
}

// `target` is Cargo build output — 7.5 GB of it under `cubejs-backend-native`
// alone, and it can hold no TypeScript test source this guard cares about.
// Walking it costs more than the entire rest of the check put together.
const IGNORED_DIRS = new Set(['node_modules', 'dist', 'coverage', '__snapshots__', 'target']);

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
 * Resolution follows jest's own `JEST_CONFIG_EXT_ORDER`, so a package carrying
 * two configs has the one inspected here that jest would actually load.
 *
 * Any *other* single-extension `jest.config.<ext>` is picked up too, and
 * reported as uninspectable. Matching only a fixed list would let a spelling
 * this check does not know — `jest.config.mts`, or whatever a later jest adds
 * — fall through to the package.json branch, find nothing, and get reported as
 * a *dist-target offender*: the wrong defect, with a fix suggestion that would
 * not help. Naming a config we cannot read is the honest failure.
 *
 * Single-extension is the load-bearing part. `jest.config.unit.js` and friends
 * are `--config` targets that jest does not load by default, so reading one as
 * *the* config would exempt a package on a config that is not in play — and
 * with several of them, on whichever the directory happened to yield first.
 */
const CONFIG_EXTENSIONS = ['js', 'ts', 'mjs', 'cjs', 'json'];
const LOADABLE_EXTENSIONS = ['.js', '.cjs', '.json'];

function resolveConfig(pkg) {
  const known = CONFIG_EXTENSIONS
    .map(extension => path.join(pkg, `jest.config.${extension}`))
    .find(candidate => fs.existsSync(candidate));

  const configFile = known || fs.readdirSync(pkg)
    .filter(entry => /^jest\.config\.[^.]+$/.test(entry))
    .map(entry => path.join(pkg, entry))
    .find(candidate => fs.statSync(candidate).isFile());

  if (configFile) {
    if (!LOADABLE_EXTENSIONS.includes(path.extname(configFile))) {
      // `require` cannot load these without a loader, and guessing would be
      // worse than saying so.
      throw new Error(
        `${label(pkg)}: ${path.basename(configFile)} cannot be inspected by this check`
      );
    }

    let loaded;
    try {
      // eslint-disable-next-line import/no-dynamic-require, global-require
      loaded = require(configFile);
    } catch (error) {
      // A config that cannot even be loaded is a worse problem than the one
      // this guard is for, and jest would fail on it too — so say which
      // package it is rather than dying on a bare stack trace.
      throw new Error(
        `${label(pkg)}: ${path.basename(configFile)} could not be loaded — ${error.message}`
      );
    }

    // Jest also accepts a config that exports a (possibly async) function.
    // Reading one as a plain object finds no `preset`, no `transform` and no
    // constraint key, so a package that is entirely correct would be reported
    // as an offender — the false-positive mode that gets a guard deleted rather
    // than fixed. Refuse to guess instead.
    if (typeof loaded === 'function') {
      throw new Error(
        `${label(pkg)}: ${path.basename(configFile)} exports a function, `
        + 'which this check cannot resolve without running jest'
      );
    }

    return loaded;
  }

  const packageJson = path.join(pkg, 'package.json');
  if (!fs.existsSync(packageJson)) return null;

  // eslint-disable-next-line import/no-dynamic-require, global-require
  return require(packageJson).jest || null;
}

/**
 * A package with no config at all is only out of scope if it never runs jest.
 * `cubejs-trino-driver` does run it — `jest dist/test/unit` — with nothing but
 * that path argument standing between it and jest's default `testMatch`, which
 * is precisely the defect. Treating config absence as "not a jest package"
 * would let the guard exit clean over it.
 *
 * Read off the `scripts` strings deliberately. A `jest` devDependency looks
 * like the deeper signal and is a trap: thirteen packages here invoke jest
 * while relying on the hoisted root copy, and two declare it without ever
 * running it — so that signal trades zero errors for thirteen false negatives.
 */
function invokesJest(pkg) {
  const packageJson = path.join(pkg, 'package.json');
  if (!fs.existsSync(packageJson)) return false;

  // eslint-disable-next-line import/no-dynamic-require, global-require
  const scripts = require(packageJson).scripts || {};

  return Object.values(scripts).some(script =>
    typeof script === 'string' && /(^|[\s;&|])jest(\s|$)/.test(script));
}

/**
 * Reads the resolved config rather than the file's `require` target. That
 * distinction is what keeps `jest.base-ts.config.js` out of scope: it extends
 * the base config too, but adds `preset: 'ts-jest'` and a `transform`, so its
 * consumers run from source deliberately. Asking the resolved object whether a
 * transform exists answers "can this execute TypeScript" directly, instead of
 * inferring it from which base file was required.
 *
 * Returns the offenders alongside what was inspected and what could not be, so
 * the caller can tell an empty offender list apart from a walk that saw nothing.
 */
function violations(roots = workspaceDirs()) {
  const inspected = [];
  const uninspectable = [];

  const offenders = roots
    .filter(dir => fs.existsSync(dir))
    .flatMap(dir => fs.readdirSync(dir, { withFileTypes: true })
      .filter(entry => entry.isDirectory())
      .map(entry => path.join(dir, entry.name)))
    .flatMap(pkg => {
      let config;
      try {
        config = resolveConfig(pkg);
      } catch (error) {
        // One config this check cannot read must not cost it the offenders it
        // has already found: on a tree with two dozen of them, aborting the
        // walk turns a useful list into a one-line message and hides the drift.
        // Collected here and reported alongside, still non-zero either way.
        uninspectable.push(error.message);
        return [];
      }

      // No jest config at all. That only means "nothing to constrain" if the
      // package never runs jest; if it does, its collection is governed by
      // jest's defaults — which constrain nothing, so the empty object below
      // runs it through the same predicate rather than a parallel copy of it.
      if (!config) {
        if (!invokesJest(pkg)) return [];
        config = {};
      }

      inspected.push(pkg);

      // With no TypeScript test sources there is nothing untransformable for
      // jest's default to pick up — a package testing plain `.js`, or with no
      // tests at all, is collectible exactly as it stands.
      const sources = typescriptTestSources(pkg);
      if (sources.length === 0) return [];

      // A package that can actually compile the tests it has may test its
      // sources deliberately, so the dist-only convention does not apply to it.
      if (transformsTypeScript(config, sources)) return [];

      // `some`, not `every`. Jest ANDs the constraint keys — `SearchSource`
      // pushes `roots` and `testMatch` as separate cases a path must satisfy
      // both of — so one key naming `dist/` confines the intersection whatever
      // the other says. `roots: ['<rootDir>/dist/test/']` beside a default-ish
      // `testMatch: ['**/*.test.js']` is a *correct* package, and demanding
      // both would report it as an offender: the false-positive mode that gets
      // a guard deleted rather than fixed.
      const present = CONSTRAINT_KEYS.filter(key => config[key] !== undefined);
      const constrained = present.some(key => targetsDist(config[key]));

      if (constrained) return [];

      return [label(pkg)];
    });

  return { offenders, inspected, uninspectable };
}

function main() {
  const { offenders, inspected, uninspectable } = violations();

  // Reported before the vacuous-pass guard below: when every config in the
  // tree is uninspectable, that guard is what fires, and exiting on it first
  // would swallow the per-package errors saying why.
  if (uninspectable.length > 0) {
    console.error('These jest configs could not be inspected, so their packages are unchecked:\n');
    uninspectable.forEach(message => console.error(`  ${message}`));
    console.error('');
  }

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
      'These packages have TypeScript tests and no jest config that confines collection\n' +
      'to dist/, so a bare `jest` in them collects the untransformable sources and fails\n' +
      'on `import`:\n'
    );
    offenders.forEach(pkg => console.error(`  ${pkg}`));
    console.error(
      "\nAdd one of the following to the package's jest.config.js:\n" +
      "  testMatch: ['<rootDir>/dist/test/**/*.{test,spec}.{ts,js}']\n" +
      "  roots: ['<rootDir>/dist/test/']"
    );
  }

  if (offenders.length > 0 || uninspectable.length > 0) process.exit(1);

  console.log(`check-dist-test-target: ${inspected.length} jest configs checked, no drift.`);
}

// Exported so the checker can be exercised against fixture trees. This file is
// the assertion for the whole convention, so it needs one of its own — nothing
// else fails if `targetsDist` or the three-part predicate quietly changes shape.
module.exports = { violations, targetsDist, transformsTypeScript, workspaceDirs };

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(`check-dist-test-target: ${error.message}`);
    process.exit(1);
  }
}
