#!/usr/bin/env node
/**
 * The checker is itself the assertion for the dist-only convention, so a change
 * to its predicate has nothing else to fail against. These cases pin the shape:
 * both constraint spellings, the values that only look constrained, the configs
 * that legitimately run from source, and the two ways a package escapes the
 * walk entirely.
 *
 * Dependency-free and fixture-driven — no jest, no build — so it runs in the
 * same `lint` step as the checker it covers.
 */

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const {
  violations, targetsDist, transformsTypeScript, workspaceDirs,
} = require('./check-dist-test-target');

const TS_TEST = 'test/unit/thing.test.ts';

/**
 * Writes one fixture package. Exactly one config carriage applies, in the order
 * checked: `configIn: 'package.json'` puts `config` in the manifest, `config`
 * alone writes `jest.config.js`, and `rawConfig` writes verbatim source to
 * `jest.config.<configExt>` — the three shapes the real tree uses, and the
 * reason the checker reads all of them.
 */
function writePackage(root, name, spec) {
  const pkg = path.join(root, name);
  fs.mkdirSync(pkg, { recursive: true });

  const manifest = { name, scripts: spec.scripts || {} };

  if (spec.configIn === 'package.json') {
    manifest.jest = spec.config;
  } else if (spec.config !== undefined) {
    fs.writeFileSync(
      path.join(pkg, 'jest.config.js'),
      `module.exports = ${JSON.stringify(spec.config)};`
    );
  } else if (spec.rawConfig) {
    fs.writeFileSync(path.join(pkg, `jest.config.${spec.configExt || 'js'}`), spec.rawConfig);
  }

  // A second config file, to pin which one the checker resolves first.
  if (spec.alsoConfig) {
    fs.writeFileSync(
      path.join(pkg, `jest.config.${spec.alsoConfigExt}`),
      `module.exports = ${JSON.stringify(spec.alsoConfig)};`
    );
  }

  fs.writeFileSync(path.join(pkg, 'package.json'), JSON.stringify(manifest));

  (spec.sources || []).forEach(relative => {
    const file = path.join(pkg, relative);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, '');
  });

  return pkg;
}

/** Runs the checker over one throwaway tree and returns what it reported. */
function check(packages) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dist-test-target-'));
  const workspace = path.join(root, 'packages');
  fs.mkdirSync(workspace);

  Object.entries(packages).forEach(([name, spec]) => writePackage(workspace, name, spec));

  try {
    const { offenders, inspected, uninspectable } = violations([workspace]);
    return {
      offenders: offenders.map(entry => path.basename(entry)),
      inspected: inspected.length,
      uninspectable,
    };
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

const cases = [];
const test = (name, fn) => cases.push([name, fn]);

// --- the three-part predicate -------------------------------------------------

test('flags a package with TS tests, no transform and no constraint', () => {
  const { offenders } = check({
    drifted: { config: { testEnvironment: 'node' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('accepts testMatch pointing into dist', () => {
  const { offenders } = check({
    ok: {
      config: { testMatch: ['<rootDir>/dist/test/**/*.test.js'] },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
});

test('accepts roots pointing into dist — the cubejs-backend-native spelling', () => {
  const { offenders } = check({
    ok: { config: { roots: ['<rootDir>/dist/test/'] }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
});

test('flags a declared testMatch that still aims at the sources', () => {
  const { offenders } = check({
    drifted: { config: { testMatch: ['<rootDir>/test/**/*.test.ts'] }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('flags a mixed array where one entry escapes dist', () => {
  const { offenders } = check({
    drifted: {
      config: { testMatch: ['<rootDir>/dist/test/a.test.js', '<rootDir>/test/b.test.ts'] },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('a negated entry need not name dist', () => {
  const { offenders } = check({
    ok: {
      config: { testMatch: ['<rootDir>/dist/test/**/*.test.js', '!**/fixtures/**'] },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
});

test('flags an empty constraint array — it constrains nothing', () => {
  const { offenders } = check({
    drifted: { config: { testMatch: [] }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('one key naming dist confines the intersection, whatever the other says', () => {
  // Jest ANDs roots and testMatch, so `dist/` roots plus a broad testMatch
  // collects only dist — a correct package. Demanding both would report it as
  // an offender, which is the false positive that gets a guard deleted.
  const { offenders } = check({
    ok: {
      config: { roots: ['<rootDir>/dist/test/'], testMatch: ['**/*.test.js'] },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
});

test('flags a package where neither present key names dist', () => {
  const { offenders } = check({
    drifted: {
      config: { roots: ['<rootDir>/'], testMatch: ['<rootDir>/test/**/*.test.ts'] },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('does not match a dist-lookalike directory', () => {
  const { offenders } = check({
    drifted: { config: { roots: ['<rootDir>/dist-helpers/'] }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

// --- packages the convention does not apply to --------------------------------

test('exempts a ts-jest preset — it runs from source deliberately', () => {
  const { offenders } = check({
    tsjest: { config: { preset: 'ts-jest' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
});

test('exempts a real .ts transform', () => {
  const { offenders } = check({
    transformed: {
      config: { transform: { '^.+\\.tsx?$': 'ts-jest' } },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
});

test('flags a .ts-only transform beside a .tsx suite', () => {
  // The transform covers nothing the package actually has.
  const { offenders } = check({
    drifted: {
      config: { transform: { '^.+\\.ts$': 'ts-jest' } },
      sources: ['src/widget.spec.tsx'],
    },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('flags a .ts-only transform when only SOME suites are .ts', () => {
  // The .ts suites run and the .tsx ones do not, so the package is not exempt:
  // an exemption has to mean every test here can run, not that one of them can.
  const { offenders } = check({
    drifted: {
      config: { transform: { '^.+\\.ts$': 'ts-jest' } },
      sources: [TS_TEST, 'src/widget.spec.tsx'],
    },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('exempts a .ts-only transform when every suite is .ts', () => {
  const { offenders } = check({
    ok: { config: { transform: { '^.+\\.ts$': 'ts-jest' } }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
});

test('flags transform: {} — jest\'s idiom for no transformation at all', () => {
  const { offenders } = check({
    drifted: { config: { transform: {} }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('flags a .js-only transform, which covers no TypeScript', () => {
  const { offenders } = check({
    drifted: {
      config: { transform: { '^.+\\.js$': 'babel-jest' } },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
});

test('exempts a package with no TypeScript tests', () => {
  const { offenders } = check({
    jsonly: { config: { testEnvironment: 'node' }, sources: ['test/unit/thing.test.js'] },
  });
  assert.deepStrictEqual(offenders, []);
});

test('sees .tsx suites and suites outside test/', () => {
  const { offenders } = check({
    tsx: { config: { testEnvironment: 'node' }, sources: ['src/widget.spec.tsx'] },
  });
  assert.deepStrictEqual(offenders, ['tsx']);
});

// --- how the config is carried ------------------------------------------------

test('reads a jest key in package.json', () => {
  const { offenders } = check({
    manifest: {
      config: { testEnvironment: 'node' },
      configIn: 'package.json',
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, ['manifest']);
});

test('a config exporting a function is reported, not misread as empty', () => {
  const { offenders, uninspectable } = check({
    fn: {
      rawConfig: 'module.exports = () => ({ testMatch: ["<rootDir>/dist/test/**"] });',
      sources: [TS_TEST],
    },
  });
  // Correct config behind a function: must not be called an offender.
  assert.deepStrictEqual(offenders, []);
  assert.strictEqual(uninspectable.length, 1);
  assert.match(uninspectable[0], /exports a function/);
});

test('an unloadable config does not abort the walk past other offenders', () => {
  const { offenders, uninspectable } = check({
    broken: { rawConfig: 'throw new Error("boom");', sources: [TS_TEST] },
    drifted: { config: { testEnvironment: 'node' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['drifted']);
  assert.strictEqual(uninspectable.length, 1);
  assert.match(uninspectable[0], /could not be loaded/);
});

test('resolves jest.config.js ahead of jest.config.ts, as jest does', () => {
  // .ts cannot be required, so if precedence ever inverts this stops being an
  // exempt package and starts being an uninspectable one.
  const { offenders, uninspectable } = check({
    both: {
      config: { testMatch: ['<rootDir>/dist/test/**'] },
      alsoConfig: {},
      alsoConfigExt: 'ts',
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
  assert.deepStrictEqual(uninspectable, []);
});

test('an uninspectable extension is reported by package name', () => {
  const { offenders, uninspectable } = check({
    esm: { rawConfig: 'export default {};', configExt: 'mjs', sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
  assert.strictEqual(uninspectable.length, 1);
  assert.match(uninspectable[0], /cannot be inspected/);
});

test('an unknown jest.config.* spelling is reported, not called a dist offender', () => {
  // Falling through to the package.json branch would find nothing and blame
  // the dist target — the wrong defect, with a fix that would not help.
  const { offenders, uninspectable } = check({
    mts: {
      rawConfig: 'export default {};',
      configExt: 'mts',
      scripts: { unit: 'jest dist/test' },
      sources: [TS_TEST],
    },
  });
  assert.deepStrictEqual(offenders, []);
  assert.strictEqual(uninspectable.length, 1);
  assert.match(uninspectable[0], /jest\.config\.mts cannot be inspected/);
});

// --- packages with no config at all -------------------------------------------

test('flags no config at all when the package still invokes jest', () => {
  const { offenders } = check({
    bare: { scripts: { unit: 'jest dist/test/unit' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, ['bare']);
});

test('exempts no config and no jest invocation', () => {
  const { offenders } = check({
    notjest: { scripts: { build: 'tsc' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
});

test('does not read jest out of an unrelated script name', () => {
  const { offenders } = check({
    notjest: { scripts: { build: 'node build-jestless.js' }, sources: [TS_TEST] },
  });
  assert.deepStrictEqual(offenders, []);
});

// --- guarding the guard --------------------------------------------------------

test('counts the packages it actually inspected', () => {
  const { inspected } = check({
    a: { config: { testMatch: ['<rootDir>/dist/test/**'] }, sources: [TS_TEST] },
    b: { config: { preset: 'ts-jest' }, sources: [TS_TEST] },
    c: { scripts: { unit: 'jest dist/test' }, sources: ['test/thing.test.js'] },
    d: { scripts: { build: 'tsc' } },
  });
  // d carries no config and runs no jest, so it is never inspected. c has no
  // config but does run jest, so it is — counted the same as a config-carrying
  // package, which is the point of running both through one predicate.
  assert.strictEqual(inspected, 3);
});

// --- unit-level predicates -----------------------------------------------------

test('targetsDist accepts a bare string as well as an array', () => {
  assert.strictEqual(targetsDist('<rootDir>/dist/test/'), true);
  assert.strictEqual(targetsDist('<rootDir>/test/'), false);
});

test('targetsDist normalizes a Windows-style separator', () => {
  assert.strictEqual(targetsDist(['<rootDir>\\dist\\test\\']), true);
});

test('transformsTypeScript reads a typescript preset case-insensitively', () => {
  assert.strictEqual(transformsTypeScript({ preset: 'ts-jest/presets/default' }), true);
  assert.strictEqual(transformsTypeScript({ preset: 'jest-preset-angular' }), false);
});

test('transformsTypeScript survives an unparseable transform pattern', () => {
  assert.strictEqual(transformsTypeScript({ transform: { '[': 'whatever' } }), false);
});

// --- deriving the walk roots ---------------------------------------------------

test('workspaceDirs reads both the array and the object spelling', () => {
  const fromArray = workspaceDirs(['packages/*', 'rust/*']).map(d => path.basename(d));
  const fromObject = workspaceDirs({ packages: ['packages/*', 'rust/*'] }).map(d => path.basename(d));
  assert.deepStrictEqual(fromArray, ['packages', 'rust']);
  assert.deepStrictEqual(fromObject, ['packages', 'rust']);
});

test('workspaceDirs keeps a ** pattern instead of dropping the tree', () => {
  // Dropping it would be undetectable: the walk still resolves configs from the
  // trees it does visit, so the vacuous-pass guard would never fire.
  assert.deepStrictEqual(
    workspaceDirs(['packages/**']).map(d => path.basename(d)),
    ['packages']
  );
});

test('workspaceDirs handles a nested pattern and dedupes', () => {
  const dirs = workspaceDirs(['a/b/*', 'a/b/*', 'packages/*']);
  assert.strictEqual(dirs.length, 2);
  assert.strictEqual(path.basename(dirs[0]), 'b');
});

test('workspaceDirs refuses a manifest that declares no workspaces', () => {
  // Named rather than a bare TypeError on `undefined.filter` — and thrown
  // where main()'s handler can report it, not at require time.
  assert.throws(() => workspaceDirs({}), /declares no workspaces/);
  assert.throws(() => workspaceDirs([]), /declares no workspaces/);
  assert.throws(() => workspaceDirs(null), /declares no workspaces/);
});

test('the real root manifest still yields both workspace trees', () => {
  assert.deepStrictEqual(workspaceDirs().map(d => path.basename(d)).sort(), ['packages', 'rust']);
});

let failed = 0;
cases.forEach(([name, fn]) => {
  try {
    const returned = fn();
    // An async body would hand back a promise this synchronous runner never
    // awaits, so a rejection would go unseen and the case would print as
    // passing. Refuse the shape rather than fail open.
    if (returned && typeof returned.then === 'function') {
      throw new Error('case returned a promise; this runner is synchronous');
    }
  } catch (error) {
    failed += 1;
    console.error(`  ✗ ${name}\n    ${error.message.split('\n').join('\n    ')}`);
  }
});

if (failed > 0) {
  console.error(`\ncheck-dist-test-target.test: ${failed} of ${cases.length} cases failed.`);
  process.exit(1);
}

console.log(`check-dist-test-target.test: ${cases.length} cases passed.`);
