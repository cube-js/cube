import fs from 'fs';
import path from 'path';
// No @types/micromatch in the tree, and this package sets `noImplicitAny: false`
// — require it rather than rely on that staying true.
// eslint-disable-next-line global-require
const micromatch = require('micromatch');

// This file executes from dist/test, so the package root is two levels up and
// the jest config must be required by absolute path rather than a relative one.
const PACKAGE_ROOT = path.join(__dirname, '..', '..');

// eslint-disable-next-line import/no-dynamic-require, global-require
const jestConfig = require(path.join(PACKAGE_ROOT, 'jest.config.js'));

// `__snapshots__` is listed rather than left to the filename filter below: a
// snapshot always carries two extensions (`foo.test.ts.snap`), so it happens not
// to match today, but that is an accident of naming rather than an intent.
const IGNORED_DIRS = new Set(['node_modules', 'dist', 'coverage', '__snapshots__']);

/**
 * Every test file in the package must have a compiled counterpart that
 * `testMatch` actually collects. This package runs its tests from `dist/test`,
 * so a source file the build never emits — or emits somewhere `testMatch` does
 * not look — is a test that silently stops running. That is how
 * `date-parser.test.js` and `normalize-query-filters-dates.test.js` came to be
 * editable but inert.
 *
 * The walk covers the whole package rather than just `test/`, so a test added
 * under `src/` (which compiles to `dist/src/`, outside `testMatch`) is caught
 * too, and it matches any `.test.*` extension so a novel one cannot slip past.
 */
function sourceTestFiles(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return IGNORED_DIRS.has(entry.name) ? [] : sourceTestFiles(full);
    }
    // `.spec.` as well as `.test.`: jest's default testMatch collected both, so
    // a spec file must not become invisible just because the pattern is pinned.
    return /\.(test|spec)\.[^.]+$/.test(entry.name) ? [full] : [];
  });
}

const SOURCE_TEST_FILES = sourceTestFiles(PACKAGE_ROOT);

const relativeToPackage = (file: string) => path.relative(PACKAGE_ROOT, file);

const toPosix = (p: string) => p.split(path.sep).join('/');

/**
 * Matches a path against the real `testMatch`, via the same glob library jest
 * uses. Deriving from the config rather than restating the pattern keeps this
 * honest when someone edits it; using micromatch rather than a hand-rolled
 * translation means the full glob grammar keeps working — sibling packages
 * already use brace and choice patterns this file would otherwise have to
 * reimplement.
 */
function isCollected(file: string): boolean {
  const patterns: string[] = jestConfig.testMatch;
  // Matched package-relative with posix separators, never as absolute paths.
  // micromatch reads `\` as an escape, so on win32 an absolute path would match
  // nothing at all — which fails asymmetrically: the "no source is collected"
  // assertion would pass vacuously while the counterpart one named every file.
  // Staying relative also keeps a glob metacharacter in the checkout path
  // (`/Users/me/cube (fork)/…`) from turning the pattern into a choice group.
  return micromatch.isMatch(
    toPosix(relativeToPackage(file)),
    patterns.map(p => toPosix(p.replace('<rootDir>/', '')))
  );
}

/** `test/foo/bar.test.ts` -> `dist/test/foo/bar.test.js` */
function compiledCounterpart(sourceFile: string): string {
  const relative = path.relative(PACKAGE_ROOT, sourceFile);
  return path.join(PACKAGE_ROOT, 'dist', relative).replace(/\.[^.]+$/, '.js');
}

describe('test collection', () => {
  test('testMatch is configured', () => {
    // Without it jest falls back to its default, which collects the sources —
    // and there is no transform, so each one dies with
    // `SyntaxError: Cannot use import statement outside a module`.
    expect(Array.isArray(jestConfig.testMatch)).toBe(true);
  });

  test('no config key narrows collection behind testMatch', () => {
    // The assertions below reason from `testMatch` alone, so a key that skips a
    // file jest would otherwise collect — the usual "temporarily ignore the
    // flaky suite" edit — would slip a silently-inert test past them.
    // `testRegex` is included because it is mutually exclusive with `testMatch`:
    // setting it makes jest throw rather than narrow, which is a different
    // failure to reason about and better named here than discovered.
    //
    // This reaches config only. A positional `testPathPattern` on the command
    // line narrows collection the same way and is invisible from here — see the
    // `unit` script — so this guard bounds the config, not every route in.
    const narrowingKeys = ['testPathIgnorePatterns', 'modulePathIgnorePatterns', 'roots', 'testRegex']
      .filter(key => jestConfig[key] !== undefined);

    expect(narrowingKeys).toEqual([]);
  });

  test('testMatch targets the compiled output, not the sources', () => {
    const collectedSources = SOURCE_TEST_FILES.filter(isCollected).map(relativeToPackage);

    expect(collectedSources).toEqual([]);
  });

  test('every test file has a compiled counterpart that is collected', () => {
    // Guards the guard: an empty walk would make the assertion below vacuous
    // and keep passing after the suite was deleted.
    expect(SOURCE_TEST_FILES.length).toBeGreaterThan(5);

    const uncollected = SOURCE_TEST_FILES
      .filter(file => {
        const compiled = compiledCounterpart(file);
        return !fs.existsSync(compiled) || !isCollected(compiled);
      })
      .map(relativeToPackage);

    expect(uncollected).toEqual([]);
  });

  test('no collected test has lost its source', () => {
    // The reverse direction, and the likelier one day to day: `tsc` is
    // incremental and only `build` does `rm -rf dist`, so renaming or deleting a
    // test leaves its old compiled copy behind, collected forever, running code
    // whose source no longer exists. That is this file's own subject mirrored —
    // a test running that nobody can see.
    const expected = new Set(SOURCE_TEST_FILES.map(compiledCounterpart));
    const orphans = sourceTestFiles(path.join(PACKAGE_ROOT, 'dist'))
      .filter(file => isCollected(file) && !expected.has(file))
      .map(relativeToPackage);

    expect(orphans).toEqual([]);
  });
});
