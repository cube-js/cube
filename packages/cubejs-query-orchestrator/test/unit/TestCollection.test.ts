import fs from 'fs';
import path from 'path';

// No @types/micromatch in the tree, so require it rather than import.
// eslint-disable-next-line global-require
const micromatch = require('micromatch');

// eslint-disable-next-line import/no-dynamic-require
const jestConfig = require('../../jest.config');

const TEST_ROOT = path.join(__dirname, '..');
const PACKAGE_ROOT = path.join(__dirname, '..', '..');

function testFilesOnDisk(dir: string): string[] {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      return testFilesOnDisk(full);
    }
    // `.abstract.ts` files hold shared suites imported by real test files; they
    // are deliberately not collected.
    return /\.test\.[tj]s$/.test(entry.name) ? [full] : [];
  });
}

/**
 * Matches a path against the real `testMatch`, via the same glob library jest
 * uses. A hand-rolled translation gets this wrong in a way that is worse than
 * useless: rendering `/**\/` as `/.*\/` demands an intervening directory, so
 * `test/Smoke.test.ts` — which jest *does* collect, since micromatch's `**`
 * matches zero segments — would be reported as uncollected and fail CI with a
 * misleading message. Deriving from the config rather than restating the
 * pattern also keeps this honest when someone edits it.
 */
function isCollected(file: string): boolean {
  const patterns: string[] = jestConfig.testMatch;
  return micromatch.isMatch(file, patterns.map(p => p.replace('<rootDir>', PACKAGE_ROOT)));
}

describe('test collection', () => {
  // `QueryOrchestrator.test.js` silently stopped running for ~10 months because
  // the shared ts-jest base matches `*.test.ts` only. Assert the config collects
  // every test file on disk, so dropping one fails CI instead of going unnoticed.
  test('every test file on disk is matched by testMatch', () => {
    expect(Array.isArray(jestConfig.testMatch)).toBe(true);

    const onDisk = testFilesOnDisk(TEST_ROOT);

    // Guards the guard: if the walk finds nothing, the assertion below is
    // vacuously true and would keep passing after the suite was deleted.
    expect(onDisk.length).toBeGreaterThan(0);

    const uncollected = onDisk
      .filter(file => !isCollected(file))
      .map(file => path.relative(PACKAGE_ROOT, file));

    expect(uncollected).toEqual([]);
  });

  // Pins the matcher against the hand-rolled translation this guard used to
  // carry: a test added directly under `test/` is collected by jest, so the
  // guard must agree rather than fail CI naming a file that does run.
  test.each([
    ['test/unit/QueryOrchestrator.test.js', true],
    ['test/unit/QueryCache.test.ts', true],
    ['test/Smoke.test.ts', true],
    ['test/unit/QueryCache.abstract.ts', false],
    ['src/orchestrator/QueryCache.ts', false],
  ])('%s is collected: %s', (relative, expected) => {
    expect(isCollected(path.join(PACKAGE_ROOT, relative))).toBe(expected);
  });
});
