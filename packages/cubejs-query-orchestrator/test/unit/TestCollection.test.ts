import fs from 'fs';
import path from 'path';

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
 * Translates a jest `testMatch` glob into a RegExp. Only the constructs the
 * patterns in this repo actually use are supported (`**`, `*`, `<rootDir>`),
 * so an unrecognised pattern fails loudly rather than silently matching
 * nothing — a guard that quietly passes is worse than no guard.
 */
function globToRegExp(pattern: string): RegExp {
  const withRoot = pattern.replace('<rootDir>', PACKAGE_ROOT);
  if (/[?[\]{}()!+@]/.test(withRoot)) {
    throw new Error(`Unsupported glob construct in testMatch pattern: ${pattern}`);
  }

  const source = withRoot
    .split('**')
    .map(segment => segment.split('*').map(part => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*'))
    .join('.*');

  return new RegExp(`^${source}$`);
}

describe('test collection', () => {
  // `QueryOrchestrator.test.js` silently stopped running for ~10 months because
  // the shared ts-jest base matches `*.test.ts` only. Assert the config collects
  // every test file on disk, so dropping one fails CI instead of going unnoticed.
  test('every test file on disk is matched by testMatch', () => {
    const patterns: string[] = jestConfig.testMatch;
    expect(patterns).toBeDefined();

    const matchers = patterns.map(globToRegExp);
    const onDisk = testFilesOnDisk(TEST_ROOT);

    // Guards the guard: if the walk finds nothing, the assertion below is
    // vacuously true and would keep passing after the suite was deleted.
    expect(onDisk.length).toBeGreaterThan(0);

    const uncollected = onDisk
      .filter(file => !matchers.some(matcher => matcher.test(file)))
      .map(file => path.relative(PACKAGE_ROOT, file));

    expect(uncollected).toEqual([]);
  });
});
