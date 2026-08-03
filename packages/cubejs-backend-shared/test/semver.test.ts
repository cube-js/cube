import { isVersionGte } from '../src';

describe('isVersionGte', () => {
  test.each([
    // null version
    [null, '1.0.0', false],
    // equal versions
    ['1.6.22', '1.6.22', true],
    ['0.0.0', '0.0.0', true],
    // greater (major, minor, patch)
    ['2.0.0', '1.6.22', true],
    ['1.7.0', '1.6.22', true],
    ['1.6.23', '1.6.22', true],
    // lesser (major, minor, patch)
    ['0.9.99', '1.0.0', false],
    ['1.5.99', '1.6.0', false],
    ['1.6.21', '1.6.22', false],
    // different segment lengths
    ['1.6', '1.6.0', true],
    ['1.6.0', '1.6', true],
    ['1.5', '1.6.0', false],
    // pre-release is less than the same clean version
    ['1.6.22-alpha', '1.6.22', false],
    ['1.6.22-beta', '1.6.22', false],
    ['1.6.22-rc.1', '1.6.22', false],
    // pre-release of a higher version still passes
    ['1.6.23-alpha', '1.6.22', true],
    ['2.0.0-beta', '1.6.22', true],
    // clean version is greater than pre-release
    ['1.6.22', '1.6.22-alpha', true],
    // both pre-release, same numeric — equal
    ['1.6.22-alpha', '1.6.22-beta', true],
  ])('isVersionGte(%j, %j) === %j', (version, minVersion, expected) => {
    expect(isVersionGte(version, minVersion)).toBe(expected);
  });
});
