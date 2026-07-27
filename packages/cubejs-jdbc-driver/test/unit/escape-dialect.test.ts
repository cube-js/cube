import { SupportedDrivers } from '../../src/supported-drivers';

describe('JDBC escape dialect', () => {
  it('declares a dialect for every supported engine', () => {
    for (const [name, options] of Object.entries(SupportedDrivers)) {
      expect([name, options.escapeDialect]).toEqual([name, expect.stringMatching(/^(ansi|mysql)$/)]);
    }
  });
});
