/**
 * Base config for packages that test their COMPILED output. It declares no
 * `preset` and no `transform`, so the TypeScript under `test/` cannot execute
 * here — run `yarn tsc` first, and expect suites to run from `dist/test`.
 *
 * `testMatch` is deliberately left to each package rather than set here:
 * several of them keep integration suites next to their unit ones and split the
 * two with the path argument in their `unit` / `integration` scripts. A pattern
 * hoisted into this file would apply to packages that have no such argument,
 * enrolling suites that need live warehouse credentials.
 *
 * So a package extending this should declare its own `testMatch` pointing into
 * `dist/`. Without one, jest's default picks up the untransformable sources
 * under `test/` and a bare `jest` fails on `import` — the path argument in the
 * npm script is then the only thing keeping the suite runnable.
 *
 * @type {import('jest').Config}
 */
module.exports = {
  testEnvironment: 'node',
  collectCoverage: true,
  coverageDirectory: 'coverage/',
  coverageReporters: ['text', 'html', 'lcov'],
  coveragePathIgnorePatterns: ['.*\\.d\\.ts'],
  collectCoverageFrom: [
    'dist/src/**/*.js',
    'dist/src/**/*.ts',
  ],
  moduleDirectories: ['node_modules', '<rootDir>/node_modules'],
  moduleNameMapper: {
    // Force module uuid to resolve with the CJS entry point, because Jest does not support package.json.exports.
    // @See https://github.com/uuidjs/uuid/issues/451
    '^uuid$': require.resolve('uuid'),
    '^yaml$': require.resolve('yaml'),
    '^antlr4$': require.resolve('antlr4'),
  },
  setupFiles: ['../../jest.setup.js'],
  snapshotFormat: {
    escapeString: true, // To keep existing variant of snapshots
    printBasicPrototype: true
  }
};
