import {
  deprecationMessage,
  displayDeprecationWarning,
  isDeprecationWarningDisabled,
  shouldDisplayDeprecationWarning,
} from '../src/deprecation';

describe('deprecation', () => {
  test('routes users to the new Cube CLI', () => {
    const message = deprecationMessage().join('\n');

    expect(message).toContain('deprecated');
    expect(message).toContain('install-cli.sh');
    expect(message).toContain('install-cli.ps1');
    expect(message).toContain('https://docs.cube.dev/reference/cli');
  });

  test('suggests a replacement for commands that have one', () => {
    expect(deprecationMessage('deploy').join('\n')).toContain('cube deploy <deployment-id>');
    expect(deprecationMessage('auth').join('\n')).toContain('cube login');
    expect(deprecationMessage('create').join('\n')).not.toContain('Instead of');
    expect(deprecationMessage('toString').join('\n')).not.toContain('Instead of');
  });

  test('only warns for commands replaced by the new CLI', () => {
    expect(shouldDisplayDeprecationWarning(undefined)).toBe(true);
    expect(shouldDisplayDeprecationWarning('deploy')).toBe(true);
    expect(shouldDisplayDeprecationWarning('auth')).toBe(true);
    expect(shouldDisplayDeprecationWarning('server')).toBe(false);
    expect(shouldDisplayDeprecationWarning('create')).toBe(false);
    expect(shouldDisplayDeprecationWarning('token')).toBe(false);
    expect(shouldDisplayDeprecationWarning('toString')).toBe(false);
    expect(shouldDisplayDeprecationWarning('constructor')).toBe(false);
  });

  test('can be disabled via env', () => {
    expect(isDeprecationWarningDisabled({})).toBe(false);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: 'false' })).toBe(false);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: 'true' })).toBe(true);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: '1' })).toBe(true);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: 'False' })).toBe(false);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: '0 ' })).toBe(false);
  });

  describe('displayDeprecationWarning', () => {
    let errorSpy: jest.SpyInstance;
    let logSpy: jest.SpyInstance;
    const originalEnv = process.env.CUBEJS_CLI_NO_DEPRECATION_WARNING;

    beforeEach(() => {
      delete process.env.CUBEJS_CLI_NO_DEPRECATION_WARNING;
      errorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
      logSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    });

    afterEach(() => {
      errorSpy.mockRestore();
      logSpy.mockRestore();
      if (originalEnv === undefined) {
        delete process.env.CUBEJS_CLI_NO_DEPRECATION_WARNING;
      } else {
        process.env.CUBEJS_CLI_NO_DEPRECATION_WARNING = originalEnv;
      }
    });

    const printed = () => errorSpy.mock.calls.map((args) => args.join(' ')).join('\n');

    test('prints to stderr only, with the replacement for the command', () => {
      displayDeprecationWarning(['deploy', '--directory', '.']);

      expect(printed()).toContain('cube deploy <deployment-id>');
      expect(logSpy).not.toHaveBeenCalled();
    });

    test('prints for bare invocation and --help', () => {
      displayDeprecationWarning([]);
      displayDeprecationWarning(['--help']);

      expect(printed()).toContain('deprecated');
      expect(printed()).not.toContain('Instead of');
    });

    test('stays silent for commands without a replacement', () => {
      displayDeprecationWarning(['server']);
      displayDeprecationWarning(['token', '-p', 'deploy=1']);

      expect(errorSpy).not.toHaveBeenCalled();
      expect(logSpy).not.toHaveBeenCalled();
    });

    test('stays silent when disabled via env', () => {
      process.env.CUBEJS_CLI_NO_DEPRECATION_WARNING = 'true';
      displayDeprecationWarning(['deploy']);

      expect(errorSpy).not.toHaveBeenCalled();
    });
  });
});
