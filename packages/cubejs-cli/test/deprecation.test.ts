import { deprecationMessage, isDeprecationWarningDisabled } from '../src/deprecation';

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
  });

  test('can be disabled via env', () => {
    expect(isDeprecationWarningDisabled({})).toBe(false);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: 'false' })).toBe(false);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: 'true' })).toBe(true);
    expect(isDeprecationWarningDisabled({ CUBEJS_CLI_NO_DEPRECATION_WARNING: '1' })).toBe(true);
  });
});
