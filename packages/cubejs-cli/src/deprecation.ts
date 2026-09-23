export const NEW_CLI_DOCS_URL = 'https://docs.cube.dev/reference/cli';
export const NEW_CLI_INSTALL_SH = 'curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh';
export const NEW_CLI_INSTALL_PS = 'irm https://raw.githubusercontent.com/cube-js/cube/master/install-cli.ps1 | iex';

// Legacy commands that have a direct counterpart in the new `cube` CLI.
export const COMMAND_REPLACEMENTS: ReadonlyMap<string, string> = new Map([
  ['auth', 'cube login'],
  ['deploy', 'cube deploy <deployment-id>'],
]);

export const isDeprecationWarningDisabled = (env: NodeJS.ProcessEnv = process.env) => {
  const value = (env.CUBEJS_CLI_NO_DEPRECATION_WARNING || '').trim().toLowerCase();
  return value !== '' && value !== 'false' && value !== '0';
};

export const deprecationMessage = (command?: string): string[] => {
  const lines = [
    'Deprecation warning. The `cubejs` CLI (npm package `cubejs-cli`) is deprecated and will stop receiving updates.',
    'Please switch to the new native Cube CLI (`cube`):',
    `  Linux / macOS:        ${NEW_CLI_INSTALL_SH}`,
    `  Windows (PowerShell): ${NEW_CLI_INSTALL_PS}`,
  ];

  const replacement = command && COMMAND_REPLACEMENTS.get(command);
  if (replacement) {
    lines.push(`Instead of \`cubejs ${command}\`, use \`${replacement}\`.`);
  }

  lines.push(`Learn more: ${NEW_CLI_DOCS_URL}`);
  lines.push('Set CUBEJS_CLI_NO_DEPRECATION_WARNING=true to hide this message.');

  return lines;
};

const HELP_FLAGS = new Set(['--help', '-h']);

const commandFrom = (argv: string[]) => (argv[0] && !argv[0].startsWith('-') ? argv[0] : undefined);

// `server` is the Docker image entrypoint, so warning on commands without a
// `cube` counterpart would put the banner in every container's logs. Other
// flag-only runs (e.g. `--version`) stay silent too, since version probes
// often capture stderr.
export const shouldDisplayDeprecationWarning = (argv: string[]) => !argv.length ||
  HELP_FLAGS.has(argv[0]) ||
  COMMAND_REPLACEMENTS.has(commandFrom(argv) ?? '');

// Printed to stderr so the command's own stdout (e.g. `cubejs deploy`)
// stays intact for scripts that consume it.
export const displayDeprecationWarning = (argv: string[] = process.argv.slice(2)) => {
  if (isDeprecationWarningDisabled() || !shouldDisplayDeprecationWarning(argv)) {
    return;
  }

  console.error('');
  deprecationMessage(commandFrom(argv)).forEach((line) => console.error(line));
  console.error('');
};
