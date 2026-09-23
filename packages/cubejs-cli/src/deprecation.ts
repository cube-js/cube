import chalk from 'chalk';

export const NEW_CLI_DOCS_URL = 'https://docs.cube.dev/reference/cli';
export const NEW_CLI_INSTALL_SH = 'curl -fsSL https://raw.githubusercontent.com/cube-js/cube/master/install-cli.sh | sh';
export const NEW_CLI_INSTALL_PS = 'irm https://raw.githubusercontent.com/cube-js/cube/master/install-cli.ps1 | iex';

// Legacy commands that have a direct counterpart in the new `cube` CLI.
export const COMMAND_REPLACEMENTS: Record<string, string> = {
  auth: 'cube login',
  deploy: 'cube deploy <deployment-id>',
};

export const isDeprecationWarningDisabled = (env: NodeJS.ProcessEnv = process.env) => {
  const value = (env.CUBEJS_CLI_NO_DEPRECATION_WARNING || '').trim().toLowerCase();
  return value !== '' && value !== 'false' && value !== '0';
};

export const deprecationMessage = (command?: string): string[] => {
  const lines = [
    `${chalk.yellow('Deprecation warning.')} The \`cubejs\` CLI (npm package \`cubejs-cli\`) is deprecated and will stop receiving updates.`,
    'Please switch to the new native Cube CLI (`cube`):',
    `  Linux / macOS:        ${NEW_CLI_INSTALL_SH}`,
    `  Windows (PowerShell): ${NEW_CLI_INSTALL_PS}`,
  ];

  const replacement = command && COMMAND_REPLACEMENTS[command];
  if (replacement) {
    lines.push(`Instead of \`cubejs ${command}\`, use \`${replacement}\`.`);
  }

  lines.push(`Learn more: ${NEW_CLI_DOCS_URL}`);
  lines.push('Set CUBEJS_CLI_NO_DEPRECATION_WARNING=true to hide this message.');

  return lines;
};

// Only warn where the new CLI actually replaces the legacy one: commands with
// a Cloud counterpart, plus bare `cubejs` / `--help`. Local commands such as
// `server` (the Docker image entrypoint), `create` or `token` have no
// equivalent in `cube`, so warning there would just spam logs.
export const shouldDisplayDeprecationWarning = (command?: string) => !command || !!COMMAND_REPLACEMENTS[command];

// Printed to stderr so commands whose stdout is consumed by scripts
// (e.g. `cubejs token`) keep their output intact.
export const displayDeprecationWarning = (argv: string[] = process.argv.slice(2)) => {
  const command = argv[0] && !argv[0].startsWith('-') ? argv[0] : undefined;
  if (isDeprecationWarningDisabled() || !shouldDisplayDeprecationWarning(command)) {
    return;
  }

  console.error('');
  deprecationMessage(command).forEach((line) => console.error(line));
  console.error('');
};
