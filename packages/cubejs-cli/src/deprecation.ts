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
  const value = env.CUBEJS_CLI_NO_DEPRECATION_WARNING;
  return !!value && value !== 'false' && value !== '0';
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

// Printed to stderr so commands whose stdout is consumed by scripts
// (e.g. `cubejs token`) keep their output intact.
export const displayDeprecationWarning = (argv: string[] = process.argv.slice(2)) => {
  if (isDeprecationWarningDisabled()) {
    return;
  }

  const command = argv.find((arg) => !arg.startsWith('-'));

  console.error('');
  deprecationMessage(command).forEach((line) => console.error(line));
  console.error('');
};
