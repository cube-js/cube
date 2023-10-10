import fs from 'fs-extra';
import path from 'path';
import { CommanderStatic } from 'commander';
import { FileRepository, getEnv } from '@cubejs-backend/shared';
import { compile } from '@cubejs-backend/schema-compiler';

import { displayError } from '../utils';

async function validate(options) {
  const schemaPath = options.schemaPath || getEnv('schemaPath');

  if (!fs.existsSync(path.join(process.cwd(), schemaPath))) {
    displayError(`Schema path not found at "${path.join(process.cwd(), schemaPath)}". Please run validate command from project directory.`);
    return;
  }

  try {
    const repo = new FileRepository(schemaPath);
    await compile(repo, {
      allowNodeRequire: true,
    });
  } catch (error: any) {
    console.log('❌ Cube Schema validation failed');
    displayError(error.messages);
    return;
  }

  console.log('✅ Cube Schema is valid');
}

export function configureValidateCommand(program: CommanderStatic) {
  program
    .command('validate')
    .option(
      '-p, --schema-path <schema-path>',
      'Path to schema files. Default: schema'
    )
    .description('Validate Cube schema')
    .action(
      (options) => validate(options)
        .catch(error => displayError(error))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs validate');
    });
}
