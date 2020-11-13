import fs from 'fs-extra';
import path from 'path';
import { CommanderStatic } from 'commander';
import { displayError, event, isDockerImage, packageExists, requireFromPackage } from '../utils';

// @todo There is another function with similar name inside utils, but without analytics
const logStage = (stage) => {
  console.log(`- ${stage}`);
};

const generate = async (options) => {
  const generateSchemaOptions = { tables: options.tables };
  event('Generate Schema', generateSchemaOptions);
  if (!options.tables) {
    await displayError([
      'You must pass table names to generate schema from (-t).',
      '',
      'Example: ',
      ' $ cubejs generate -t orders,customers'
    ], generateSchemaOptions);
  }

  const relativeResolution = isDockerImage();
  if (!packageExists('@cubejs-backend/server', relativeResolution)) {
    await displayError(
      '@cubejs-backend/server dependency not found. Please run generate command from project directory.',
      generateSchemaOptions
    );
  }

  logStage('Fetching DB schema');
  const CubejsServer = await requireFromPackage(
    '@cubejs-backend/server',
    relativeResolution
  );
  const driver = await CubejsServer.createDriver();
  await driver.testConnection();
  const dbSchema = await driver.tablesSchema();
  if (driver.release) {
    await driver.release();
  }

  logStage('Generating schema files');
  const ScaffoldingTemplate = await requireFromPackage(
    '@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate.js',
    relativeResolution
  );
  const scaffoldingTemplate = new ScaffoldingTemplate(dbSchema, driver);
  const files = scaffoldingTemplate.generateFilesByTableNames(options.tables);
  await Promise.all(files.map(file => fs.writeFile(path.join('schema', file.fileName), file.content)));

  await event('Generate Schema Success', generateSchemaOptions);
  logStage(`Schema for ${options.tables.join(', ')} was successfully generated 🎉`);
};

const list = (val) => val.split(',');

export function configureGenerateCommand(program: CommanderStatic) {
  program
    .command('generate')
    .option('-t, --tables <tables>', 'Comma delimited list of tables to generate schema from', list)
    .description('Generate Cube.js schema from DB tables schema')
    .action(
      (options) => generate(options)
        .catch(e => displayError(e.stack || e, { dbType: options.dbType }))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs generate -t orders,customers');
    });
}
