import fs from 'fs-extra';
import path from 'path';
import { CommanderStatic } from 'commander';
import { isDockerImage, requireFromPackage, packageExists } from '@cubejs-backend/shared';
import type { ServerContainer as ServerContainerType } from '@cubejs-backend/server';

import { displayError, event } from '../utils';

// @todo There is another function with similar name inside utils, but without analytics
const logStage = (stage) => {
  console.log(`- ${stage}`);
};

const generate = async (options) => {
  const generateSchemaOptions = { tables: options.tables };

  event({
    event: 'Generate Schema',
    ...generateSchemaOptions,
  });

  if (!options.tables) {
    await displayError([
      'You must pass table names to generate schema from (-t).',
      '',
      'Example: ',
      ' $ cubejs generate -t orders,customers'
    ], generateSchemaOptions);
  }

  const relative = isDockerImage();

  if (!packageExists('@cubejs-backend/server', relative)) {
    await displayError(
      '@cubejs-backend/server dependency not found. Please run generate command from project directory.',
      generateSchemaOptions
    );
  }

  logStage('Fetching DB schema');
  const serverPackage = requireFromPackage<{ ServerContainer: any }>(
    '@cubejs-backend/server',
    {
      relative,
    }
  );

  if (!serverPackage.ServerContainer) {
    await displayError(
      '@cubejs-backend/server is too old. Please use @cubejs-backend/server >= v0.26.11',
      generateSchemaOptions
    );
  }

  const container: ServerContainerType = new serverPackage.ServerContainer({ debug: false });
  const configuration = await container.lookupConfiguration();
  const server = await container.runServerInstance(
    configuration,
    true,
    Object.keys(configuration).length === 0
  );

  const driver = await server.getDriver({
    dataSource: options.dataSource,
    authInfo: null,
    securityContext: null,
    requestId: 'CLI REQUEST'
  }, {
    poolSize: 1, // TODO (buntarb): check this point!
  });

  const dbSchema = await driver.tablesSchema();
  await driver.release();

  logStage('Generating schema files');
  const ScaffoldingTemplate = requireFromPackage<any>(
    '@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate.js',
    {
      relative,
    }
  );
  const scaffoldingTemplate = new ScaffoldingTemplate(dbSchema, driver);
  const { tables, dataSource } = options;
  const files = scaffoldingTemplate.generateFilesByTableNames(tables, { dataSource });
  await Promise.all(files.map(file => fs.writeFile(path.join('schema', file.fileName), file.content)));

  await event({
    event: 'Generate Schema Success',
    ...generateSchemaOptions
  });

  logStage(`Schema for ${options.tables.join(', ')} was successfully generated 🎉`);
};

const list = (val) => val.split(',');

export function configureGenerateCommand(program: CommanderStatic) {
  program
    .command('generate')
    .option('-t, --tables <tables>', 'Comma delimited list of tables to generate schema from', list)
    .option('-d, --dataSource <dataSource>', '', 'default')
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
