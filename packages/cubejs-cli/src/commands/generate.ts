import { Command, flags } from '@oclif/command'
import { displayError, event, requireFromPackage } from '../utils';
import fs from 'fs-extra';
import path from 'path';
import { logStage } from '../logger';

export class Generate extends Command {
  static description = 'Generate Cube.js schema from DB tables schema';

  static flags = {
    tables: flags.string({
      name: 'tables',
      char: 't',
      description: (
        'Comma delimited list of tables to generate schema from'
      ),
      required: true,
      multiple: true,
    }),
  }

  static args = [
    { name: 'projectName' }
  ];

  public async run() {
    const { args, flags } = this.parse(Generate)

    const generateSchemaOptions = { tables: flags.tables };
    event('Generate Schema', generateSchemaOptions);

    if (!flags.tables) {
      await displayError([
        'You must pass table names to generate schema from (-t).',
        '',
        'Example: ',
        ' $ cubejs generate -t orders,customers'
      ], generateSchemaOptions);
    }

    if (!(await fs.pathExists(path.join(process.cwd(), 'node_modules', '@cubejs-backend/server')))) {
      await displayError(
        '@cubejs-backend/server dependency not found. Please run generate command from project directory.',
        generateSchemaOptions
      );
    }

    logStage('Fetching DB schema');
    const CubejsServer = await requireFromPackage('@cubejs-backend/server');
    const driver = await CubejsServer.createDriver();
    await driver.testConnection();
    const dbSchema = await driver.tablesSchema();

    if (driver.release) {
      await driver.release();
    }

    logStage('Generating schema files');
    const ScaffoldingTemplate = await requireFromPackage('@cubejs-backend/schema-compiler/scaffolding/ScaffoldingTemplate');
    const scaffoldingTemplate = new ScaffoldingTemplate(dbSchema, driver);
    const files = scaffoldingTemplate.generateFilesByTableNames(flags.tables);
    await Promise.all(files.map(file => fs.writeFile(path.join('schema', file.fileName), file.content)));

    await event('Generate Schema Success', generateSchemaOptions);
    logStage(`Schema for ${flags.tables.join(', ')} was successfully generated 🎉`);
  }
}
