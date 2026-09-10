import fs from 'fs-extra';
import chalk from 'chalk';
import inquirer from 'inquirer';
import path from 'path';
import crypto from 'crypto';
import { CommanderStatic } from 'commander';
import { requireFromPackage, requirePackageManifest } from '@cubejs-backend/shared';

import {
  displayError,
  executeCommand, findMaxVersion,
  loadCliManifest,
  npmInstall,
  writePackageJson,
  event,
} from '../utils';
import templates from '../templates';

// @todo There is another function with similar name inside utils, but without analytics
const logStage = (stage) => {
  console.log(`- ${stage}`);
};

const create = async (projectName, options) => {
  const createAppOptions = { projectName, dbType: options.dbType, template: 'docker' };

  event({
    event: 'Create App',
    ...createAppOptions,
  });

  if (await fs.pathExists(projectName)) {
    await displayError(
      `We cannot create a project called ${chalk.green(
        projectName
      )}: directory already exist.\n`,
      createAppOptions
    );
  }

  const templateConfig = templates.docker;

  await fs.ensureDir(projectName);
  process.chdir(projectName);

  const cliManifest = loadCliManifest();

  logStage('Creating project structure');
  await writePackageJson({
    name: projectName,
    version: '0.0.1',
    private: true,
    scripts: templateConfig.scripts,
    template: 'docker',
    templateVersion: cliManifest.version,
  });

  logStage('Installing server dependencies');
  await npmInstall(['@cubejs-backend/server'], true);

  if (!options.dbType) {
    const Drivers = requireFromPackage<any>('@cubejs-backend/server-core/dist/src/core/DriverDependencies.js');
    const prompt = await inquirer.prompt([{
      type: 'list',
      name: 'dbType',
      message: 'Select database',
      choices: Object.keys(Drivers)
    }]);

    options.dbType = prompt.dbType;
  }

  logStage('Installing DB driver dependencies');
  const CubejsServer = requireFromPackage<any>('@cubejs-backend/server');

  const driverPackageName = CubejsServer.driverDependencies(options.dbType);
  if (!driverPackageName) {
    await displayError(`Unsupported db type: ${chalk.green(options.dbType)}`, createAppOptions);
  }

  await npmInstall([driverPackageName], true);

  if (driverPackageName === '@cubejs-backend/jdbc-driver') {
    logStage('Installing JDBC dependencies');

    // eslint-disable-next-line import/no-dynamic-require,global-require,@typescript-eslint/no-var-requires
    const JDBCDriver = require(path.join(process.cwd(), 'node_modules', '@cubejs-backend', 'jdbc-driver'));

    const { jdbcDriver } = await inquirer.prompt([{
      type: 'list',
      name: 'jdbcDriver',
      message: 'Select JDBC driver',
      choices: JDBCDriver.getSupportedDrivers(),
    }]);

    const dbTypeDescription = JDBCDriver.dbTypeDescription(
      jdbcDriver
    );
    if (!dbTypeDescription) {
      await displayError(`Unsupported JDBC driver: ${chalk.green(jdbcDriver)}`, createAppOptions);
    }

    const newPackageJson = await fs.readJson('package.json');
    if (dbTypeDescription.mavenDependency) {
      newPackageJson.java = {
        dependencies: [dbTypeDescription.mavenDependency]
      };
    }
    newPackageJson.scripts = newPackageJson.scripts || {};
    newPackageJson.scripts.install = './node_modules/.bin/node-java-maven';
    await writePackageJson(newPackageJson);

    await executeCommand('npm', ['install']);
  }

  logStage('Writing files from template');

  const driverClass = requireFromPackage<any>(driverPackageName);

  const driverPackageManifest = await requirePackageManifest(driverPackageName);
  const serverCorePackageManifest = await requirePackageManifest('@cubejs-backend/server-core');
  const serverPackageManifest = await requirePackageManifest('@cubejs-backend/server');

  const dockerVersion = findMaxVersion([
    serverPackageManifest.version,
    serverCorePackageManifest.version,
    driverPackageManifest.version
  ]);

  const env = {
    dbType: options.dbType,
    apiSecret: crypto.randomBytes(64).toString('hex'),
    dockerVersion: `v${dockerVersion.version}`,
    driverEnvVariables: driverClass.driverEnvVariables && driverClass.driverEnvVariables()
  };

  await Promise.all(Object.keys(templateConfig.files).map(async fileName => {
    await fs.ensureDir(path.dirname(fileName));
    await fs.writeFile(fileName, templateConfig.files[fileName](env));
  }));

  await event({
    event: 'Create App Success',
    projectName,
    dbType: options.dbType
  });

  logStage(`${chalk.green(projectName)} app has been created 🎉`);

  console.log();
  console.log('📊 Next step: run dev server');
  console.log();
  console.log(`     $ cd ${projectName}`);
  console.log('     $ npm run dev');
  console.log();
};

export function configureCreateCommand(program: CommanderStatic) {
  program
    .command('create <name>')
    .option(
      '-d, --db-type <db-type>',
      'Preconfigure for selected database.\n\t\t\t     ' +
      'Options: postgres, mysql, mongobi, athena, redshift, bigquery, mssql, clickhouse, snowflake, presto, questdb, materialize, firebolt'
    )
    .description('Create new Cube app')
    .action(
      (projectName, options) => create(projectName, options)
        .catch(e => displayError(e.stack || e, { projectName, dbType: options.dbType }))
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs create hello-world -d postgres');
    });
}
