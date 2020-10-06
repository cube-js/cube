import {Command, flags} from '@oclif/command'

import fs from 'fs-extra';
import chalk from 'chalk';
import crypto from 'crypto';
import path from 'path';
import inquirer from 'inquirer';

import { displayError, event, requireFromPackage } from '../utils';
import { logStage } from '../logger';
import { executeCommand, npmInstall, writePackageJson } from '../packages';
import templates from '../templates';

export class Create extends Command {
  static description = 'Create new Cube.js app';

  static flags = {
    dbType: flags.string({
      name: 'db-type',
      char: 'd',
      description: (
        'Preconfigure for selected database.\n\t\t\t' +
        'Options: postgres, mysql, mongobi, athena, redshift, bigquery, mssql, clickhouse, snowflake, presto'
      )
    }),
    template: flags.string({
      name: 'template',
      char: 't',
      description: (
        'Preconfigure for selected database.\n\t\t\t' +
        'Options: postgres, mysql, mongobi, athena, redshift, bigquery, mssql, clickhouse, snowflake, presto'
      )
    }),
  }

  static args = [
    { name: 'name' }
  ];

  protected async getDBDriver(dbType?: string) {
    if (dbType) {
      return dbType;
    }

    const Drivers = await requireFromPackage('@cubejs-backend/server-core/core/DriverDependencies.js');
    const prompt = await inquirer.prompt([{
      type: 'list',
      name: 'dbType',
      message: 'Select database',
      choices: Object.keys(Drivers)
    }]);

    return prompt.dbType;
  }

  public async run() {
    const { args, flags } = this.parse(Create);

    const template = flags.template || 'express';
    const createAppOptions = { projectName: args.name, dbType: flags.dbType, template };

    event('Create App', createAppOptions);

    if (await fs.pathExists(args.name)) {
      await displayError(
        `We cannot create a project called ${chalk.green(
          args.name
        )}: directory already exist.\n`,
        createAppOptions
      );
    }

    if (!templates[template]) {
      await displayError(
        `Unknown template ${chalk.red(template)}`,
        createAppOptions
      );
    }

    await fs.ensureDir(args.name);
    process.chdir(args.name);

    const templateConfig = templates[template];

    logStage('Creating project structure');
    await writePackageJson({
      name: args.name,
      version: '0.0.1',
      private: true,
      scripts: templateConfig.scripts,
    });

    logStage('Installing server dependencies');
    await npmInstall(['@cubejs-backend/server']);

    const dbType = await this.getDBDriver(flags.dbType);

    logStage('Installing DB driver dependencies');
    const CubejsServer = await requireFromPackage('@cubejs-backend/server');
    let driverDependencies = CubejsServer.driverDependencies(dbType);
    if (!driverDependencies) {
      await displayError(`Unsupported db type: ${chalk.green(dbType)}`, createAppOptions);
    }

    driverDependencies = Array.isArray(driverDependencies) ? driverDependencies : [driverDependencies];
    if (driverDependencies[0] === '@cubejs-backend/jdbc-driver') {
      driverDependencies.push('node-java-maven');
    }
    await npmInstall(driverDependencies);

    if (driverDependencies[0] === '@cubejs-backend/jdbc-driver') {
      logStage('Installing JDBC dependencies');
      const JDBCDriver = require(path.join(process.cwd(), 'node_modules', '@cubejs-backend', 'jdbc-driver', 'driver', 'JDBCDriver'));
      const dbTypeDescription = JDBCDriver.dbTypeDescription(dbType);
      if (!dbTypeDescription) {
        await displayError(`Unsupported db type: ${chalk.green(dbType)}`, createAppOptions);
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

    const driverClass = await requireFromPackage(driverDependencies[0]);

    const env = {
      dbType,
      apiSecret: crypto.randomBytes(64).toString('hex'),
      projectName: args.name,
      driverEnvVariables: driverClass.driverEnvVariables && driverClass.driverEnvVariables()
    };
    await Promise.all(Object.keys(templateConfig.files).map(async fileName => {
      await fs.ensureDir(path.dirname(fileName));
      await fs.writeFile(fileName, templateConfig.files[fileName](env));
    }));

    if (templateConfig.dependencies) {
      logStage('Installing template dependencies');
      await npmInstall(templateConfig.dependencies);
    }

    if (templateConfig.devDependencies) {
      logStage('Installing template dev dependencies');
      await npmInstall(templateConfig.devDependencies);
    }

    await event('Create App Success', { projectName: args.name, dbType, });
    logStage(`${chalk.green(args.name)} app has been created 🎉`);

    console.log();
    console.log('📊 Next step: run dev server');
    console.log();
    console.log(`     $ cd ${args.name}`);
    console.log('     $ npm run dev');
    console.log();
  }
}
