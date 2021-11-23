import path from 'path';

import chalk from 'chalk';
import type { CommanderStatic } from 'commander';

import { displayError, event } from '../utils';
import { Config } from '../config';
import { PostgresConnectionTest } from './doctor/PostgresConnectionTest';
import { MySqlConnectionTest } from './doctor/MySqlConnectionTest';
import { ClickHouseConnectionTest } from './doctor/ClickHouseConnectionTest';

const DB_MAP = {
  clickhouse: ClickHouseConnectionTest,
  mysql: MySqlConnectionTest,
  postgres: PostgresConnectionTest,
};

const info = (msg) => process.env.LOG_LEVEL === 'info' && console.info(chalk.green(msg));
const success = (msg) => console.info(chalk.blue(msg));
const error = (msg) => console.error(chalk.red(msg));

const CURRENT_PROJECT = path.resolve(process.cwd(), '.env');

const testConnection = async () => {
  const config = await new Config().envFile(CURRENT_PROJECT);

  if (!config) {
    throw new Error('Could not find .env file for project');
  }

  const dbType = config.CUBEJS_DB_TYPE;

  info(`Database type detected as "${dbType}"`);

  const options = {
    host: config.CUBEJS_DB_HOST,
    database: config.CUBEJS_DB_NAME,
    port: config.CUBEJS_DB_PORT,
    user: config.CUBEJS_DB_USER,
    password: config.CUBEJS_DB_PASS,
  };
  const ResolvedConnectionTest = DB_MAP[dbType];

  if (!ResolvedConnectionTest) {
    error('The doctor is not yet qualified to diagnose this database, PRs are welcome');
    process.exit(1);
  }

  info('Initialising driver...');
  const connectionTest = new ResolvedConnectionTest();
  const driverInstance = await connectionTest.createDriver(options);

  try {
    info('Testing connection...');
    await driverInstance.testConnection();
    success('🚀 Successfully connected to the database');
  } catch (e) {
    if ('code' in e) {
      let msg = '';

      if (e.code === 'ECONNREFUSED') {
        msg = [
          'The connection was refused, this might be caused by',
          '',
          '  - The port number is incorrect'
        ].join('\n');
      }

      if (e.code === 'ETIMEDOUT') {
        msg = [
          'The connection timed out, this might be caused by',
          '',
          '  - The database is unavailable ',
          '  - The port number is incorrect'
        ].join('\n');
      }

      if (e.code === 'ENOTFOUND') {
        msg = [
          `A connection to the host "${options.host}" could not be made, this might be caused by`,
          '',
          '  - The host is temporarily unavailable',
          '  - The host is incorrect',
        ].join('\n');
      }

      msg = connectionTest.handleErrors(e, options, msg);

      console.error(e);
      error(msg);
      return;
    }

    console.error(e);

  } finally {
    info('Cleaning up resources...');
    await driverInstance.release();
  }

  // console.log(config);

  // await event({
  //   event: 'Cube CLI Doctor'
  // });

  // console.log('Token successfully added!');
};

export function configureDoctorCommand(program: CommanderStatic): void {
  program
    .command('doctor')
    .description('Test database connection')
    .action(
      () => testConnection()
        .catch(e => displayError(e.stack || e)),
    )
    .on('--help', () => {
      console.log('');
      console.log('Examples:');
      console.log('');
      console.log('  $ cubejs doctor');
    });
}
