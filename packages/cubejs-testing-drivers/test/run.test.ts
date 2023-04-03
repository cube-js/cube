import yargs from 'yargs/yargs';
import { testConnection } from '../src/tests/testConnection';
import { testQueries } from '../src/tests/testQueries';
import { testSequence } from '../src/tests/testSequence';

type Drivers =
  | 'athena'
  | 'clickhouse'
  | 'databricks-jdbc'
  | 'mssql'
  | 'postgres';
type Suites =
  | 'driver'
  | 'scheduler'
  | 'integration';
type Args = {
  driver: Drivers,
  suite: Suites,
};

const args: Args = <Args>yargs(process.argv.slice(2))
  .exitProcess(false)
  .options({
    driver: {
      describe: 'Driver to run tests for.',
      choices: [
        'athena',
        'clickhouse',
        'databricks-jdbc',
        'mssql',
        'postgres',
      ],
      default: 'postgres',
    },
    suite: {
      describe: 'Test suite to run.',
      choices: [
        'driver',
        'scheduler',
        'integration',
      ],
      default: 'driver',
    }
  })
  .argv;

switch (args.suite) {
  default:
  case 'driver':
    testConnection(args.driver);
    break;
  case 'scheduler':
    testSequence(args.driver);
    break;
  case 'integration':
    testQueries(args.driver);
    break;
}
