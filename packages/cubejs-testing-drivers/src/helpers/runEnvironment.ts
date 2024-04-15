import * as fs from 'fs-extra';
import * as path from 'path';
import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import { config } from 'dotenv';
import yargs from 'yargs/yargs';
import { DockerComposeEnvironment, Wait } from 'testcontainers';
import { isCI, pausePromise } from '@cubejs-backend/shared';
import { getFixtures } from './getFixtures';
import { getTempPath } from './getTempPath';
import { getComposePath } from './getComposePath';
import { getCubeJsPath } from './getCubeJsPath';
import { getPackageJsonPath } from './getPackageJsonPath';
import { getSchemaPath } from './getSchemaPath';
import { Environment } from '../types/Environment';

interface CubeEnvironment {
  withStartupTimeout(startupTimeout: number): this;
  withEnvironment(environment: { [key in string]: string; }): this;
  up(): Promise<void>;
  down(): Promise<void>;
}

class CubeCliEnvironment implements CubeEnvironment {
  public cli: ChildProcessWithoutNullStreams | null = null;

  private env: any = {};

  public constructor(private dir: string) {
  }

  public async up(): Promise<void> {
    try {
      this.cli = spawn(
        path.resolve(process.cwd(), '../cubejs-server/bin/server'),
        [],
        {
          cwd: this.dir,
          shell: true,
          detached: true,
          stdio: [
            'pipe',
            'pipe',
            'pipe',
          ],
          env: {
            ...process.env, ...this.env
          },
        }
      );
      if (this.cli.stdout) {
        this.cli.stdout.on('data', (msg) => {
          process.stdout.write(msg);
        });
      }
      if (this.cli.stderr) {
        this.cli.stderr.on('data', (msg) => {
          process.stdout.write(msg);
        });
      }
      await pausePromise(10 * 1000);
    } catch (e) {
      process.stdout.write(`Error spawning cube: ${e}\n`);
    }
  }

  public withEnvironment(environment: { [key in string]: string }): this {
    this.env = { ...this.env, ...environment };
    return this;
  }

  public withStartupTimeout(startupTimeout: number): this {
    return this;
  }

  public async down() {
    if (this.cli) {
      const { cli } = this;
      await new Promise((resolve) => {
        cli.once('disconnect', () => resolve(null));
        cli.once('exit', () => resolve(null));
        cli.kill('SIGKILL');
      });
      process.stdout.write('Cube Exited\n');
    }
  }
}

export async function runEnvironment(
  type: string,
  suf?: string,
  { extendedEnv }: { extendedEnv?: string } = {}
): Promise<Environment> {
  const fixtures = getFixtures(type, extendedEnv);
  getTempPath();
  getSchemaPath(type, suf);
  getCubeJsPath(type);
  getPackageJsonPath(type);
  const { mode } = yargs(process.argv.slice(2))
    .exitProcess(false)
    .options({
      mode: {
        describe: 'Determines test mode',
        choices: [
          'local',
          'docker'
        ],
        default: 'docker',
      }
    })
    .argv;
  const isLocal = mode === 'local';
  const [composePath, composeFile] = getComposePath(type, isLocal);
  const compose = new DockerComposeEnvironment(
    composePath,
    composeFile,
  );
  compose.withStartupTimeout((isCI() ? 60 : 30) * 1000);
  compose.withEnvironment({
    CUBEJS_TELEMETRY: 'false',
    CUBEJS_SCHEMA_PATH: 'schema'
  });

  const _path = `${path.resolve(process.cwd(), `./fixtures/${type}.env`)}`;
  if (fs.existsSync(_path)) {
    config({
      path: _path,
      encoding: 'utf8',
      override: true,
    });
  }
  Object.keys(fixtures.cube.environment).forEach((key) => {
    const val = fixtures.cube.environment[key];
    const { length } = val;
    if (val.indexOf('${') === 0 && val.indexOf('}') === length - 1) {
      const name = val.slice(2, length - 1).trim();
      process.env[key] = process.env[name];
    }

    if (process.env[key]) {
      compose.withEnvironment({ [key]: <string>process.env[key] });
    } else if (fixtures.cube.environment[key]) {
      process.env[key] = fixtures.cube.environment[key];
    }
  });
  // TODO extract as a config
  if (type === 'mssql') {
    compose.withWaitStrategy('data', Wait.forLogMessage('SQL Server is now ready for client connections'));
  }
  // TODO: Add health checks for all drivers
  if (type === 'clickhouse') {
    compose.withWaitStrategy('data', Wait.forHealthCheck());
  }

  const environment = await compose.up();

  const store = {
    port: environment.getContainer('store').getMappedPort(3030),
    logs: await environment.getContainer('store').logs(),
  };

  const cliEnv = isLocal ? new CubeCliEnvironment(composePath) : null;
  const mappedDataPort = fixtures.data ? environment.getContainer('data').getMappedPort(
    parseInt(fixtures.data.ports[0], 10),
  ) : null;
  if (cliEnv) {
    cliEnv.withEnvironment({
      CUBEJS_CUBESTORE_HOST: '127.0.0.1',
      CUBEJS_CUBESTORE_PORT: process.env.CUBEJS_CUBESTORE_PORT ? process.env.CUBEJS_CUBESTORE_PORT : `${store.port}`,
      CUBEJS_SCHEMA_PATH: 'schema'
    });
    if (mappedDataPort) {
      cliEnv.withEnvironment({
        CUBEJS_DB_HOST: '127.0.0.1',
        CUBEJS_DB_PORT: `${mappedDataPort}`,
        CUBEJS_SCHEMA_PATH: 'schema'
      });
    }
    await cliEnv.up();
  }
  const cube = cliEnv ? {
    port: 4000,
    pgPort: parseInt(fixtures.cube.ports[1], 10),
    logs: cliEnv.cli?.stdout || process.stdout
  } : {
    port: environment.getContainer('cube').getMappedPort(
      parseInt(fixtures.cube.ports[0], 10),
    ),
    pgPort: fixtures.cube.ports[1] && environment.getContainer('cube').getMappedPort(
      parseInt(fixtures.cube.ports[1], 10),
    ) || undefined,
    logs: await environment.getContainer('cube').logs(),
  };

  if (fixtures.data) {
    const data = {
      port: mappedDataPort!,
      logs: await environment.getContainer('data').logs(),
    };
    return {
      cube,
      store,
      data,
      stop: async () => {
        await environment.down({ timeout: 30 * 1000 });
      },
    };
  }
  return {
    cube,
    store,
    stop: async () => {
      await environment.down({ timeout: 30 * 1000 });
      if (cliEnv) {
        await cliEnv.down();
      }
    },
  };
}
