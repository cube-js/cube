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
  const fixture = getFixtures(type, extendedEnv);
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
    .parseSync();
  const isLocal = mode === 'local';
  const [composePath, composeFile] = getComposePath(type, fixture, isLocal);
  const compose = new DockerComposeEnvironment(
    composePath,
    composeFile,
  );
  compose.withStartupTimeout((isCI() ? 60 : 30) * 1000);
  compose.withEnvironment({
    CUBEJS_TELEMETRY: 'false',
  });

  Object.keys(fixture.cube.environment).forEach((key) => {
    const val = fixture.cube.environment[key];
    const { length } = val;

    if (val.indexOf('${') === 0 && val.indexOf('}') === length - 1) {
      const name = val.slice(2, length - 1).trim();
      const value = process.env[name];
      if (value) {
        process.env[key] = process.env[name];
      } else {
        throw new Error(`Env variable ${name} must be defined, because it's used as ${key}`);
      }
    }

    if (process.env[key]) {
      compose.withEnvironment({ [key]: <string>process.env[key] });
    } else if (fixture.cube.environment[key]) {
      process.env[key] = fixture.cube.environment[key];
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
  const mappedDataPort = fixture.data ? environment.getContainer('data').getMappedPort(
    parseInt(fixture.data.ports[0], 10),
  ) : null;
  if (cliEnv) {
    cliEnv.withEnvironment({
      CUBEJS_CUBESTORE_HOST: '127.0.0.1',
      CUBEJS_CUBESTORE_PORT: process.env.CUBEJS_CUBESTORE_PORT ? process.env.CUBEJS_CUBESTORE_PORT : `${store.port}`,
    });
    if (mappedDataPort) {
      cliEnv.withEnvironment({
        CUBEJS_DB_HOST: '127.0.0.1',
        CUBEJS_DB_PORT: `${mappedDataPort}`,
      });
    }
    await cliEnv.up();
  }
  const cube = cliEnv ? {
    port: 4000,
    pgPort: parseInt(fixture.cube.ports[1], 10),
    logs: cliEnv.cli?.stdout || process.stdout
  } : {
    port: environment.getContainer('cube').getMappedPort(
      parseInt(fixture.cube.ports[0], 10),
    ),
    pgPort: fixture.cube.ports[1] && environment.getContainer('cube').getMappedPort(
      parseInt(fixture.cube.ports[1], 10),
    ) || undefined,
    logs: await environment.getContainer('cube').logs(),
  };

  if (fixture.data) {
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
        if (cliEnv) {
          await cliEnv.down();
        }
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
