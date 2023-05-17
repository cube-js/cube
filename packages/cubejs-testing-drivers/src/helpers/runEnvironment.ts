import * as fs from 'fs-extra';
import * as path from 'path';
import { ChildProcess, ChildProcessWithoutNullStreams, spawn } from 'child_process';
import { config } from 'dotenv';
import yargs from 'yargs/yargs';
import { DockerComposeEnvironment } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';
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
      process.kill(-this.cli.pid, 'SIGINT');
      process.stdout.write('Cube Exited\n');
    }
  }
}

export async function runEnvironment(type: string, suf?: string): Promise<Environment> {
  const fixtures = getFixtures(type);
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
  compose.withStartupTimeout(30 * 1000);
  compose.withEnvironment({ CUBEJS_TELEMETRY: 'false' });
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
  const environment = await compose.up();

  const store = {
    port: environment.getContainer('store').getMappedPort(3030),
    logs: await environment.getContainer('store').logs(),
  };

  const cliEnv = isLocal ? new CubeCliEnvironment(composePath) : null;
  if (cliEnv) {
    cliEnv.withEnvironment({
      CUBEJS_CUBESTORE_HOST: '127.0.0.1',
      CUBEJS_CUBESTORE_PORT: process.env.CUBEJS_CUBESTORE_PORT ? process.env.CUBEJS_CUBESTORE_PORT : `${store.port}`,
    });
    await cliEnv.up();
  }
  const cube = cliEnv ? {
    port: 4000,
    logs: cliEnv.cli?.stdout || process.stdout
  } : {
    port: environment.getContainer('cube').getMappedPort(
      parseInt(fixtures.cube.ports[0], 10),
    ),
    logs: await environment.getContainer('cube').logs(),
  };

  if (fixtures.data) {
    const data = {
      port: environment.getContainer('data').getMappedPort(
        parseInt(fixtures.data.ports[0], 10),
      ),
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
