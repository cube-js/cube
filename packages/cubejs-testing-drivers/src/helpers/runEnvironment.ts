import * as fs from 'fs-extra';
import * as path from 'path';
import { config } from 'dotenv';
import { DockerComposeEnvironment } from 'testcontainers';
import { buildCube } from './buildCube';
import { getFixtures } from './getFixtures';
import { getTempPath } from './getTempPath';
import { getComposePath } from './getComposePath';
import { getCubeJsPath } from './getCubeJsPath';
import { getSchemaPath } from './getSchemaPath';
import { Environment } from '../types/Environment';

export async function runEnvironment(type: string): Promise<Environment> {
  const fixtures = getFixtures(type);
  // buildCube();
  getTempPath();
  getSchemaPath(type);
  getCubeJsPath(type);
  const [composePath, composeFile] = getComposePath(type);
  const compose = new DockerComposeEnvironment(
    composePath,
    composeFile,
  );
  compose.withStartupTimeout(30 * 1000);
  compose.withEnv('CUBEJS_TELEMETRY', 'false');
  const _path = `${path.resolve(process.cwd(), './fixtures/postgres.env')}`;
  if (fs.existsSync(_path)) {
    config({
      path: _path,
      encoding: 'utf8',
      override: true,
    });
  }
  Object.keys(fixtures.cube.environment).forEach((key) => {
    if (process.env[key]) {
      compose.withEnv(key, <string>process.env[key]);
    } else if (fixtures.cube.environment[key]) {
      process.env[key] = fixtures.cube.environment[key];
    }
  });
  const environment = await compose.up();
  const cube = {
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
      data,
      stop: async () => {
        await environment.down();
      },
    };
  }
  return {
    cube,
    stop: async () => {
      await environment.down();
    },
  };
}
