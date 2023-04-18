import * as fs from 'fs-extra';
import * as path from 'path';
import { config } from 'dotenv';
import { DockerComposeEnvironment } from 'testcontainers';
import { getFixtures } from './getFixtures';
import { getTempPath } from './getTempPath';
import { getComposePath } from './getComposePath';
import { getCubeJsPath } from './getCubeJsPath';
import { getPackageJsonPath } from './getPackageJsonPath';
import { getSchemaPath } from './getSchemaPath';
import { Environment } from '../types/Environment';

export async function runEnvironment(type: string, suf?: string): Promise<Environment> {
  const fixtures = getFixtures(type);
  getTempPath();
  getSchemaPath(type, suf);
  getCubeJsPath(type);
  getPackageJsonPath(type);
  const [composePath, composeFile] = getComposePath(type);
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
  const cube = {
    port: environment.getContainer('cube').getMappedPort(
      parseInt(fixtures.cube.ports[0], 10),
    ),
    logs: await environment.getContainer('cube').logs(),
  };
  const store = {
    port: environment.getContainer('store').getMappedPort(3030),
    logs: await environment.getContainer('store').logs(),
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
    },
  };
}
