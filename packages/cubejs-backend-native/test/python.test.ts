import fs from 'fs/promises';
import path from 'path';

import * as native from '../js';
import { PyConfiguration } from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;

suite('Python', () => {
  async function loadConfigurationFile() {
    const content = await fs.readFile(path.join(process.cwd(), 'test', 'config.py'), 'utf8');
    console.log('content', {
      content
    });

    const config = await native.pythonLoadConfig(
      content,
      {
        file: 'config.py'
      }
    );

    console.log('loaded config', config);

    return config;
  }

  let config: PyConfiguration;

  beforeAll(async () => {
    config = await loadConfigurationFile();
  });

  test('async checkAuth', async () => {
    expect(config).toEqual({
      schemaPath: 'models',
      pgSqlPort: 5555,
      telemetry: false,
      contextToApiScopes: expect.any(Function),
      checkAuth: expect.any(Function),
      queryRewrite: expect.any(Function),
    });

    if (config.checkAuth) {
      await config.checkAuth({ requestId: 'test' }, 'MY_SECRET_TOKEN');
    } else {
      throw new Error('checkAuth was defined in config.py');
    }
  });

  test('context_to_api_scopes', async () => {
    if (config.contextToApiScopes) {
      expect(await config.contextToApiScopes()).toEqual(['meta', 'data', 'jobs']);
    } else {
      throw new Error('contextToApiScopes was defined in config.py');
    }
  });
});
