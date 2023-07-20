import fs from 'fs/promises';
import path from 'path';

import * as native from '../js';
import { PyConfiguration } from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;
// TODO(ovr): Find what is going wrong with parallel tests & python on Linux
const darwinSuite = process.platform === 'darwin' && !native.isFallbackBuild() ? describe : xdescribe;

async function loadConfigurationFile(file: string) {
  const content = await fs.readFile(path.join(process.cwd(), 'test', file), 'utf8');
  console.log('content', {
    content,
    file
  });

  const config = await native.pythonLoadConfig(
    content,
    {
      file
    }
  );

  console.log(`loaded config ${file}`, config);

  return config;
}

suite('Python Config', () => {
  let config: PyConfiguration;

  beforeAll(async () => {
    config = await loadConfigurationFile('config.py');
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

    if (!config.checkAuth) {
      throw new Error('checkAuth was not defined in config.py');
    }

    await config.checkAuth(
      { requestId: 'test' },
      'MY_SECRET_TOKEN'
    );
  });

  test('context_to_api_scopes', async () => {
    if (!config.contextToApiScopes) {
      throw new Error('contextToApiScopes was not defined in config.py');
    }

    expect(await config.contextToApiScopes()).toEqual(['meta', 'data', 'jobs']);
  });

  test('cross language converting (js -> python -> js)', async () => {
    if (!config.queryRewrite) {
      throw new Error('queryRewrite was not defined in config.py');
    }

    const input = {
      str: 'string',
      int_number: 1,
      int_max_number: Number.MAX_VALUE,
      int_min_number: Number.MIN_VALUE,
      float_number: 3.1415,
      nan_number: NaN,
      infinity_number: 10 ** 10000,
      bool_true: true,
      bool_false: false,
      undefined_field: undefined,
      obj: {
        field_str: 'string',
      },
      array_int: [1, 2, 3, 4, 5],
      array_obj: [{
        field_str_first: 'string',
      }, {
        field_str_second: 'string',
      }]
    };

    expect(await config.queryRewrite(input, {})).toEqual(
      input
    );
  });
});

darwinSuite('Scoped Python Config', () => {
  test('test', async () => {
    const config = await loadConfigurationFile('scoped-config.py');
    expect(config).toEqual({
      schemaPath: 'models',
      pgSqlPort: 5555,
      telemetry: false,
      contextToApiScopes: expect.any(Function),
      checkAuth: expect.any(Function),
      queryRewrite: expect.any(Function),
    });

    if (!config.checkAuth) {
      throw new Error('checkAuth was not defined in config.py');
    }

    await config.checkAuth(
      { requestId: 'test' },
      'MY_SECRET_TOKEN'
    );
  });
});
