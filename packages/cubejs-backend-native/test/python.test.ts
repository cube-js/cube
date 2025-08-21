import fs from 'fs/promises';
import path from 'path';

import * as native from '../js';
import { PyConfiguration } from '../js';

const suite = native.isFallbackBuild() ? xdescribe : describe;
// TODO(ovr): Find what is going wrong with parallel tests & python on Linux
const darwinSuite = process.platform === 'darwin' && !native.isFallbackBuild() ? describe : xdescribe;

async function loadConfigurationFile(fileName: string) {
  const fullFileName = path.join(process.cwd(), 'test', fileName);
  const content = await fs.readFile(fullFileName, 'utf8');
  console.log('content', {
    content,
    fileName: fullFileName
  });

  const config = await native.pythonLoadConfig(
    content,
    {
      fileName: fullFileName
    }
  );

  console.log(`loaded config ${fileName}`, config);

  return config;
}

const nativeInstance = new native.NativeInstance();

suite('Python Models', () => {
  test('models import', async () => {
    const fullFileName = path.join(process.cwd(), 'test', 'globals.py');
    const content = await fs.readFile(fullFileName, 'utf8');

    // Just checking it won't fail
    await nativeInstance.loadPythonContext(fullFileName, content);
  });

  test('models import with sys.path changed', async () => {
    const fullFileName = path.join(process.cwd(), 'test', 'globals_w_import_path.py');
    const content = await fs.readFile(fullFileName, 'utf8');

    // Just checking it won't fail
    await nativeInstance.loadPythonContext(fullFileName, content);
  });
});

suite('Python Config', () => {
  let config: PyConfiguration;

  beforeAll(async () => {
    config = await loadConfigurationFile('config.py');
  });

  test('async checkAuth', async () => {
    expect(config).toEqual({
      schemaPath: 'models',
      telemetry: false,
      contextToApiScopes: expect.any(Function),
      logger: expect.any(Function),
      pgSqlPort: 5555,
      preAggregationsSchema: expect.any(Function),
      checkAuth: expect.any(Function),
      extendContext: expect.any(Function),
      queryRewrite: expect.any(Function),
      repositoryFactory: expect.any(Function),
      schemaVersion: expect.any(Function),
      contextToRoles: expect.any(Function),
      contextToGroups: expect.any(Function),
      scheduledRefreshContexts: expect.any(Function),
      scheduledRefreshTimeZones: expect.any(Function),
    });

    if (!config.checkAuth) {
      throw new Error('checkAuth was not defined in config.py');
    }

    const result = await config.checkAuth(
      { requestId: 'test' },
      'MY_SECRET_TOKEN'
    );

    expect(result).toEqual({
      security_context: {
        sub: '1234567890',
        iat: 1516239022,
        user_id: 42
      },
    });
  });

  test('context_to_roles', async () => {
    if (!config.contextToRoles) {
      throw new Error('contextToRoles was not defined in config.py');
    }

    expect(await config.contextToRoles({})).toEqual(['admin']);
  });

  test('context_to_groups', async () => {
    if (!config.contextToGroups) {
      throw new Error('contextToGroups was not defined in config.py');
    }

    expect(await config.contextToGroups({})).toEqual(['dev', 'analytics']);
  });

  test('context_to_api_scopes', async () => {
    if (!config.contextToApiScopes) {
      throw new Error('contextToApiScopes was not defined in config.py');
    }

    expect(await config.contextToApiScopes()).toEqual(['meta', 'data', 'jobs']);
  });

  test('scheduled_refresh_time_zones', async () => {
    if (!config.scheduledRefreshTimeZones) {
      throw new Error('scheduledRefreshTimeZones was not defined in config.py');
    }

    expect(await config.scheduledRefreshTimeZones({})).toEqual(['Europe/Kyiv', 'Antarctica/Troll', 'Australia/Sydney']);
  });

  test('scheduled_refresh_contexts', async () => {
    if (!config.scheduledRefreshContexts) {
      throw new Error('scheduledRefreshContexts was not defined in config.py');
    }

    expect(await config.scheduledRefreshContexts({})).toEqual([
      {
        securityContext: {
          appid: 'test1', u: { prop1: 'value1' }
        }
      },
      {
        securityContext: {
          appid: 'test2', u: { prop1: 'value2' }
        }
      },
      {
        securityContext: {
          appid: 'test3', u: { prop1: 'value3' }
        }
      },
    ]);
  });

  test('extend_context', async () => {
    if (!config.extendContext) {
      throw new Error('extendContext was not defined in config.py');
    }

    // Without security context
    expect(await config.extendContext({})).toEqual({
      security_context: {
        error: 'missing',
      },
    });

    // With security context
    expect(await config.extendContext({
      securityContext: { sub: '1234567890', iat: 1516239022, user_id: 42 }
    })).toEqual({
      security_context: {
        extended_by_config: true,
        sub: '1234567890',
        iat: 1516239022,
        user_id: 42
      },
    });
  });

  test('repository factory', async () => {
    if (!config.repositoryFactory) {
      throw new Error('repositoryFactory was not defined in config.py');
    }

    const ctx = {
      securityContext: { schemaPath: path.join(process.cwd(), 'test', 'fixtures', 'schema-tenant-1') }
    };

    const repository: any = await config.repositoryFactory(ctx);
    expect(repository).toEqual({
      dataSchemaFiles: expect.any(Function)
    });

    const files = await repository.dataSchemaFiles();
    expect(files).toContainEqual({
      fileName: 'test.yml',
      content: expect.any(String),
    });
    expect(files).toContainEqual({
      fileName: 'test.yml.jinja',
      content: expect.any(String),
    });
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
      obj_with_nested_object: {
        sub_object: {
          sub_field_str: 'string'
        }
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

darwinSuite('Old Python Config', () => {
  test('test', async () => {
    const config = await loadConfigurationFile('old-config.py');
    expect(config).toEqual({
      schemaPath: 'models',
      telemetry: false,
      contextToApiScopes: expect.any(Function),
      extendContext: expect.any(Function),
      logger: expect.any(Function),
      pgSqlPort: 5555,
      preAggregationsSchema: expect.any(Function),
      checkAuth: expect.any(Function),
      queryRewrite: expect.any(Function),
      repositoryFactory: expect.any(Function),
      schemaVersion: expect.any(Function),
      contextToRoles: expect.any(Function),
      contextToGroups: expect.any(Function),
      scheduledRefreshContexts: expect.any(Function),
      scheduledRefreshTimeZones: expect.any(Function),
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
