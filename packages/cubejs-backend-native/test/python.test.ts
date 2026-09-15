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

  // `chat_completion` only reaches JavaScript if it is BOTH declared on the
  // Python `Configuration` class AND present in the Rust allow-list in
  // `cube_config.rs`. Miss either and the attribute is dropped in silence —
  // no error, no warning, just a hook that never runs. These two cases are
  // what makes that omission loud.
  test('chat_completion returning a list of chunks', async () => {
    if (!config.chatCompletion) {
      throw new Error('chatCompletion was not defined in config.py');
    }

    expect(await config.chatCompletion({ model: 'gateway-model', messages: [] })).toEqual([
      { content: 'Hello from ' },
      { content: 'gateway-model' },
      { usage_metadata: { input_tokens: 3, output_tokens: 4, total_tokens: 7 } },
    ]);
  });

  test('chat_completion returning a next() pull stream', async () => {
    const streamConfig = await loadConfigurationFile('config-chat-completion-stream.py');

    if (!streamConfig.chatCompletion) {
      throw new Error('chatCompletion was not defined in config-chat-completion-stream.py');
    }

    const stream = (await streamConfig.chatCompletion({
      model: 'gateway-model',
      messages: [],
    })) as { next: () => Promise<unknown> };

    // A Python closure survives the bridge as a callable, which is the whole
    // reason the streaming form is shaped as a pull function rather than as an
    // async generator (an async generator object has no bridge representation).
    expect(typeof stream.next).toEqual('function');

    const chunks: unknown[] = [];
    for (;;) {
      // eslint-disable-next-line no-await-in-loop
      const chunk = await stream.next();
      if (chunk === undefined) {
        break;
      }
      chunks.push(chunk);
    }

    expect(chunks).toEqual([
      { content: 'strea' },
      { content: 'med ' },
      { content: 'gateway-model' },
    ]);
  });

  // `CLRepr::Null` converts to `cx.undefined()`, so the Python `None` that ends
  // a stream arrives as `undefined` and never as `null`. A consumer that stops
  // on `null` alone loops forever on an already-finished stream — pinned here
  // because the Python side says `return None` and nothing about writing it
  // suggests the JavaScript side sees anything else.
  test('a Python None crosses as undefined, not null', async () => {
    const streamConfig = await loadConfigurationFile('config-chat-completion-stream.py');
    const stream = (await streamConfig.chatCompletion!({
      model: 'gateway-model',
      messages: [],
    })) as { next: () => Promise<unknown> };

    await stream.next();
    await stream.next();
    await stream.next();

    const terminator = await stream.next();
    expect(terminator).toBeUndefined();
    expect(terminator).not.toBeNull();
  });

  // The boundary the two supported forms exist to work around. A customer's
  // first instinct is to hand back a model object the way `cube.js` can; this
  // pins that it fails loudly at the bridge rather than producing an empty
  // response.
  test('chat_completion returning an arbitrary Python object is rejected', async () => {
    const objectConfig = await loadConfigurationFile('config-chat-completion-object.py');

    if (!objectConfig.chatCompletion) {
      throw new Error('chatCompletion was not defined in config-chat-completion-object.py');
    }

    await expect(
      objectConfig.chatCompletion({ model: 'gateway-model', messages: [] })
    ).rejects.toThrow(/PyObject/);
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
      contextToGroups: expect.any(Function),
      scheduledRefreshContexts: expect.any(Function),
      scheduledRefreshTimeZones: expect.any(Function),
      chatCompletion: expect.any(Function),
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
