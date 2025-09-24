import fs from 'fs/promises';
import path from 'path';
import { benchmarkSuite } from 'jest-bench';

import * as native from '../js';
import { PyConfiguration } from '../js';

async function loadConfigurationFile(fileName: string): Promise<PyConfiguration> {
  const fullFileName = path.join(process.cwd(), 'benchmarks', 'fixtures', fileName);
  const content = await fs.readFile(fullFileName, 'utf8');

  return native.pythonLoadConfig(content, {
    fileName: fullFileName
  });
}

// Global variables to hold loaded configs and test data
let configPy: PyConfiguration;
let configAsyncPy: PyConfiguration;
let configContent: string;
let configAsyncContent: string;

describe('Python Configuration Loading', () => {
  beforeAll(async () => {
    // Load file contents once for all benchmarks
    const configPath = path.join(process.cwd(), 'benchmarks', 'fixtures', 'config.py');
    const configAsyncPath = path.join(process.cwd(), 'benchmarks', 'fixtures', 'config-async.py');
    const oldConfigPath = path.join(process.cwd(), 'test', 'old-config.py');

    [configContent, configAsyncContent] = await Promise.all([
      fs.readFile(configPath, 'utf8'),
      fs.readFile(configAsyncPath, 'utf8'),
    ]);

    // Pre-load configurations for function benchmarks
    configPy = await loadConfigurationFile('config.py');
    configAsyncPy = await loadConfigurationFile('config-async.py');
  });

  benchmarkSuite('Config Loading', {
    'Load config.py': async () => {
      const fullFileName = path.join(process.cwd(), 'benchmarks', 'fixtures', 'config.py');
      await native.pythonLoadConfig(configContent, { fileName: fullFileName });
    },

    'Load config-async.py': async () => {
      const fullFileName = path.join(process.cwd(), 'benchmarks', 'fixtures', 'config-async.py');
      await native.pythonLoadConfig(configAsyncContent, { fileName: fullFileName });
    }
  });

  benchmarkSuite('Data Conversion Performance', {
    'Small payload (3 fields)': async () => {
      const smallPayload = { simple_string: 'test', number: 42, boolean: true };
      await configPy.queryRewrite!(smallPayload, {});
    },

    'Medium payload (100 users)': async () => {
      const mediumPayload = {
        users: Array.from({ length: 100 }, (_, i) => ({
          id: i, name: `User ${i}`, active: i % 2 === 0
        }))
      };
      await configPy.queryRewrite!(mediumPayload, {});
    },

    'Large payload (1000 items)': async () => {
      const largePayload = {
        data: Array.from({ length: 1000 }, (_, i) => ({
          id: i, value: `Value ${i}`, nested: { prop: i * 2 }
        }))
      };
      await configPy.queryRewrite!(largePayload, {});
    }
  });

  benchmarkSuite('Function Execution', {
    'checkAuth - sync version (sequential)': async () => {
      await configPy.checkAuth!({ requestId: 'sync-bench' }, 'SYNC_TOKEN');
    },

    // It should help to identify any potential issues with GIL
    'checkAuth - sync version (parallel 50x)': async () => {
      await Promise.all(
          Array.from({ length: 50 }, () => configPy.checkAuth!({ requestId: 'sync-bench' }, 'SYNC_TOKEN'))
      );
    },

    'checkAuth - async version (sequential)': async () => {
      await configAsyncPy.checkAuth!({ requestId: 'async-bench' }, 'ASYNC_TOKEN');
    },

    // It should help to identify any potential issues with GIL
    'checkAuth - async version (parallel 50x)': async () => {
      await Promise.all(
          Array.from({ length: 50 }, () => configAsyncPy.checkAuth!({ requestId: 'async-bench' }, 'ASYNC_TOKEN'))
      );
    },

    'extendContext - sync version': async () => {
      await configPy.extendContext!({
        securityContext: { sub: '1234567890', iat: 1516239022, user_id: 42 }
      });
    },

    'extendContext - async version (sequential)': async () => {
      await configAsyncPy.extendContext!({
        securityContext: { sub: '1234567890', iat: 1516239022, user_id: 42 }
      });
    },

    'queryRewrite - sync version': async () => {
      const testQuery = { str: 'string', int_number: 1, bool_true: true };
      await configPy.queryRewrite!(testQuery, {});
    },

    'queryRewrite - async version (sequential)': async () => {
      const testQuery = { str: 'string', int_number: 1, bool_true: true };
      await configAsyncPy.queryRewrite!(testQuery, {});
    },
  });
});