import fs from 'fs/promises';
import path from 'path';

import * as native from '../js';

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

  test('async checkAuth', async () => {
    const config = await loadConfigurationFile();
    expect(config).toEqual({
      schemaPath: 'models',
      pgSqlPort: 5555,
      telemetry: false,
      checkAuth: expect.any(Function),
      queryRewrite: expect.any(Function),
    })

    if (config.checkAuth) {
      await config.checkAuth({ requestId: 'test' }, 'MY_SECRET_TOKEN');
    } else {
      throw new Error('checkAuth was defined in config.py')
    }
  })
});
