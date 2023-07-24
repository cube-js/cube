import fs from 'fs/promises';
import path from 'path';

import * as native from '../js';

(async () => {
  native.setupLogger(
    ({ event }) => console.log(event),
    'trace',
  );

  const content = await fs.readFile(path.join(process.cwd(), 'test', 'config.py'), 'utf8');
  console.log('content', {
    content
  });

  const config = await native.pythonLoadConfig(
    content,
    {
      fileName: 'config.py'
    }
  );

  console.log(config);

  if (config.queryRewrite) {
    console.log('->queryRewrite');

    const result = await config.queryRewrite(
      {
        measures: ['Orders.count']
      },
      {
        securityContext: {
          tenantId: 1
        }
      }
    );

    console.log(result, '<-');
  }

  if (config.checkAuth) {
    console.log('->checkAuth');

    const result = await config.checkAuth({
      url: 'test',
      method: 'GET',
      headers: {
        'X-MY-HEADER': 'LONG HEADER VALUE'
      }
    }, 'MY_LONG_TOKEN');

    console.log(result, '<-');
  }

  if (config.contextToApiScopes) {
    console.log('->contextToApiScopes');

    const result = await config.contextToApiScopes();

    console.log(result, '<-');
  }

  console.log('js finish');
})();
