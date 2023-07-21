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

  if (!config.checkAuth) {
    throw new Error('config.checkAuth was not defined');
  }

  const promises = [];

  for (let i = 0; i < 1000; i++) {
    promises.push(config.checkAuth({
      url: 'test',
      method: 'GET',
      headers: {
        'X-MY-HEADER': 'LONG HEADER VALUE'
      }
    }, 'MY_LONG_TOKEN'));
  }

  await Promise.all(promises);

  console.log('js finish');
})();
