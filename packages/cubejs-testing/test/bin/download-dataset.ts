/* eslint-disable no-restricted-syntax */
import { downloadAndExtractFile } from '@cubejs-backend/shared';
import path from 'path';

import { getDataSetDescription, DataSetSchema } from './utils';

async function downloadDataSet(schema: DataSetSchema) {
  for (const file of schema.files) {
    await downloadAndExtractFile(`https://github.com/cube-js/testing-fixtures/raw/master/${file}`, {
      cwd: path.resolve(path.join(__dirname, '..', '..', '..', 'birdbox-fixtures', 'datasets')),
      showProgress: true,
    });
  }
}

(async () => {
  const schema = await getDataSetDescription('minimal');
  await downloadDataSet(schema);
})();
