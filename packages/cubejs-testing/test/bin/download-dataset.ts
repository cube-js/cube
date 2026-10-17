/* eslint-disable no-restricted-syntax */
import { downloadAndExtractFile } from '@cubejs-backend/shared';
import path from 'path';

import { getDataSetDescription, DataSetSchema, datasetUrl, withRetries } from './utils';

async function downloadDataSet(schema: DataSetSchema) {
  for (const file of schema.files) {
    await withRetries(file, () => downloadAndExtractFile(datasetUrl(file), {
      cwd: path.resolve(path.join(__dirname, '..', '..', '..', 'birdbox-fixtures', 'datasets')),
      showProgress: true,
    }));
  }
}

(async () => {
  const schema = await getDataSetDescription('minimal');
  await downloadDataSet(schema);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
