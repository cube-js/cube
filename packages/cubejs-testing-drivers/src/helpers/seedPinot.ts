import { pausePromise } from '@cubejs-backend/shared';

import type { StartedDockerComposeEnvironment } from 'testcontainers';

const PINOT_TABLES = [
  'customers_pinot',
  'products_pinot',
  'ecommerce_pinot',
  'bigecommerce_pinot',
  'retailcalendar_pinot',
];

// Directory the committed fixtures/pinot resources are mounted at inside the
// controller container (see fixtures/pinot.json `services.pinot-controller.volumes`).
const RESOURCE_DIR = '/tmp/data/test-resources';
const ADMIN = '/opt/pinot/bin/pinot-admin.sh';
const CONTROLLER_URL = 'http://localhost:9000';
const AUTH = 'admin:mysecret';

async function waitForSegmentsOnline(controller: any, table: string): Promise<void> {
  const url = `${CONTROLLER_URL}/tables/${table}/externalview`;

  for (let attempt = 0; attempt < 60; attempt += 1) {
    // eslint-disable-next-line no-await-in-loop
    const { output } = await controller.exec(['curl', '-s', '-u', AUTH, url]);
    if (output.includes('ERROR')) {
      throw new Error(`Pinot segment load failed for ${table}: ${output}`);
    }

    if (output.includes('ONLINE')) {
      return;
    }

    // eslint-disable-next-line no-await-in-loop
    await pausePromise(2 * 1000);
  }

  throw new Error(`Timed out waiting for ${table} segments to come ONLINE`);
}

export async function seedPinot(environment: StartedDockerComposeEnvironment): Promise<void> {
  const controller = environment.getContainer('pinot-controller');

  for (const table of PINOT_TABLES) {
    await controller.exec([
      ADMIN, 'AddTable',
      '-controllerPort', '9000',
      '-schemaFile', `${RESOURCE_DIR}/${table}.schema.json`,
      '-tableConfigFile', `${RESOURCE_DIR}/${table}.table.json`,
      '-exec',
    ]);
  }

  for (const table of PINOT_TABLES) {
    await controller.exec([
      ADMIN, 'LaunchDataIngestionJob',
      '-jobSpecFile', `${RESOURCE_DIR}/${table}.jobspec.yml`,
    ]);
  }

  // Wait until every table's segments have been loaded by the server.
  for (const table of PINOT_TABLES) {
    await waitForSegmentsOnline(controller, table);
  }
}
