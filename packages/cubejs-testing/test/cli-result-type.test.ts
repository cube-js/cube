/* eslint-disable import/no-extraneous-dependencies */

import { createBirdBoxTestCase } from './abstract-result-type-test-case';
import { startBirdBoxFromCli } from '../src';

createBirdBoxTestCase('cli-result-type', () => startBirdBoxFromCli({
  dbType: 'postgresql',
  useCubejsServerBinary: process.env.USE_LOCAL_CUBEJS_BINARY === 'true',
  cubejsConfig: 'single/cube.js',
  loadScript: 'load.sh',
}));
