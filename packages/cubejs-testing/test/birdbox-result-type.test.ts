/* eslint-disable import/no-extraneous-dependencies */

import { createBirdBoxTestCase } from './abstract-result-type-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('cli-result-type', () => startBirdBoxFromContainer({
  name: 'postgresql',
  loadScript: 'load.sh',
}));
