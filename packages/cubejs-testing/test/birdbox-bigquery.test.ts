import { createBirdBoxTestCase } from './abstract-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('bigquery', () => startBirdBoxFromContainer({
  name: 'bigquery',
  envPath: '/Users/cristipp/.env'
}));
