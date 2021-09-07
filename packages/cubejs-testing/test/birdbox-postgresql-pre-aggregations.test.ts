import { createBirdBoxTestCase } from './pre-aggregations-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('postgresql-cubestore', () => startBirdBoxFromContainer({
  name: 'postgresql-cubestore',
  loadScript: 'load-pre-aggregations.sh',
}));
