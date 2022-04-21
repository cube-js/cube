import { createBirdBoxTestCase } from './abstract-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('postgresql-cubestore', () => startBirdBoxFromContainer({
  type: 'postgresql-cubestore',
  loadScript: 'postgres-load-events.sh',
}));
