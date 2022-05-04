import { createBirdBoxTestCase } from './abstract-test-case';
import { startBirdBoxFromCli, startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromContainer({
  type: 'postgresql',
  loadScript: 'postgres-load-events.sh',
}));
