import { createBirdBoxTestCase } from './abstract-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromContainer({
  name: 'postgresql',
  loadScript: 'postgres-load-events.sh',
}));
