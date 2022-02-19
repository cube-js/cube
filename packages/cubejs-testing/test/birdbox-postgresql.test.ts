import { createBirdBoxTestCase } from './abstract-test-case';
import { startBirdBoxFromContainer } from '../src';

const entrypoint = () => startBirdBoxFromContainer({
  name: 'postgresql',
  loadScript: 'postgres-load-events.sh',
});

createBirdBoxTestCase('postgresql', entrypoint);
