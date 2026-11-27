import { startBirdBoxFromCli } from '../src';
import { createBirdBoxTestCase } from './abstract-test-case';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromCli({
  type: 'postgresql',
  loadScript: 'postgres-load-events.sh',
  schemaDir: 'postgresql/schema',
  cubejsConfig: 'postgresql/single/cube.js',
}));
