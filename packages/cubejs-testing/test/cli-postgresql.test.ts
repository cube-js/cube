import { startBirdBoxFromCli } from '../src';
import { createBirdBoxTestCase } from './abstract-test-case';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromCli({
  dbType: 'postgresql',
  useCubejsServerBinary: process.env.USE_LOCAL_CUBEJS_BINARY === 'true',
  cubejsConfig: 'single/cube.js',
  loadScript: 'postgres-load-events.sh',
}));
