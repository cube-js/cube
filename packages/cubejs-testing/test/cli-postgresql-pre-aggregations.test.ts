import { startBirdBoxFromCli } from '../src';
import { createBirdBoxTestCase } from './pre-aggregations-test-case';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromCli({
  type: 'postgresql',
  useCubejsServerBinary: true,
  loadScript: 'load-pre-aggregations.sh',
  schemaDir: 'postgresql/schema',
  cubejsConfig: 'postgresql/single/cube.js',
}));
