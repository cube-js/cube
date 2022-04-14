import { startBirdBoxFromCli } from '../src';
import { createBirdBoxTestCase } from './pre-aggregations-test-case';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromCli({
  type: 'postgresql',
  useCubejsServerBinary: true,
  loadScript: 'load-pre-aggregations.sh',
  cubejsConfig: 'cubestore/cube.js'
}));
