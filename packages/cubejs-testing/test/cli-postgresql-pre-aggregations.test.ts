import { startBirdBoxFromCli } from '../src';
import { createBirdBoxTestCase } from './pre-aggregations-test-case';

createBirdBoxTestCase('postgresql', () => startBirdBoxFromCli({
  dbType: 'postgresql',
  useCubejsServerBinary: true,
  loadScript: 'load-pre-aggregations.sh'
}));
