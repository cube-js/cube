import { createBirdBoxTestCase } from './driver-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('questdb', () => startBirdBoxFromContainer({
  type: 'questdb',
  loadScript: 'questdb-load-events.sh',
}));
