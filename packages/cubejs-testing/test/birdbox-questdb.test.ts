import { createBirdBoxTestCase } from './driver-test-case';
import { startBirdBoxFromContainer } from '../src';

createBirdBoxTestCase('questdb', () => startBirdBoxFromContainer({
  name: 'questdb',
  loadScript: 'questdb-load-events.sh',
}));
