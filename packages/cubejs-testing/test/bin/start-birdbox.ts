import { startBirdBoxFromContainer } from '../../src';

(async () => {
  console.log('[Birdbox] Starting');

  try {
    await startBirdBoxFromContainer({
      type: 'postgresql-cubestore',
    });
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
})();
