import { startBirdBoxFromContainer } from '../../src';

(async () => {
  let birdbox;

  console.log('[Birdbox] Starting');

  try {
    birdbox = await startBirdBoxFromContainer({
      name: 'postgresql-cubestore',
    });
  } catch (e) {
    console.log(e);
    process.exit(1);
  }

  console.log('[Birdbox] Started');
})();
