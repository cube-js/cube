import cypress from 'cypress';
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

  console.log('[Cypress] Starting');

  let cypressFailed = false;

  try {
    const browser = process.env.BIRDBOX_CYPRESS_BROWSER || 'chrome';

    await cypress.run({
      browser,
      config: {
        baseUrl: birdbox.configuration.playgroundUrl,
        video: true,
      }
    });
  } catch (e) {
    cypressFailed = true;

    console.log(e);
  }

  console.log('[Cypress] Finished');

  console.log('[Birdbox] Cleaning');

  try {
    await birdbox.stop();
  } catch (e) {
    console.log(e);
    process.exit(1);
  }

  if (cypressFailed) {
    process.exit(1);
  }
})();
