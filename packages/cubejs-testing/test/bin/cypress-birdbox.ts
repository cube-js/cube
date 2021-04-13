import cypress from 'cypress';
import { startBirdBoxFromContainer } from '../../src';

(async () => {
  let birdbox;

  const name = process.env.BIRDBOX_CYPRESS_TARGET || 'postgresql-cubestore';

  console.log(`[Birdbox] Starting "${name}"`);

  try {
    birdbox = await startBirdBoxFromContainer({
      name,
    });
  } catch (e) {
    console.log(e);
    process.exit(1);
  }

  console.log(`[Birdbox] Started`);
  console.log('[Cypress] Starting');

  let cypressFailed = false;

  try {
    const browser = process.env.BIRDBOX_CYPRESS_BROWSER || 'chrome';

    const results = await cypress.run({
      browser,
      // @todo tput: No value for $TERM and no -T specified
      // headless: true,
      config: {
        baseUrl: birdbox.configuration.playgroundUrl,
        video: true,
        taskTimeout: 10 * 1000,
      }
    });

    if (results.status === 'failed') {
      throw new Error('Cypress failed');
    }

    if (results.status === 'finished' && results.totalFailed > 0) {
      throw new Error('Cypress failed');
    }
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
