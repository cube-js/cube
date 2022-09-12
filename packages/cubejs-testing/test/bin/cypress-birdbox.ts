import cypress from 'cypress';
import { DriverType, startBirdBoxFromContainer } from '../../src';

(async () => {
  let birdbox;

  const name = (process.env.BIRDBOX_CYPRESS_TARGET || 'postgresql-cubestore') as DriverType;

  console.log(`[Birdbox] Starting "${name}"`);

  try {
    birdbox = await startBirdBoxFromContainer({
      type: name,
      loadScript: 'load.sh'
    });
  } catch (e) {
    console.log(e);
    process.exit(1);
  }

  console.log('[Birdbox] Started');
  console.log('[Cypress] Starting');

  let cypressFailed = false;

  try {
    const browser = process.env.BIRDBOX_CYPRESS_BROWSER || 'chrome';

    const options: Partial<CypressCommandLine.CypressRunOptions> = {
      browser,
      // @todo tput: No value for $TERM and no -T specified
      // headless: true,
      config: {
        baseUrl: birdbox.configuration.playgroundUrl,
        video: true,
        // default 4000
        defaultCommandTimeout: 15 * 1000,
        // default 5000
        requestTimeout: 10 * 1000,
        taskTimeout: 10 * 1000,
      },
      env: {
        ...birdbox.configuration.env
      },
    };

    const { BIRDBOX_CYPRESS_UPDATE_SCREENSHOTS } = process.env;

    if (BIRDBOX_CYPRESS_UPDATE_SCREENSHOTS && (BIRDBOX_CYPRESS_UPDATE_SCREENSHOTS.toLowerCase() === 'true' || BIRDBOX_CYPRESS_UPDATE_SCREENSHOTS === '1')) {
      console.log('[Cypress] Update screenshots enabled');

      options.env = {
        ...options.env,
        updateSnapshots: true,
      };
    } else {
      console.log('[Cypress] Update screenshots disabled');
    }

    if (process.env.CYPRESS_RECORD_KEY) {
      options.record = true;
      options.key = process.env.CYPRESS_RECORD_KEY;

      console.log('[Cypress] Recording enabled');
    }

    if (process.env.TEST_PLAYGROUND_PORT) {
      console.log(`[Cypress] Testing local Playground at ${birdbox.configuration.playgroundUrl}`);
      await cypress.open(options);
    } else {
      const results = await cypress.run(options);

      if (results.status === 'failed') {
        throw new Error('Cypress failed');
      }

      if (results.status === 'finished' && results.totalFailed > 0) {
        throw new Error('Cypress failed');
      }
    }
  } catch (e) {
    cypressFailed = true;

    console.log(e);
  }

  console.log('[Cypress] Finished');

  console.log('[Birdbox] Cleaning');

  try {
    if (process.env.TEST_PLAYGROUND_PORT == null) {
      await birdbox.stop();
    }
  } catch (e) {
    console.log(e);
    process.exit(1);
  }

  if (cypressFailed) {
    process.exit(1);
  }
})();
