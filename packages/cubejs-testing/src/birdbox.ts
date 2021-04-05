import { DockerComposeEnvironment } from 'testcontainers';
import path from 'path';
import { spawn } from 'child_process';
import { pausePromise } from '@cubejs-backend/shared';
import { PostgresDBRunner } from './db/postgres';

export interface BirdBoxTestCaseOptions {
  name: string
}

export interface BirdBox {
  stop: () => Promise<void>,
  configuration: {
    playgroundUrl: string,
    apiUrl: string,
  }
}

export async function startBirdBoxFromContainer(options: BirdBoxTestCaseOptions): Promise<BirdBox> {
  if (process.env.TEST_CUBE_HOST) {
    const host = process.env.TEST_CUBE_HOST || 'localhost';
    const port = process.env.TEST_CUBE_PORT || '8888';

    return {
      stop: async () => {
        console.log('[Birdbox] Closed');
      },
      configuration: {
        playgroundUrl: `http://${host}:${port}`,
        apiUrl: `http://${host}:${port}`,
      },
    };
  }

  const dc = new DockerComposeEnvironment(
    path.resolve(path.dirname(__filename), '../../birdbox-fixtures/'),
    `${options.name}.yml`
  );

  const env = await dc
    .withStartupTimeout(30 * 1000)
    .withEnv('BIRDBOX_CUBEJS_VERSION', process.env.BIRDBOX_CUBEJS_VERSION || 'latest')
    .up();

  const host = '127.0.0.1';
  const port = env.getContainer('birdbox-cube').getMappedPort(4000);

  return {
    stop: async () => {
      console.log('[Birdbox] Closing');

      await env.down();

      console.log('[Birdbox] Closed');
    },
    configuration: {
      playgroundUrl: `http://${host}:${port}`,
      apiUrl: `http://${host}:${port}/cubejs-api/v1`,
    },
  };
}

export interface StartCliWithEnvOptions {
  dbType: string
}

export async function startBirdBoxFromCli(options: StartCliWithEnvOptions): Promise<BirdBox> {
  if (options.dbType !== 'postgres') {
    throw new Error('Unsupported');
  }

  const db = await PostgresDBRunner.startContainer({});

  const cli = spawn('npm', ['run', 'dev'], {
    cwd: path.join(process.cwd(), 'birdbox-test-project'),
    // Show output of Cube.js process in console
    stdio: 'inherit'
  });

  await pausePromise(5 * 1000);

  return {
    stop: async () => {
      console.log('[Birdbox] Closing');

      await db.stop();

      console.log('[Birdbox] Done with DB');

      await cli.kill('SIGTERM');

      console.log('[Birdbox] Closed');
    },
    configuration: {
      playgroundUrl: 'http://127.0.0.1:4000',
      apiUrl: 'http://127.0.0.1:4000/cubejs-api/v1',
    },
  };
}
