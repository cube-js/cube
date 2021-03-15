import { DockerComposeEnvironment } from 'testcontainers';
import path from 'path';

export interface BirdBoxTestCaseOptions {
  name: string
}

// @todo Move this to @cubejs-backend/testing
export async function startBidBoxContainer(options: BirdBoxTestCaseOptions) {
  if (process.env.TEST_CUBE_HOST) {
    const host = process.env.TEST_CUBE_HOST || 'localhost';
    const port = process.env.TEST_CUBE_PORT || '8888';

    return {
      env: null,
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
    .withEnv('BIRDBOX_CUBEJS_VERSION', process.env.BIRDBOX_CUBEJS_VERSION || 'latest')
    .up();

  const host = '127.0.0.1';
  const port = env.getContainer('birdbox-cube').getMappedPort(4000);

  return {
    env,
    configuration: {
      playgroundUrl: `http://${host}:${port}`,
      apiUrl: `http://${host}:${port}/cubejs-api/v1`,
    },
  };
}
