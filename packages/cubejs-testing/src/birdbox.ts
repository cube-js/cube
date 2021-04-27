import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';
import HttpProxy from 'http-proxy';
import { DockerComposeEnvironment } from 'testcontainers';
import { pausePromise } from '@cubejs-backend/shared';
import fsExtra from 'fs-extra';

import { PostgresDBRunner } from './db/postgres';
import { getLocalHostnameByOs } from './utils';

export interface BirdBoxTestCaseOptions {
  name: string;
}

export interface BirdBox {
  stop: () => Promise<void>;
  configuration: {
    playgroundUrl: string;
    apiUrl: string;
    wsUrl: string;
    env?: Record<string, string>;
  };
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
        apiUrl: `http://${host}:${port}/cubejs-api/v1`,
        wsUrl: `ws://${host}:${port}`,
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
  const playgroundPort = process.env.TEST_PLAYGROUND_PORT ? process.env.TEST_PLAYGROUND_PORT : port;

  let proxyServer: HttpProxy | null = null;

  if (process.env.TEST_PLAYGROUND_PORT) {
    console.log(`[Birdbox] Creating a proxy server 4000->${port} for local testing`);
    // As local Playground proxies requests to the 4000 port
    proxyServer = HttpProxy.createProxyServer({ target: `http://localhost:${port}` }).listen(4000);

    proxyServer.on('error', async (err, req, res) => {
      console.log('[Proxy Server] error:', err);

      if (!res.headersSent) {
        res.writeHead(500, { 'content-type': 'application/json' });
      }

      res.end(JSON.stringify({ error: err.message }));
    });
  }

  {
    console.log('[Birdbox] Executing load.sh script');

    const { output, exitCode } = await env.getContainer('birdbox-db').exec(['/scripts/load.sh']);

    if (exitCode === 0) {
      console.log('[Birdbox] Script load.sh finished successfully');
    } else {
      console.log(output);

      console.log(`[Birdbox] Script load.sh finished with error: ${exitCode}`);

      await env.down();

      process.exit(1);
    }
  }

  return {
    stop: async () => {
      console.log('[Birdbox] Closing');

      await env.down();
      proxyServer?.close();

      console.log('[Birdbox] Closed');
    },
    configuration: {
      playgroundUrl: `http://${host}:${playgroundPort}`,
      apiUrl: `http://${host}:${port}/cubejs-api/v1`,
      wsUrl: `ws://${host}:${port}`,
      env: {
        ...(process.env.TEST_PLAYGROUND_PORT ? { CUBEJS_DB_HOST: getLocalHostnameByOs() } : null),
      },
    },
  };
}

export interface StartCliWithEnvOptions {
  dbType: string;
}

export async function startBirdBoxFromCli(options: StartCliWithEnvOptions): Promise<BirdBox> {
  if (options.dbType !== 'postgresql') {
    throw new Error('Unsupported');
  }

  const db = await PostgresDBRunner.startContainer({
    volumes: [
      {
        source: path.join(__dirname, '..', '..', 'birdbox-fixtures', 'datasets'),
        target: '/data',
        bindMode: 'ro',
      },
      {
        source: path.join(__dirname, '..', '..', 'birdbox-fixtures', options.dbType, 'scripts'),
        target: '/scripts',
        bindMode: 'ro',
      },
    ],
  });

  {
    console.log('[Birdbox] Executing load.sh script');

    const { output, exitCode } = await db.exec(['/scripts/load.sh']);

    if (exitCode === 0) {
      console.log('[Birdbox] Script load.sh finished successfully');
    } else {
      console.log(output);

      console.log(`[Birdbox] Script load.sh finished with error: ${exitCode}`);

      await db.stop();

      process.exit(1);
    }
  }

  const testDir = path.join(process.cwd(), 'birdbox-test-project');

  if (fs.existsSync(path.join(testDir, '.env'))) {
    fs.unlinkSync(path.join(testDir, '.env'));
  }

  fsExtra.copySync(
    path.join(process.cwd(), 'birdbox-fixtures', options.dbType, 'schema'),
    path.join(testDir, 'schema')
  );

  const cli = spawn('npm', ['run', 'dev'], {
    cwd: testDir,
    shell: true,
    // Show output of Cube.js process in console
    stdio: ['pipe', 'pipe', 'pipe'],
    env: {
      ...process.env,
      CUBEJS_DB_TYPE: 'postgres',
      CUBEJS_DB_HOST: db.getHost(),
      CUBEJS_DB_PORT: `${db.getMappedPort(5432)}`,
      CUBEJS_DB_NAME: 'test',
      CUBEJS_DB_USER: 'test',
      CUBEJS_DB_PASS: 'test',
      CUBEJS_DEV_MODE: 'true',
      CUBEJS_WEB_SOCKETS: 'true',
      CUBEJS_API_SECRET: 'mysupersecret',
    },
  });
  // cli.stdout.on('data', (msg) => {
  //   console.log(msg.toString());
  // });
  cli.stderr.on('data', (msg) => {
    console.log(msg.toString());
  });
  // cli.on('exit', (code) => {
  //   throw new Error(
  //     `Cube.js exited with ${code}`
  //   );
  // });

  await pausePromise(10 * 1000);

  return {
    stop: async () => {
      console.log('[Birdbox] Closing');

      await db.stop();

      console.log('[Birdbox] Done with DB');

      cli.kill();

      console.log('[Birdbox] Closed');
    },
    configuration: {
      playgroundUrl: 'http://127.0.0.1:4000',
      apiUrl: 'http://127.0.0.1:4000/cubejs-api/v1',
      wsUrl: 'ws://127.0.0.1:4000',
    },
  };
}
