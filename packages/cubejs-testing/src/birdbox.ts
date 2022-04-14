import path from 'path';
import fs from 'fs-extra';
import yargs from 'yargs/yargs';
import { ChildProcess, spawn } from 'child_process';
import HttpProxy from 'http-proxy';
import {
  DockerComposeEnvironment,
  StartedTestContainer,
} from 'testcontainers';
import {
  execInDir,
  pausePromise
} from '@cubejs-backend/shared';
import {
  getLocalHostnameByOs,
  PostgresDBRunner,
} from '@cubejs-backend/testing-shared';
import { REQ_ENV_VARS } from './REQ_ENV_VARS';

/**
 * Logging options defined in CLI.
 */
enum Log {
  NONE = 'ignore',
  PIPE = 'pipe',
}

/**
 * Birdbox modes defined in CLI.
 */
enum Mode {
  CLI = 'cli',
  LOC = 'local',
  DOCKER = 'docker',
}

/**
 * Arguments interface.
 */
interface Args {
  mode: Mode,
  log: Log,
}

/**
 * Birdbox options for container mode.
 */
export interface ContainerOptions {
  type: string;
  env?: Record<string, string>;
  log?: Log;
  loadScript?: string;
}

/**
 * Birdbox options for local/cli mode.
 */
export interface LocalOptions
  extends ContainerOptions {
  cubejsConfig?: string;
  useCubejsServerBinary?: boolean;
}

/**
 * Birdbox environments for cube.js passed for testcase.
 */
export interface Env {
  CUBEJS_DEV_MODE: string,
  CUBEJS_WEB_SOCKETS: string,
  CUBEJS_EXTERNAL_DEFAULT: string,
  CUBEJS_SCHEDULED_REFRESH_DEFAULT: string,
  CUBEJS_REFRESH_WORKER: string,
  CUBEJS_ROLLUP_ONLY: string,
  [key: string]: string,
}

/**
 * List of permanent test data files.
 */
const files = [
  'CAST.js',
  'Customers.sql.js',
  'ECommerce.sql.js',
  'Products.sql.js',
];

/**
 * List of test schemas needs to be patched for certain datasource.
 */
const schemas = [
  'Customers.js',
  'ECommerce.js',
  'Products.js',
];

/**
 * Test data files source folder.
 */
const source = path.join(
  process.cwd(),
  'birdbox-fixtures',
  'driver-test-data',
);

/**
 * Test data files target source.
 */
const target = path.join(
  process.cwd(),
  'birdbox-fixtures',
  'postgresql',
  'schema',
);

/**
 * Remove test data files from target directory.
 */
function clearTestData() {
  files.concat(schemas).forEach((name) => {
    if (fs.existsSync(path.join(target, name))) {
      fs.removeSync(path.join(target, name));
    }
  });
}

/**
 * Prepare and copy test data files.
 */
function prepareTestData(type: string) {
  clearTestData();
  files.forEach((name) => {
    fs.copySync(
      path.join(source, name),
      path.join(target, name),
    );
  });
  schemas.forEach((name) => {
    fs.writeFileSync(
      path.join(target, name),
      fs.readFileSync(
        path.join(source, name), 'utf8'
      ).replace('_type_', type)
    );
  });
}

/**
 * Birdbox object interface.
 */
export interface BirdBox {
  stop: () => Promise<void>;
  configuration: {
    playgroundUrl: string;
    apiUrl: string;
    wsUrl: string;
    env?: Record<string, string>;
  };
}

/**
 * Returns Birdbox with container mode.
 */
export async function startBirdBoxFromContainer(
  options: ContainerOptions
): Promise<BirdBox> {
  if (process.env.TEST_CUBE_HOST) {
    const host = process.env.TEST_CUBE_HOST || 'localhost';
    const port = process.env.TEST_CUBE_PORT || '8888';

    return {
      stop: async () => {
        process.stdout.write('[Birdbox] Closed\n');
      },
      configuration: {
        playgroundUrl: `http://${host}:${port}`,
        apiUrl: `http://${host}:${port}/cubejs-api/v1`,
        wsUrl: `ws://${host}:${port}`,
      },
    };
  }

  if (process.env.BIRDBOX_CUBEJS_REGISTRY_PATH === undefined) {
    process.env.BIRDBOX_CUBEJS_REGISTRY_PATH = 'localhost:5000/';
  }

  if (process.env.BIRDBOX_CUBEJS_VERSION === undefined) {
    process.env.BIRDBOX_CUBEJS_VERSION = 'latest';
    const tag = `${
      process.env.BIRDBOX_CUBEJS_REGISTRY_PATH
    }cubejs/cube:${
      process.env.BIRDBOX_CUBEJS_VERSION
    }`;
    if (
      execInDir(
        '../..',
        `docker build . -f packages/cubejs-docker/dev.Dockerfile -t ${
          tag
        }`
      ) !== 0
    ) {
      throw new Error('[Birdbox] Docker build failed.');
    }
  }

  if (process.env.BIRDBOX_CUBESTORE_VERSION === undefined) {
    process.env.BIRDBOX_CUBESTORE_VERSION = 'latest';
  }

  const composeFile = `${options.type}.yml`;
  let dc = new DockerComposeEnvironment(
    path.resolve(path.dirname(__filename), '../../birdbox-fixtures/'),
    composeFile
  );

  if (options.env) {
    for (const k of Object.keys(options.env)) {
      dc = dc.withEnv(k, options.env[k]);
    }
  }
  if (options.log === Log.PIPE) {
    process.stdout.write(
      `[Birdbox] Using ${composeFile} compose file\n`
    );
  }
  
  const env = await dc
    .withStartupTimeout(30 * 1000)
    .withEnv(
      'BIRDBOX_CUBEJS_VERSION',
      process.env.BIRDBOX_CUBEJS_VERSION
    )
    .withEnv(
      'BIRDBOX_CUBESTORE_VERSION',
      process.env.BIRDBOX_CUBESTORE_VERSION
    )
    .up();

  const host = '127.0.0.1';
  const port = env.getContainer('birdbox-cube').getMappedPort(4000);
  const playgroundPort = process.env.TEST_PLAYGROUND_PORT ?? port;
  let proxyServer: HttpProxy | null = null;

  if (process.env.TEST_PLAYGROUND_PORT) {
    if (options.log === Log.PIPE) {
      process.stdout.write(
        `[Birdbox] Creating a proxy server 4000->${
          port
        } for local testing\n`
      );
    }
    
    // As local Playground proxies requests to the 4000 port
    proxyServer = HttpProxy.createProxyServer({
      target: `http://localhost:${port}`
    }).listen(4000);
    proxyServer.on('error', async (err, req, res) => {
      if (options.log === Log.PIPE) {
        process.stderr.write(`[Proxy Server] error: ${err}\n`);
      }
      if (!res.headersSent) {
        res.writeHead(500, { 'content-type': 'application/json' });
      }
      res.end(JSON.stringify({ error: err.message }));
    });
  }

  if (options.loadScript) {
    const { loadScript } = options;
    if (options.log === Log.PIPE) {
      process.stdout.write(
        `[Birdbox] Executing ${loadScript} script\n`
      );
    }
    const {
      output,
      exitCode,
    } = await env
      .getContainer('birdbox-db')
      .exec([`/scripts/${loadScript}`]);

    if (exitCode === 0) {
      if (options.log === Log.PIPE) {
        process.stdout.write(
          `[Birdbox] Script ${loadScript} finished successfully\n`
        );
      }
    } else {
      if (options.log === Log.PIPE) {
        process.stdout.write(`${output}\n`);
        process.stderr.write(
          `[Birdbox] Script ${
            loadScript
          } finished with error: ${
            exitCode
          }\n`
        );
      }
      await env.down();
      process.exit(1);
    }
  }

  return {
    stop: async () => {
      clearTestData();
      if (options.log === Log.PIPE) {
        process.stdout.write('[Birdbox] Closing\n');
      }
      await env.down();
      proxyServer?.close();
      if (options.log === Log.PIPE) {
        process.stdout.write('[Birdbox] Closed\n');
      }
    },
    configuration: {
      playgroundUrl: `http://${host}:${playgroundPort}`,
      apiUrl: `http://${host}:${port}/cubejs-api/v1`,
      wsUrl: `ws://${host}:${port}`,
      env: {
        ...(
          process.env.TEST_PLAYGROUND_PORT
            ? { CUBEJS_DB_HOST: getLocalHostnameByOs() }
            : null
        ),
      },
    },
  };
}

/**
 * Returns Birdbox in cli/local mode.
 */
export async function startBirdBoxFromCli(
  options: LocalOptions
): Promise<BirdBox> {
  let db: StartedTestContainer;
  let cli: ChildProcess;
  if (options.loadScript) {
    db = await PostgresDBRunner.startContainer({
      volumes: [
        {
          source: path.join(
            __dirname,
            '..',
            '..',
            'birdbox-fixtures',
            'datasets'
          ),
          target: '/data',
          bindMode: 'ro',
        },
        {
          source: path.join(
            __dirname,
            '..',
            '..',
            'birdbox-fixtures',
            'postgresql',
            'scripts'
          ),
          target: '/scripts',
          bindMode: 'ro',
        },
      ],
    });

    if (options.log === Log.PIPE) {
      process.stdout.write('[Birdbox] Executing load script\n');
    }

    const loadScript = `/scripts/${options.loadScript}`;
    const { output, exitCode } = await db.exec([loadScript]);

    if (exitCode === 0 && options.log === Log.PIPE) {
      process.stdout.write(
        `[Birdbox] Script ${
          loadScript
        } finished successfully\n`
      );
    } else {
      if (options.log === Log.PIPE) {
        process.stdout.write(`${output}\n`);
        process.stdout.write(
          `[Birdbox] Script ${
            loadScript
          } finished with error: ${
            exitCode
          }\n`
        );
      }
      await db.stop();
      process.exit(1);
    }
  }

  const testDir = path.join(process.cwd(), 'birdbox-test-project');

  // Do not remove whole dir as it contains node_modules
  if (fs.existsSync(path.join(testDir, '.env'))) {
    fs.unlinkSync(path.join(testDir, '.env'));
  }

  if (fs.existsSync(path.join(testDir, '.cubestore'))) {
    fs.removeSync(path.join(testDir, '.cubestore'));
  }

  fs.copySync(
    path.join(process.cwd(), 'birdbox-fixtures', 'postgresql'),
    path.join(testDir)
  );

  if (options.cubejsConfig) {
    fs.copySync(
      path.join(
        process.cwd(),
        'birdbox-fixtures',
        'postgresql',
        options.cubejsConfig
      ),
      path.join(testDir, 'cube.js')
    );
  }

  try {
    cli = spawn(
      options.useCubejsServerBinary
        ? path.resolve(process.cwd(), '../cubejs-server/bin/server')
        : 'npm',
      options.useCubejsServerBinary
        ? []
        : ['run', 'dev'],
      {
        cwd: testDir,
        shell: true,
        detached: true,
        stdio: [
          options.log,
          options.log,
          options.log,
        ],
        env: {
          ...process.env,
          CUBEJS_DB_TYPE: options.type === 'postgresql'
            ? 'postgres'
            : options.type,
          CUBEJS_DEV_MODE: 'true',
          CUBEJS_API_SECRET: 'mysupersecret',
          CUBEJS_WEB_SOCKETS: 'true',
          CUBEJS_PLAYGROUND_AUTH_SECRET: 'mysupersecret',
          ...options.env
            ? options.env
            : {
              CUBEJS_DB_HOST: db!.getHost(),
              CUBEJS_DB_PORT: `${db!.getMappedPort(5432)}`,
              CUBEJS_DB_NAME: 'test',
              CUBEJS_DB_USER: 'test',
              CUBEJS_DB_PASS: 'test',
            }
        },
      }
    );
    if (cli.stdout) {
      cli.stdout.on('data', (msg) => {
        process.stdout.write(msg);
      });
    }
    if (cli.stderr) {
      cli.stderr.on('data', (msg) => {
        process.stdout.write(msg);
      });
    }
    await pausePromise(10 * 1000);
  } catch (e) {
    // @ts-ignore
    db.stop();
  }
  return {
    stop: async () => {
      clearTestData();
      if (options.log === Log.PIPE) {
        process.stdout.write('[Birdbox] Closing\n');
      }
      if (db) {
        await db.stop();
      }
      if (options.log === Log.PIPE) {
        process.stdout.write('[Birdbox] Done with DB\n');
      }
      process.kill(-cli.pid, 'SIGINT');
      if (options.log === Log.PIPE) {
        process.stdout.write('[Birdbox] Closed\n');
      }
    },
    configuration: {
      playgroundUrl: 'http://127.0.0.1:4000',
      apiUrl: 'http://127.0.0.1:4000/cubejs-api/v1',
      wsUrl: 'ws://127.0.0.1:4000',
    },
  };
}

/**
 * Returns birdbox.
 */
export async function getBirdbox(
  type: string,
  env: Env,
) {
  // extract mode
  const args: Args = yargs(process.argv.slice(2))
    .exitProcess(false)
    .options({
      mode: {
        describe: 'Determines Birdbox mode.',
        choices: [
          Mode.CLI,
          Mode.LOC,
          Mode.DOCKER,
        ],
        default: Mode.LOC,
      },
      log: {
        describe: 'Determines Birdbox logging.',
        choices: [
          Log.NONE,
          Log.PIPE,
        ],
        default: Log.PIPE,
      }
    })
    .argv as Args;
  const { mode, log } = args;

  // extract/assert env variables
  if (REQ_ENV_VARS[type] === undefined) {
    if (log === Log.PIPE) {
      process.stderr.write(
        `List of required environment variables is missing for ${
          type
        }\n`
      );
    }
    process.exit(1);
  } else {
    REQ_ENV_VARS[type].forEach((key: string) => {
      if (process.env[key] === undefined) {
        if (log === Log.PIPE) {
          process.stderr.write(
            `${key} is required environment variable for ${type}\n`
          );
        }
        process.exit(1);
      } else {
        // @ts-ignore
        env[key] = process.env[key];
      }
    });
  }

  // prepare test data
  prepareTestData(type);

  // birdbox instantiation
  let birdbox;
  try {
    switch (mode) {
      case Mode.CLI:
      case Mode.LOC: {
        birdbox = await startBirdBoxFromCli({
          type,
          env,
          log,
          cubejsConfig: 'single/cube.js',
          useCubejsServerBinary: mode === Mode.LOC,
        });
        break;
      }
      case Mode.DOCKER: {
        birdbox = await startBirdBoxFromContainer({
          type: type === 'postgres' ? 'postgresql' : type,
          log,
          env
        });
        break;
      }
      default: {
        if (log === Log.PIPE) {
          process.stderr.write(
            `Unsupported Birdbox mode: ${mode}\n`
          );
        }
        process.exit(1);
      }
    }
  } catch (e) {
    if (log === Log.PIPE) {
      process.stderr.write(e as string);
    }
    clearTestData();
    process.exit(1);
  }
  return birdbox;
}
