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
// eslint-disable-next-line import/no-extraneous-dependencies
import globby from 'globby';
// eslint-disable-next-line import/no-extraneous-dependencies
import { parse as parseYaml } from 'yaml';
import { uniq } from 'ramda';
import internal from 'stream';
import { REQUIRED_ENV_VARS } from './REQUIRED_ENV_VARS';

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
  LOCAL = 'local',
  DOCKER = 'docker',
}

/**
 * Arguments interface.
 */
interface Args {
  mode: Mode,
  log: Log,
}

export type DriverType = 'postgresql' | 'postgres' | 'multidb' | 'materialize' | 'crate' | 'bigquery' | 'athena' | 'postgresql-cubestore' | 'firebolt' | 'questdb' | 'redshift';

export type Schemas = string[];

/**
 * Birdbox options for container mode.
 */
export interface ContainerOptions {
  type: DriverType;
  env?: Record<string, string | undefined>;
  log?: Log;
  loadScript?: string;
  schemas?: Schemas,
}

/**
 * Birdbox options for local/cli mode.
 */
export interface LocalOptions extends ContainerOptions {
  schemaDir?: string
  cubejsConfig?: string
  useCubejsServerBinary?: boolean
}

type RequiredEnv = {
  CUBEJS_DEV_MODE: string,
  CUBEJS_WEB_SOCKETS: string,
  CUBEJS_EXTERNAL_DEFAULT: string,
  CUBEJS_SCHEDULED_REFRESH_DEFAULT: string,
  CUBEJS_REFRESH_WORKER: string,
  CUBEJS_ROLLUP_ONLY: string,
};

type OptionalEnv = {
  // SQL API
  CUBEJS_SQL_PORT?: string,
  CUBEJS_SQL_USER?: string,
  CUBEJS_PG_SQL_PORT?: string,
  CUBEJS_SQL_PASSWORD?: string,
  CUBEJS_SQL_SUPER_USER?: string,
};

const driverNameToFolderNameMapper: Record<DriverType, string> = {
  postgresql: 'postgresql',
  postgres: 'postgresql',
  multidb: 'postgresql',
  materialize: 'postgresql',
  crate: 'postgresql',
  bigquery: 'postgresql',
  athena: 'postgresql',
  'postgresql-cubestore': 'postgresql',
  firebolt: 'postgresql',
  questdb: 'postgresql',
  redshift: 'postgresql',
};

/**
 * Birdbox environments for cube.js passed for testcase.
 */
export type Env = RequiredEnv & OptionalEnv & Record<string, string | undefined>;
/**
 * List of permanent test data files.
 */
const FILES = [
  'CAST.js',
  'Customers.sql.js',
  'ECommerce.sql.js',
  'Products.sql.js',
];

/**
 * List of test schemas needs to be patched for certain datasource.
 */
const SCHEMAS = [
  'Customers.js',
  'ECommerce.js',
  'Products.js',
];

/**
 * Test data files source folder.
 */
const SOURCE = path.join(
  process.cwd(),
  'birdbox-fixtures',
  'driver-test-data',
);

/**
 * Test data files target source.
 */
const getTargetFolder = (type: DriverType) => path.join(
  process.cwd(),
  'birdbox-fixtures',
  driverNameToFolderNameMapper[type],
  'schema',
);

const extendsFiles = globby.sync(
  `${SOURCE}/**/*.js`,
  { objectMode: true, ignore: SCHEMAS.concat(FILES).map(f => path.join(SOURCE, f)) }
)
  .map(glob => glob.name);

/**
 * Remove test data files from target directory.
 */
function clearTestData(type: DriverType) {
  const targetFolder = getTargetFolder(type);
  FILES.concat(SCHEMAS).concat(extendsFiles).forEach((name) => {
    if (fs.existsSync(path.join(targetFolder, name))) {
      fs.removeSync(path.join(targetFolder, name));
    }
  });
}

function runSchemasGeneration(type: DriverType, schemas: Schemas) {
  const targetFolder = getTargetFolder(type);

  schemas.forEach((s) => {
    const originalContent = fs.readFileSync(
      path.join(SOURCE, s), 'utf8'
    );

    const { base } = path.parse(s);
    const updatedContent = originalContent.replace('_type_', type);
    fs.writeFileSync(
      path.join(targetFolder, base),
      updatedContent
    );
  });
}

/**
 * Prepare and copy test data files.
 */
function prepareTestData(type: DriverType, schemas?: Schemas) {
  const targetFolder = getTargetFolder(type);
  clearTestData(type);
  
  if (schemas) {
    runSchemasGeneration(type, schemas);
  } else {
    FILES.forEach((name) => {
      fs.copySync(
        path.join(SOURCE, name),
        path.join(targetFolder, name),
      );
    });
    SCHEMAS.forEach((name) => {
      const originalContent = fs.readFileSync(
        path.join(SOURCE, name), 'utf8'
      );
  
      const updatedContent = originalContent.replace('_type_', type);
      fs.writeFileSync(
        path.join(targetFolder, name),
        updatedContent
      );
    });
  }
}

/**
 * Birdbox object interface.
 */
export interface BirdBox {
  stop: () => Promise<void>;
  stdout: internal.Readable | null;
  configuration: {
    playgroundUrl: string;
    apiUrl: string;
    systemUrl: string;
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
      stdout: null,
      configuration: {
        playgroundUrl: `http://${host}:${port}`,
        apiUrl: `http://${host}:${port}/cubejs-api/v1`,
        systemUrl: `http://${host}:${port}/cubejs-system/v1`,
        wsUrl: `ws://${host}:${port}`,
      },
    };
  }

  if (process.env.BIRDBOX_CUBEJS_VERSION === undefined) {
    process.env.BIRDBOX_CUBEJS_VERSION = 'latest';
    const tag = `${process.env.BIRDBOX_CUBEJS_REGISTRY_PATH}cubejs/cube:${process.env.BIRDBOX_CUBEJS_VERSION}`;
    if (
      execInDir(
        '../..',
        `docker build . -f packages/cubejs-docker/dev.Dockerfile -t ${tag}`
      ) !== 0
    ) {
      throw new Error('[Birdbox] Docker build failed.');
    }
  }

  if (process.env.BIRDBOX_CUBESTORE_VERSION === undefined) {
    process.env.BIRDBOX_CUBESTORE_VERSION = 'latest';
  }
  
  const composeFileName = `${options.type}.yml`;
  const composeFilePath = path.resolve(path.dirname(__filename), '../../birdbox-fixtures/');
  let dc: DockerComposeEnvironment;
  if (options.schemas) {
    const dockerComposeFileContent = fs.readFileSync(path.join(composeFilePath, composeFileName), 'utf8');
    const yamlContent = parseYaml(dockerComposeFileContent);

    if (!yamlContent?.services?.cube?.volumes) {
      throw new Error('there is no services.cube.volumes in your docker compose');
    }

    options.schemas.forEach(s => {
      yamlContent.services.cube.volumes.push(`./${options.type}/schema/${s}:/cube/conf/schema/${s}`);
    });
    yamlContent.services.cube.volumes = uniq(yamlContent.services.cube.volumes);

    const newComposeFileName = `${options.type}.json`;
    fs.writeFileSync(path.join(composeFilePath, newComposeFileName), JSON.stringify(yamlContent));
    
    dc = new DockerComposeEnvironment(
      composeFilePath,
      newComposeFileName
    );
  } else {
    dc = new DockerComposeEnvironment(
      composeFilePath,
      composeFileName
    );
  }

  if (options.env) {
    for (const k of Object.keys(options.env)) {
      const val = options.env[k];
      if (val) {
        dc = dc.withEnv(k, val);
      }
    }
  }
  if (options.log === Log.PIPE) {
    process.stdout.write(
      `[Birdbox] Using ${composeFileName} compose file\n`
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
  const cubeStdout = await env.getContainer('birdbox-cube').logs();
  const port = env.getContainer('birdbox-cube').getMappedPort(4000);
  const playgroundPort = process.env.TEST_PLAYGROUND_PORT ?? port;
  let proxyServer: HttpProxy | null = null;

  if (process.env.TEST_PLAYGROUND_PORT) {
    if (options.log === Log.PIPE) {
      process.stdout.write(
        `[Birdbox] Creating a proxy server 4000->${port} for local testing\n`
      );
    }

    // As local Playground proxies requests to the 4000 port
    proxyServer = HttpProxy.createProxyServer({
      target: `http://localhost:${port}`
    }).listen(4000);
    proxyServer.on('error', async (err, req, res) => {
      process.stderr.write(`[Proxy Server] error: ${err}\n`);
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
      }
      await env.down();
      process.stderr.write(
        `[Birdbox] Script ${loadScript} finished with error: ${exitCode}\n`
      );
      process.exit(1);
    }
  }

  return {
    stdout: cubeStdout,
    stop: async () => {
      clearTestData(options.type);
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
      systemUrl: `http://${host}:${port}/cubejs-system/v1`,
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

  if (!options.schemaDir) {
    options.schemaDir = 'postgresql/schema';
  }

  if (!options.cubejsConfig) {
    options.cubejsConfig = 'postgresql/single/cube.js';
  }

  if (options.loadScript) {
    db = await PostgresDBRunner.startContainer({
      volumes: [
        {
          source: path.join(__dirname, '..', '..', 'birdbox-fixtures', 'datasets'),
          target: '/data',
          bindMode: 'ro',
        },
        {
          source: path.join(__dirname, '..', '..', 'birdbox-fixtures', 'postgresql', 'scripts'),
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

    if (exitCode !== 0) {
      if (options.log === Log.PIPE) {
        process.stdout.write(`${output}\n`);
      }
      await db.stop();
      process.stderr.write(
        `[Birdbox] Script ${loadScript} finished with error: ${exitCode}\n`
      );
      process.exit(1);
    }
    if (options.log === Log.PIPE) {
      process.stdout.write(
        `[Birdbox] Script ${loadScript} finished successfully\n`
      );
    }
  }

  const testDir = path.join(process.cwd(), 'birdbox-test-project');

  if (!options.useCubejsServerBinary) {
    // cli mode, using a project created via cli
    if (!fs.existsSync(testDir)) {
      execInDir('.', 'npx cubejs-cli create birdbox-test-project -d postgres');
    }
  }

  // Do not remove whole dir as it contains node_modules
  if (fs.existsSync(path.join(testDir, '.env'))) {
    fs.unlinkSync(path.join(testDir, '.env'));
  }

  if (fs.existsSync(path.join(testDir, '.cubestore'))) {
    fs.removeSync(path.join(testDir, '.cubestore'));
  }

  if (fs.existsSync(path.join(testDir, 'schema'))) {
    fs.removeSync(path.join(testDir, 'schema'));
  }

  // Ignored if not explicitly required by a schema file.
  fs.copySync(
    path.join(process.cwd(), 'birdbox-fixtures', 'postgresql', 'dbt-project'),
    path.join(testDir, 'dbt-project')
  );

  if (options.schemaDir) {
    fs.copySync(
      path.join(process.cwd(), 'birdbox-fixtures', options.schemaDir),
      path.join(testDir, 'schema')
    );
  }

  if (options.cubejsConfig) {
    fs.copySync(
      path.join(process.cwd(), 'birdbox-fixtures', options.cubejsConfig),
      path.join(testDir, 'cube.js')
    );
  }

  const env = {
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
  };

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
        env,
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
    process.stdout.write(`Error spawning cube: ${e}\n`);
    // @ts-ignore
    db.stop();
  }

  return {
    // @ts-expect-error
    stdout: cli.stdout,
    stop: async () => {
      clearTestData(options.type);
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
      systemUrl: 'http://127.0.0.1:4000/cubejs-system/v1',
      wsUrl: 'ws://127.0.0.1:4000',
    },
  };
}

export interface BirdboxOptions {
   // Schema directory. LOCAL mode.
  schemaDir?: string,
  // Config file. LOCAL mode.
  cubejsConfig?: string,
  schemas?: Schemas,
}

/**
 * Returns birdbox.
 */
export async function getBirdbox(
  type: DriverType,
  env: Env,
  options?: BirdboxOptions,
) {
  // default options
  if (!options) {
    options = {};
  }

  // extract mode
  const args: Args = yargs(process.argv.slice(2))
    .exitProcess(false)
    .options({
      mode: {
        describe: 'Determines Birdbox mode.',
        choices: [
          Mode.CLI,
          Mode.LOCAL,
          Mode.DOCKER,
        ],
        default: Mode.LOCAL,
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
  if (REQUIRED_ENV_VARS[type] === undefined) {
    if (log === Log.PIPE) {
      process.stderr.write(
        `Error: list of required environment variables is missing for ${type}\n`
      );
    }
    process.exit(1);
  } else {
    REQUIRED_ENV_VARS[type].forEach((key: string) => {
      if (process.env[key] === undefined) {
        process.stderr.write(
          `Error: ${key} is required environment variable for ${type}\n`
        );
        process.exit(1);
      } else {
        // @ts-ignore
        env[key] = process.env[key];
      }
    });
  }

  // prepare test data
  prepareTestData(type, options?.schemas);

  // birdbox instantiation
  let birdbox;
  try {
    switch (mode) {
      case Mode.CLI:
      case Mode.LOCAL: {
        birdbox = await startBirdBoxFromCli({
          type,
          env,
          log,
          cubejsConfig: options.cubejsConfig,
          schemaDir: options.schemaDir,
          useCubejsServerBinary: mode === Mode.LOCAL,
        });
        break;
      }
      case Mode.DOCKER: {
        birdbox = await startBirdBoxFromContainer({
          type: type === 'postgres' ? 'postgresql' : type,
          log,
          env,
          schemas: options?.schemas,
        });
        break;
      }
      default: {
        process.stderr.write(
          `Error: unsupported Birdbox mode: ${mode}\n`
        );
        process.exit(1);
      }
    }
  } catch (e) {
    clearTestData(type);
    process.stderr.write(e.toString());
    process.exit(1);
  }
  return birdbox;
}
