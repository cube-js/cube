const path = require('path');
const { exec, ChildProcess } = require('child_process');
const { DockerComposeEnvironment, Wait } = require('testcontainers');

describe('cli', () => {
  jest.setTimeout(6 * 60 * 1000);

  let env: any;

  // eslint-disable-next-line consistent-return,func-names
  beforeAll(async () => {
    const dc = new DockerComposeEnvironment(
      path.resolve(path.dirname(__filename), '../../'),
      'docker-compose.yml'
    );

    env = await dc
      .withStartupTimeout(240 * 1000)
      .withWaitStrategy('postgres', Wait.forHealthCheck())
      .up();
  });

  // eslint-disable-next-line consistent-return,func-names
  afterAll(async () => {
    if (env) {
      await env.down();
    }
  });

  it('aggregation-warmup command', async () => {
    let result = await cli(
      [
        'CUBEJS_DEV_MODE=true',
        'CUBEJS_SCHEMA_PATH=mock/model',
        'CUBEJS_DB_TYPE=postgres',
        'CUBEJS_DB_HOST=localhost',
        'CUBEJS_DB_PORT=5454',
        'CUBEJS_DB_NAME=postgres',
        'CUBEJS_DB_USER=postgres',
        'CUBEJS_DB_PASS=postgres'
      ],
      ['aggregation-warmup'], 
      '.'
    );
    console.log(result.stdout);
    expect(result.code).toBe(0);
  });

  function cli(exports, args, cwd): Promise<typeof ChildProcess> {
    return new Promise(resolve => { 
      exec(`${exports.join(' ')} node ${path.resolve('./dist/src/cli')} ${args.join(' ')}`,
      { cwd }, 
      (error, stdout, stderr) => { resolve({
      code: error && error.code ? error.code : 0,
      error,
      stdout,
      stderr })
    })
  })}
});
