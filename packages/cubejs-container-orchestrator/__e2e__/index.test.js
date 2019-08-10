const path = require('path');
const { spawn } = require('child_process');
const axios = require('axios');
const io = require('./__fixtures__/socket');
const getSubprocessEnvironment = require('./__fixtures__/getSubprocessEnvironment');

const ENV_FILTER_REGEXP = /^CUBEJS_/i;
let containerOrchestratorEnv;
let containerOrchestratorProcess;
let containerOrchestratorUrl;
let socket;

beforeAll(async () => {
  containerOrchestratorEnv = getSubprocessEnvironment(ENV_FILTER_REGEXP);
  containerOrchestratorUrl = `http://localhost:${containerOrchestratorEnv.CUBEJS_TEST_PORT}`;
  // I may not need to have a detached version right away.
  // I should probably stash this away and simply use the normal
  // spawn method.
  // spawnDetached('node', [path.resolve(__dirname, '../../src/index.js')] );
  containerOrchestratorProcess = spawn('node', [path.resolve(__dirname, '../src/index.js')], {
    stdio: 'inherit',
    shell: true,
    env: containerOrchestratorEnv,
  });
  socket = await io(containerOrchestratorUrl);
});

afterAll(async () => {
  containerOrchestratorProcess.unref();
});

it('should start an express server at specified port', async () => {
  // act
  const { data } = await axios.get(containerOrchestratorUrl);
  // assert
  expect(data).toBe('Hello World!');
});

it('should not allow multiple processes to start at the same time given the same port', async () => {
  // arrange
  // act
  const child = spawn('node', [path.resolve(__dirname, '../src/index.js')], {
    stdio: 'pipe',
    shell: true,
    env: containerOrchestratorEnv,
  });
  child.stderr.on('data', (data) => {
    // assert
    expect(data.toString('utf8')).toMatch('EADDRINUSE :::32125');
    child.unref();
  });
});

it(`should close after ${process.env.CUBEJS_TEST_EXIT_TIMEOUT}ms when all socket connections close`, async () => {
  let res;
  try {
    // act
    res = await axios.get(containerOrchestratorUrl);
    socket.disconnect();
    await new Promise(resolve => setTimeout(
      resolve,
      parseInt(containerOrchestratorEnv.CUBEJS_TEST_EXIT_TIMEOUT, 10) + 250
    ));
    await axios.get(containerOrchestratorUrl);
  } catch (err) {
    // assert
    expect(res).not.toBe(undefined);
    expect(err.code).toBe('ECONNREFUSED');
  }
});
