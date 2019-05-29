const path = require('path');
const fs = require('fs-extra');
const { machineIdSync } = require('node-machine-id');
const chalk = require('chalk');
const { promisify } = require('util');
const Analytics = require('analytics-node');

const client = new Analytics('dSR8JiNYIGKyQHKid9OaLYugXLao18hA', { flushInterval: 100 });

const anonymousId = machineIdSync();

const event = async (name, props) => {
  try {
    await promisify(client.track.bind(client))({
      event: name,
      anonymousId,
      properties: props
    });
    await promisify(client.flush.bind(client))();
  } catch (e) {
    // ignore
  }
};

exports.event = event;

const displayError = async (text, options = {}) => {
  console.error('');
  console.error(chalk.cyan('Cube.js Error ---------------------------------------'));
  console.error('');
  if (Array.isArray(text)) {
    text.forEach((str) => console.error(str));
  } else {
    console.error(text);
  }
  console.error('');
  console.error(chalk.yellow('Need some help? -------------------------------------'));
  await event('Error', { error: Array.isArray(text) ? text.join('\n') : text.toString(), ...options });
  console.error('');
  console.error(`${chalk.yellow(`  Ask this question in Cube.js Slack:`)} https://cubejs-community.herokuapp.com`);
  console.error(`${chalk.yellow(`  Post an issue:`)} https://github.com/statsbotco/cube.js/issues`);
  console.error('');
  process.exit(1);
};

exports.displayError = displayError;

exports.requireFromPackage = async (module) => {
  if (
    !(await fs.pathExists(path.join(process.cwd(), 'node_modules', module))) &&
    !(await fs.pathExists(path.join(process.cwd(), 'node_modules', `${module}.js`)))
  ) {
    await displayError(
      `${module} dependency not found. Please run this command from project directory.`
    );
  }

  // eslint-disable-next-line global-require,import/no-dynamic-require
  return require(path.join(process.cwd(), 'node_modules', module));
};
