const path = require('path');
const fs = require('fs-extra');
const { machineIdSync } = require('node-machine-id');
const chalk = require('chalk');
const track = require('./track');

const anonymousId = machineIdSync();

export const event = async (name: string, props: any) => {
  try {
    await track({
      event: name,
      anonymousId,
      ...props
    });
  } catch (e) {
    // ignore
  }
};

export const displayError = async (text: string|string[], options = {}) => {
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
  console.error(`${chalk.yellow('  Ask this question in Cube.js Slack:')} https://slack.cube.dev`);
  console.error(`${chalk.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');
  process.exit(1);
};

export const requireFromPackage = async (module: string) => {
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

export const logStage = async (stage: string, eventName: string, props: any) => {
  console.log(`- ${stage}`);
  if (eventName) {
    await event(eventName, props);
  }
};
