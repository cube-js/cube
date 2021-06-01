import color from '@oclif/color';
import process from 'process';

export const displayCLIError = async (text: string, pkg: string) => {
  console.error('');
  console.error(color.cyan(`Cube.js ${pkg} ---------------------------------------`));
  console.error('');

  console.error(text);

  console.error('');
  console.error(color.yellow('Need some help? -------------------------------------'));

  console.error('');
  console.error(`${color.yellow('  Ask this question in Cube.js Slack:')} https://slack.cube.dev`);
  console.error(`${color.yellow('  Post an issue:')} https://github.com/cube-js/cube.js/issues`);
  console.error('');

  process.exit(1);
};

export const displayCLIWarning = (message: string) => {
  console.log(`${color.yellow('Warning.')} ${message}`);
};
