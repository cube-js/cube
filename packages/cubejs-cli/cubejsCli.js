const program = require('commander');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const chalk = require('chalk');
const spawn = require('cross-spawn');

const packageJson = require('./package.json');

program.name(Object.keys(packageJson.bin)[0])
  .version(packageJson.version);

const executeCommand = (command, args) => {
  const child = spawn(command, args, { stdio: 'inherit' });
  return new Promise((resolve, reject) => {
    child.on('close', code => {
      if (code !== 0) {
        reject({
          command: `${command} ${args.join(' ')}`,
        });
        return;
      }
      resolve();
    });
  })
};

const createApp = async (projectName) => {
  if (await fs.pathExists(projectName)) {
    console.error(
      chalk.red(
        `We cannot create a project called ${chalk.green(
          projectName
        )}: directory already exist.\n`
      )
    );
    return;
  }
  await fs.ensureDir(projectName);
  await fs.writeJson(path.join(projectName, 'package.json'), {
    name: projectName,
    version: '0.0.1',
    private: true,
  }, {
    spaces: 2,
    EOL: os.EOL
  });
};

program
  .command('create <project-name>')
  .description('create cube.js app')
  .action(createApp);

program.parse(process.argv);