/**
 * Executes the passed command only if there are changes to the index that are within
 * the specified directory.
 * This is to prevent execution of ALL precommit hooks everytime a commit is done in the
 * repository.
 * @description Executes the passed command only if there are changes to the index that are
 * within the specified directory.
 * @module scripts/gitExecuteLocally
 * @author Philippe Hebert <philippe@goarthur.ai>
 */
const path = require('path');
const { spawn } = require('child_process');
const program = require('commander');
const git = require('simple-git/promise');
const chalk = require('chalk');
const logSymbols = require('log-symbols');

program
  .version('0.0.1')
  .option('-e, --exec <command>', 'Command to execute. Defaults to `npm run test`')
  .option(
    '-d, --directory <dir>',
    'Directory to observe relative to root. Defaults to current working directory (where the package.json is located).'
  )
  .option(
    '-r, --root <dir>',
    'Relative path to root from current working directory. Defaults to "../"'
  )
  .parse(process.argv);

gitExecuteLocally(program);

// ////////////////////////////////////////////////

async function gitExecuteLocally(args) {
  try {
    const command = args.exec || 'npm run test';
    const rootDir = path.resolve(process.cwd(), args.root || '../');
    const directory = args.directory || process.cwd();
    const localDir = directory.replace(`${rootDir}/`, '');
    const absoluteDir = path.resolve(rootDir, localDir);
    console.log();
    console.log(
      chalk.bold(`node scripts/${path.basename(__filename)} -e "${command}" -d ${directory}`)
    );
    console.log();
    if (process.env.DEBUG === 'true') {
      console.log(command);
      console.log({ rootDir, directory, localDir, absoluteDir });
    }
    process.stdout.write('Checking status...');

    const status = await git(rootDir).status();
    const files = []
      .concat(
        status.created,
        status.deleted,
        status.modified,
        status.renamed.map(({ from, to }) => [from, to]).reduce((acc, arr) => [...acc, ...arr], [])
      )
      .map(makeAbsolute(rootDir));
    process.stdout.clearLine();
    process.stdout.cursorTo(0);

    if (files.some(isSubDirectory(absoluteDir))) {
      console.log(`${logSymbols.success} Some files have been changed: running command.\n`);
      await execute(command, { stdio: 'inherit' });
    } else {
      console.log(`${logSymbols.info} No changes detected within "${localDir}/*". Exiting.\n`);
    }

    process.exit(0);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
}

function makeAbsolute(absoluteRoot) {
  return dirRelativeToRoot => path.resolve(absoluteRoot, dirRelativeToRoot);
}

function isSubDirectory(parent) {
  return dir => {
    const relative = path.relative(parent, dir);
    return relative && !relative.startsWith('..') && !path.isAbsolute(relative);
  };
}

function execute(command, opts = { stdio: 'pipe' }) {
  return new Promise((resolve, reject) => {
    const commandParts = command.split(' ');
    const cp = spawn(commandParts[0], commandParts.slice(1), { ...opts, shell: true });
    let stdout = '';
    let stderr = '';
    if (opts.stdio === 'pipe') {
      cp.stdout.on('data', chunk => {
        stdout += chunk;
      });
      cp.stderr.on('data', chunk => {
        stderr += chunk;
      });
    }
    cp.on('close', code => {
      if (code === 0) {
        resolve(stdout);
      } else {
        reject(stderr);
      }
    });

    cp.on('error', reject);
  });
}
