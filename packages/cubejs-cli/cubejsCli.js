const program = require('commander');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const chalk = require('chalk');
const spawn = require('cross-spawn');
const crypto = require('crypto');

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

const indexJs = `const CubejsServer = require('@cubejs-backend/server');

const server = new CubejsServer();

server.listen().then(({ port }) => {
  console.log(\`Cube.js server listening on \${port}\`);
});
`;

const dotEnv = `CUBEJS_DB_HOST=<YOUR_DB_HOST_HERE>
CUBEJS_DB_NAME=<YOUR_DB_NAME_HERE>
CUBEJS_DB_USER=<YOUR_DB_USER_HERE>
CUBEJS_DB_PASS=<YOUR_DB_PASS_HERE>
`;

const writePackageJson = async (packageJson) => {
  return fs.writeJson('package.json', packageJson, {
    spaces: 2,
    EOL: os.EOL
  });
};

const displayError = (text) => {
  console.error('');
  console.error(chalk.cyan('Cube.js Error ---------------------------------------'));
  console.error('');
  console.error(text)
  console.error('');
};

const requireFromPackage = (module) => require(path.join(process.cwd(), 'node_modules', module));

const npmInstall = (dependencies) => {
  return executeCommand('npm', ['install', '--save'].concat(dependencies));
};

const logStage = (stage) => {
  console.log(`- ${stage}`);
};

const createApp = async (projectName, options) => {
  if (!options.dbType) {
    displayError("You must pass an application name and a database type (-d).");
    process.exit(1);
  }
  if (await fs.pathExists(projectName)) {
    console.error(
      chalk.red(
        `We cannot create a project called ${chalk.green(
          projectName
        )}: directory already exist.\n`
      )
    );
    process.exit(1);
  }
  await fs.ensureDir(projectName);
  process.chdir(projectName);

  logStage('Creating project structure');
  await writePackageJson({
    name: projectName,
    version: '0.0.1',
    private: true,
  });
  await fs.writeFile('index.js', indexJs);
  await fs.ensureDir('schema');

  logStage('Installing server dependencies');
  await npmInstall(['@cubejs-backend/server']);

  logStage('Installing DB driver dependencies');
  const CubejsServer = requireFromPackage('@cubejs-backend/server');
  let driverDependencies = CubejsServer.driverDependencies(options.dbType);
  driverDependencies = Array.isArray(driverDependencies) ? driverDependencies : [driverDependencies];
  if (driverDependencies[0] === '@cubejs-backend/jdbc-driver') {
    driverDependencies.push('node-java-maven')
  }
  await npmInstall(driverDependencies);

  if (driverDependencies[0] === '@cubejs-backend/jdbc-driver') {
    logStage('Installing JDBC dependencies');
    const JDBCDriver = require(path.join(process.cwd(), 'node_modules', '@cubejs-backend', 'jdbc-driver', 'driver', 'JDBCDriver'));
    const dbTypeDescription = JDBCDriver.dbTypeDescription(options.dbType);
    if (!dbTypeDescription) {
      console.error(
        chalk.red(
          `Unsupported db type: ${chalk.green(options.dbType)}`
        )
      );
      process.exit(1);
    }

    const packageJson = await fs.readJson('package.json');
    if (dbTypeDescription.mavenDependency) {
      packageJson.java = {
        dependencies: [dbTypeDescription.mavenDependency]
      }
    }
    packageJson.scripts = packageJson.scripts || {};
    packageJson.scripts.install = './node_modules/.bin/node-java-maven';
    await writePackageJson(packageJson);

    await executeCommand('npm', ['install']);
  }

  logStage('Creating default configuration');
  await fs.writeFile('.env', dotEnv + `CUBEJS_DB_TYPE=${options.dbType}\nCUBEJS_API_SECRET=${crypto.randomBytes(64).toString('hex')}\n`);

  logStage(`${chalk.green(projectName)} app has been created 🎉`);
};

program
  .command('create <name>')
  .description('create new Cube.js app')
  .option('-d, --db-type <db-type>', 'Preconfigure for selected database (options: postgres, mysql)')
  .action(createApp);


if (!process.argv.slice(2).length) {
  program.help();
}

program.parse(process.argv);
