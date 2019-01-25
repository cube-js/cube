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

const createApp = async (projectName, options) => {
  if (!options.dbType) {
    console.error(
      chalk.red(`${chalk.green('--db-type')} option is required`)
    );
    return;
  }
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
  process.chdir(projectName);

  console.log('- Creating project structure');
  await writePackageJson({
    name: projectName,
    version: '0.0.1',
    private: true,
  });
  await fs.writeFile('index.js', indexJs);
  await fs.ensureDir('schema');

  const dependencies = ['@cubejs-backend/server', '@cubejs-backend/jdbc-driver', 'node-java-maven'];
  console.log('- Installing dependencies');
  await executeCommand('npm', ['install', '--save'].concat(dependencies));
  const JDBCDriver = require(path.join(process.cwd(), 'node_modules', '@cubejs-backend', 'jdbc-driver', 'driver', 'JDBCDriver'));
  let dbTypeDescription = JDBCDriver.dbTypeDescription(options.dbType);
  console.log(dbTypeDescription);

  const packageJson = await fs.readJson('package.json');
  if (dbTypeDescription.mavenDependency) {
    packageJson.java = {
      dependencies: [dbTypeDescription.mavenDependency]
    }
  }
  packageJson.scripts = packageJson.scripts || {};
  packageJson.scripts.install = './node_modules/.bin/node-java-maven';
  await writePackageJson(packageJson);

  console.log('- Installing JDBC dependencies');
  await executeCommand('npm', ['install']);

  console.log('- Creating default configuration');
  await fs.writeFile('.env', dotEnv + `CUBEJS_DB_TYPE=${options.dbType}\nCUBEJS_API_SECRET=${crypto.randomBytes(64).toString('hex')}\n`);

  console.log(`- ${chalk.green(projectName)} app has been created 🎉`);
};

program
  .command('create <project-name>')
  .description('create cube.js app')
  .option('-d, --db-type <db-type>', 'Database type. Can be: postgres, mysql.')
  .action(createApp);

program.parse(process.argv);