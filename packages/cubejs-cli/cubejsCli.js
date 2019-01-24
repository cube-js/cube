const program = require('commander');

const packageJson = require('./package.json');

program.name(Object.keys(packageJson.bin)[0])
  .version(packageJson.version);

program
  .command('create <project-name>')
  .description('create cube.js app')
  .action((projectName) => {
    console.log(projectName);
  });

program.parse(process.argv);