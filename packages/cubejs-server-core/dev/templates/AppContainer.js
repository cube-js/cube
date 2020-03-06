const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');
const SourceContainer = require('./SourceContainer');

class AppContainer {
  constructor(appPath, templatePackages, templateConfig) {
    this.appPath = appPath;
    this.templatePackages = templatePackages;
    this.templateConfig = templateConfig;
    this.packageCache = {};
  }

  async applyTemplates() {
    if (!this.templatePackages || !this.templatePackages.length) {
      throw new Error(`templatePackages is required`);
    }
    const toApply = await this.templatePackages.map(
      templatePackage => async (packages) => packages.concat([await this.createTemplatePackage(templatePackage)])
    ).reduce((a, b) => a.then(b), Promise.resolve([]));

    this.initDependencies();

    const rootPackages = toApply.filter(root => !toApply.find(p => this.packagesToReceive(p).indexOf(root) !== -1));
    if (rootPackages.length > 1) {
      throw new Error(`Only one root allowed but found: ${rootPackages.map(p => p.name).join(', ')}`);
    }
    await rootPackages[0].applyPackage(await this.loadSources());
  }

  async loadSources() {
    return new SourceContainer(await AppContainer.fileContentsRecursive(this.appPath));
  }

  static async fileContentsRecursive(dir, rootPath, includeNodeModules) {
    if (!rootPath) {
      rootPath = dir;
    }
    if (!(await fs.pathExists(dir))) {
      return [];
    }
    if (dir.indexOf('node_modules') !== -1 && !includeNodeModules) {
      return [];
    }
    const files = await fs.readdir(dir);
    return (await Promise.all(files
      .map(async file => {
        const fileName = path.join(dir, file);
        const stats = await fs.lstat(fileName);
        if (!stats.isDirectory()) {
          const content = await fs.readFile(fileName, "utf-8");
          return [{
            fileName: fileName.replace(rootPath, '').replace(/\\/g, '/'),
            content
          }];
        } else {
          return AppContainer.fileContentsRecursive(fileName, rootPath, includeNodeModules);
        }
      }))).reduce((a, b) => a.concat(b), []);
  }

  async getPackageVersions() {
    return (await fs.readJson(path.join(this.appPath, 'package.json'))).cubejsTemplates || {};
  }

  async persistSources(sourceContainer, packageVersions) {
    const sources = sourceContainer.outputSources();
    await Promise.all(
      sources.map(file => fs.outputFile(
        path.join(this.appPath, file.fileName),
        file.content
      ))
    );
    const packageJson = await fs.readJson(path.join(this.appPath, 'package.json'));
    packageJson.cubejsTemplates = { ...packageJson.cubejsTemplates, ...packageVersions };
    await fs.writeJson(path.join(this.appPath, 'package.json'), packageJson, {
      spaces: 2
    });
  }

  async createTemplatePackage(templatePackage) {
    if (!this.packageCache[templatePackage]) {
      // eslint-disable-next-line global-require,import/no-dynamic-require
      const TemplatePackageClass = require(`./packages/${templatePackage}`);
      const template = new TemplatePackageClass(this.templateConfig[templatePackage]);
      template.appContainer = this;
      template.scaffoldingPath = path.join(__dirname, `packages/${templatePackage}/scaffolding`);
      await template.initSources();
      this.packageCache[templatePackage] = template;
    }
    return this.packageCache[templatePackage];
  }

  initDependencies() {
    this.templatePackages.forEach(p => {
      const template = this.packageCache[p];
      if (template.requires) {
        const requiredPackage = this.packageCache[template.requires];
        if (!requiredPackage) {
          throw new Error(`Required package not found within selected template packages: ${requiredPackage}`);
        }
        requiredPackage.receivesPackages = requiredPackage.receivesPackages || [];
        if (requiredPackage.receivesPackages.indexOf(template) === -1) {
          requiredPackage.receivesPackages.push(template);
        }
      }
      if (template.receives) {
        template.receivesPackages = template.receivesPackages || [];
        template.receivesPackages = template.receivesPackages.concat(template.receives.map(
          receivableTypeOrName => Object.keys(this.packageCache)
            .filter(
              name => this.packageCache[name].type === receivableTypeOrName || name === receivableTypeOrName
            )
            .map(name => this.packageCache[name])
        ).reduce((a, b) => a.concat(b), []));
      }
    });
  }

  packagesToReceive(byPackage) {
    return byPackage.receivesPackages || [];
  }

  executeCommand(command, args, options) {
    const child = spawn(command, args, { stdio: 'inherit', ...options });
    return new Promise((resolve, reject) => {
      child.on('close', code => {
        if (code !== 0) {
          reject(new Error(`${command} ${args.join(' ')} failed with exit code ${code}. Please check your console.`));
          return;
        }
        resolve();
      });
    });
  }

  async ensureDependencies() {
    const dependencies = await this.importDependencies();
    const packageJson = await fs.readJson(path.join(this.appPath, 'package.json'));
    if (!packageJson || !packageJson.dependencies) {
      return [];
    }
    const toInstall = Object.keys(dependencies).filter(dependency => !packageJson.dependencies[dependency]);
    if (toInstall.length) {
      await this.executeCommand(
        'npm',
        ['install', '--save'].concat(toInstall),
        { cwd: path.resolve(this.appPath) }
      );
    }
    return toInstall;
  }

  async importDependencies() {
    const sourceContainer = await this.loadSources();

    const allImports = sourceContainer.sourceFiles
      .filter(f => f.fileName.match(/\.js$/))
      .map(f => sourceContainer.targetSourceByFile(f.fileName).imports)
      .reduce((a, b) => a.concat(b));
    const dependencies = allImports
      .filter(i => i.get('source').node.value.indexOf('.') !== 0)
      .map(i => {
        const importName = i.get('source').node.value.split('/');
        const dependency = importName[0].indexOf('@') === 0 ? [importName[0], importName[1]].join('/') : importName[0];
        return this.withPeerDependencies(dependency);
      }).reduce((a, b) => ({ ...a, ...b }));

    return dependencies;
  }

  withPeerDependencies(dependency) {
    let result = {
      [dependency]: 'latest'
    };
    if (dependency === 'graphql-tag') {
      result = {
        ...result,
        graphql: 'latest'
      };
    }
    if (dependency === 'react-chartjs-2') {
      result = {
        ...result,
        'chart.js': 'latest'
      };
    }
    return result;
  }
}

module.exports = AppContainer;
