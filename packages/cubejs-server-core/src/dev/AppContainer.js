const R = require('ramda');
const fs = require('fs-extra');
const path = require('path');

const SourceContainer = require('./SourceContainer');
const { fileContentsRecursive, executeCommand } = require('./utils');

class AppContainer {
  static getPackageVersions(appPath) {
    try {
      return fs.readJsonSync(path.join(appPath, 'package.json')).cubejsTemplates || {};
    } catch (error) {
      return {};
    }
  }

  constructor(rootNode, { appPath, packagesPath }, playgroundContext) {
    this.rootNode = rootNode;
    this.playgroundContext = playgroundContext;
    this.appPath = appPath;
    this.packagesPath = packagesPath;

    this.initDependencyTree();
  }

  async applyTemplates() {
    this.sourceContainer = await this.loadSources();
    await this.rootNode.packageInstance.applyPackage(this.sourceContainer);
  }

  initDependencyTree() {
    this.createInstances(this.rootNode);
    this.setChildren(this.rootNode);
  }

  setChildren(node) {
    if (!node) {
      return;
    }

    node.children.forEach((currentNode) => {
      this.setChildren(currentNode);
      const [installsTo] = Object.keys(currentNode.package.installsTo);
      if (!node.packageInstance.children[installsTo]) {
        node.packageInstance.children[installsTo] = [];
      }
      node.packageInstance.children[installsTo].push(currentNode.packageInstance);
    });
  }

  createInstances(node) {
    const stack = [node];

    while (stack.length) {
      const child = stack.pop();

      const scaffoldingPath = path.join(this.packagesPath, child.package.name, 'scaffolding');
      // eslint-disable-next-line
      const instance = require(path.join(this.packagesPath, child.package.name))({
        appContainer: this,
        package: {
          ...child.package,
          scaffoldingPath,
        },
        playgroundContext: this.playgroundContext,
      });

      child.packageInstance = instance;

      child.children.forEach((currentChild) => {
        stack.push(currentChild);
      });
    }
  }

  async loadSources() {
    return new SourceContainer(await fileContentsRecursive(this.appPath));
  }

  async persistSources(sourceContainer, packageVersions) {
    const sources = sourceContainer.outputSources();
    await Promise.all(sources.map((file) => fs.outputFile(path.join(this.appPath, file.fileName), file.content)));
    const packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));
    packageJson.cubejsTemplates = {
      ...packageJson.cubejsTemplates,
      ...packageVersions,
    };
    await fs.writeJson(path.join(this.appPath, 'package.json'), packageJson, {
      spaces: 2,
    });
  }

  executeCommand(command, args, options) {
    return executeCommand(command, args, options);
  }

  async ensureDependencies() {
    const dependencies = this.sourceContainer.importDependencies;
    const packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));

    if (!packageJson || !packageJson.dependencies) {
      return [];
    }

    const toInstall = R.toPairs(dependencies)
      .map(([dependency, version]) => {
        const currentDependency = (version !== 'latest' ? `${dependency}@${version}` : dependency);
        if (!packageJson.dependencies[dependency] || version !== 'latest') {
          return currentDependency;
        }
      
        return false;
      })
      .filter(Boolean);

    if (toInstall.length) {
      await this.executeCommand('npm', ['install', '--save'].concat(toInstall), { cwd: path.resolve(this.appPath) });
    }
    return toInstall;
  }

  getPackageVersions() {
    return AppContainer.getPackageVersions(this.appPath);
  }
}

module.exports = AppContainer;
