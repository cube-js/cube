const fs = require('fs-extra');
const path = require('path');
const spawn = require('cross-spawn');

const { SourceContainer, utils } = require('@cubejs-playground/core');

class AppContainer {
  static packagesPath() {
    // Will be the path of the dowloaded packages
    return path.join(__dirname, '..', '__tmp__');
  }

  constructor(dependencyTree, appPath, playgroundContext) {
    this.dependencyTree = dependencyTree;
    this.appPath = appPath;
    this.playgroundContext = playgroundContext;

    this.initDependencyTree();
  }

  async applyTemplates() {
    this.sourceContainer = await this.loadSources();
    await this.dependencyTree.packageInstance.applyPackage(this.sourceContainer);
  }

  initDependencyTree() {
    this.createInstances(this.dependencyTree);
    this.setChildren(this.dependencyTree);
  }

  setChildren(node) {
    if (!node) {
      return;
    }

    node.children.forEach((currentNode) => {
      this.setChildren(currentNode);
      node.packageInstance.children.push(currentNode.packageInstance);
    });
  }

  createInstances(node) {
    const stack = [node];

    while (stack.length) {
      const child = stack.pop();

      const scaffoldingPath = path.join(AppContainer.packagesPath(), child.package.name, 'scaffolding');
      // eslint-disable-next-line
      const instance = require(path.join(AppContainer.packagesPath(), child.package.name))({
        appContainer: this,
        package: {
          ...child.package,
          scaffoldingPath,
        },
        playgroundContext: this.playgroundContext,
      });

      child.packageInstance = instance;

      child.children.forEach((child) => {
        stack.push(child);
      });
    }
  }

  async loadSources() {
    return new SourceContainer(await utils.fileContentsRecursive(this.appPath));
  }

  getPackageVersions() {
    return fs.readJsonSync(path.join(this.appPath, 'package.json')).cubejsTemplates || {};
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
    const child = spawn(command, args, { stdio: 'inherit', ...options });
    return new Promise((resolve, reject) => {
      child.on('close', (code) => {
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
    const toInstall = Object.keys(dependencies)
      .filter((dependency) => !packageJson.dependencies[dependency])
      .map((dependency) => (dependency === 'graphql-tools' ? `${dependency}@5.0.0` : dependency));

    if (toInstall.length) {
      await this.executeCommand('npm', ['install', '--save'].concat(toInstall), { cwd: path.resolve(this.appPath) });
    }
    return toInstall;
  }

  async importDependencies() {
    const sourceContainer = await this.loadSources();

    const allImports = sourceContainer.sourceFiles
      .filter((f) => f.fileName.match(/\.js$/))
      .map((f) => sourceContainer.targetSourceByFile(f.fileName).imports)
      .reduce((a, b) => a.concat(b));
    const dependencies = allImports
      .filter((i) => i.get('source').node.value.indexOf('.') !== 0)
      .map((i) => {
        const importName = i.get('source').node.value.split('/');
        const dependency = importName[0].indexOf('@') === 0 ? [importName[0], importName[1]].join('/') : importName[0];
        return this.withPeerDependencies(dependency);
      })
      .reduce((a, b) => ({ ...a, ...b }));

    return dependencies;
  }

  withPeerDependencies(dependency) {
    let result = {
      [dependency]: 'latest',
    };
    if (dependency === 'graphql-tag') {
      result = {
        ...result,
        graphql: 'latest',
      };
    }
    if (dependency === 'react-chartjs-2') {
      result = {
        ...result,
        'chart.js': 'latest',
      };
    }
    return result;
  }
}

module.exports = AppContainer;
