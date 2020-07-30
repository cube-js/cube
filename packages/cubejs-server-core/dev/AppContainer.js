const fs = require('fs-extra');
const path = require('path');
const traverse = require('@babel/traverse').default;
const { parse } = require('@babel/parser');

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
    
    const allImports = sourceContainer.outputSources()
      .filter((f) => f.fileName.match(/\.js$/))
      .map(({ fileName, content }) => {
        const imports = [];
        
        const ast = parse(content, {
          sourceFilename: fileName,
          sourceType: 'module',
          plugins: ['jsx'],
        });

        traverse(ast, {
          ImportDeclaration(currentPath) {
            imports.push(currentPath);
          },
        });
        return imports;
      })
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
  
  getPackageVersions() {
    return AppContainer.getPackageVersions(this.appPath);
  }
}

module.exports = AppContainer;
