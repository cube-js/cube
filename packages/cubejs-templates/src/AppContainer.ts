import R from 'ramda';
import fs from 'fs-extra';
import path from 'path';

import { executeCommand, fileContentsRecursive } from './utils';
import { SourceContainer } from './SourceContainer';

export class AppContainer {
  public static getPackageVersions(appPath) {
    try {
      return fs.readJsonSync(path.join(appPath, 'package.json')).cubejsTemplates || {};
    } catch (error) {
      return {};
    }
  }

  protected sourceContainer: SourceContainer | null = null;

  protected playgroundContext: Record<string, unknown>;

  protected appPath: string;

  protected packagesPath: string;

  public constructor(protected rootNode, { appPath, packagesPath }, playgroundContext) {
    this.playgroundContext = playgroundContext;
    this.appPath = appPath;
    this.packagesPath = packagesPath;

    this.initDependencyTree();
  }

  public async applyTemplates() {
    this.sourceContainer = await this.loadSources();
    await this.rootNode.packageInstance.applyPackage(this.sourceContainer);
  }

  protected initDependencyTree() {
    this.createInstances(this.rootNode);
    this.setChildren(this.rootNode);
  }

  protected setChildren(node) {
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

  protected createInstances(node) {
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

  protected async loadSources() {
    return new SourceContainer(await fileContentsRecursive(this.appPath));
  }

  public async persistSources(sourceContainer, packageVersions) {
    const sources = sourceContainer.outputSources();
    await Promise.all(sources.map((file) => fs.outputFile(path.join(this.appPath, file.fileName), file.content)));

    await Promise.all(
      Object.entries<string>(sourceContainer.filesToMove).map(async ([from, to]) => {
        try {
          await this.executeCommand(`cp ${from} ${path.join('.', to)}`, [], {
            shell: true,
            cwd: path.resolve(this.appPath),
          });
        } catch (error) {
          console.log(`Unable to copy file: ${from} -> ${to}`);
        }
      })
    );

    const packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));
    packageJson.cubejsTemplates = {
      ...packageJson.cubejsTemplates,
      ...packageVersions,
    };
    await fs.writeJson(path.join(this.appPath, 'package.json'), packageJson, {
      spaces: 2,
    });
  }

  public async executeCommand(command, args, options) {
    return executeCommand(command, args, options);
  }

  public async ensureDependencies() {
    const dependencies = this.sourceContainer?.importDependencies || [];
    const packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));

    if (!packageJson || !packageJson.dependencies) {
      return [];
    }

    const toInstall = <string[]>R.toPairs(dependencies)
      .map(([dependency, version]) => {
        const currentDependency = version !== 'latest' ? `${dependency}@${version}` : dependency;
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

  public getPackageVersions() {
    return AppContainer.getPackageVersions(this.appPath);
  }
}
