import R from 'ramda';
import fs from 'fs-extra';
import path from 'path';
import { executeCommand } from '@cubejs-backend/shared';

import { File, fileContentsRecursive } from './utils';
import { DependencyNode } from './DependencyTree';
import { SourceContainer } from './SourceContainer';

export class AppContainer {
  public static getPackageVersions(appPath: string) {
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

  public constructor(
    protected rootNode: DependencyNode,
    { appPath, packagesPath }: { appPath: string, packagesPath: string },
    playgroundContext: Record<string, unknown>,
  ) {
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

  protected setChildren(node: DependencyNode) {
    if (!node) {
      return;
    }

    node.children.forEach((currentNode: DependencyNode) => {
      this.setChildren(currentNode);
      const [installsTo] = Object.keys(currentNode.package.installsTo!);
      if (!node.packageInstance.children[installsTo]) {
        node.packageInstance.children[installsTo] = [];
      }
      node.packageInstance.children[installsTo].push(currentNode.packageInstance);
    });
  }

  protected createInstances(node: DependencyNode) {
    const stack = [node];

    while (stack.length) {
      const child = stack.pop()!;

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

      child.children.forEach((currentChild: DependencyNode) => {
        stack.push(currentChild);
      });
    }
  }

  protected async loadSources() {
    return new SourceContainer(await fileContentsRecursive(this.appPath));
  }

  public async persistSources(sourceContainer: SourceContainer, packageVersions: Record<string, string>) {
    const sources = sourceContainer.outputSources();
    await Promise.all(sources.map((file: File) => fs.outputFile(path.join(this.appPath, file.fileName), file.content)));

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

    try {
      const packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));
      packageJson.cubejsTemplates = {
        ...packageJson.cubejsTemplates,
        ...packageVersions,
      };
      await fs.writeJson(path.join(this.appPath, 'package.json'), packageJson, {
        spaces: 2,
      });
    } catch (_) {
      //
    }
  }

  public async executeCommand(command: string, args: string | string[], options: object) {
    return executeCommand(command, args, options);
  }

  public async ensureDependencies() {
    const dependencies = this.sourceContainer?.importDependencies || [];
    let packageJson;

    try {
      packageJson = fs.readJsonSync(path.join(this.appPath, 'package.json'));
    } catch (_) {
      //
    }

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
