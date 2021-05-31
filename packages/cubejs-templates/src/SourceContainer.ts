import { fromPairs, toPairs } from 'ramda';
import { File } from './utils';

type Dependency = [string, string];

export class SourceContainer {
  protected fileToTargetSource: Record<string, any> = {};

  protected fileContent: Record<string, string>;

  public importDependencies: Record<string, string> = {};

  protected filesToMove: Record<string, string> = {};

  public constructor(sourceFiles: File[]) {
    this.fileContent = fromPairs(sourceFiles.map(({ fileName, content }) => [fileName, content]));
  }

  public getTargetSource(fileName) {
    return this.fileToTargetSource[fileName];
  }

  public addTargetSource(fileName, target) {
    this.fileToTargetSource[fileName] = target;
  }

  public add(fileName, content) {
    this.fileContent[fileName] = content;
  }

  public addImportDependencies(importDependencies: Record<string, string> = {}) {
    // if some template returns a dependency with a specified version
    // it should have a priority over the same dependency with the `latest` version
    const specificDependencies = fromPairs(
      <Dependency[]>Object.keys(importDependencies)
        .map<boolean | [string, string]>((name) => {
          const version: string =
            this.importDependencies[name] && this.importDependencies[name] !== 'latest'
              ? this.importDependencies[name]
              : importDependencies[name];

          if (importDependencies[name]) {
            return [name, version];
          }

          return false;
        })
        .filter(Boolean)
    );

    // todo: version validation
    this.importDependencies = {
      ...this.importDependencies,
      ...importDependencies,
      ...specificDependencies,
    };
  }

  public addFileToMove(from, to) {
    this.filesToMove[from] = to;
  }

  public outputSources() {
    return toPairs(this.fileContent).map(([fileName, content]) => ({
      fileName,
      content,
    }));
  }
}
