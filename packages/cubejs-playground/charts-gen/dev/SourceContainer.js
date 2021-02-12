const { toPairs, fromPairs } = require('ramda');

class SourceContainer {
  constructor(sourceFiles) {
    this.fileToTargetSource = {};
    this.fileContent = fromPairs(
      sourceFiles.map(({ fileName, content }) => [fileName, content])
    );
    
    this.importDependencies = {};
  }

  getTargetSource(fileName) {
    return this.fileToTargetSource[fileName];
  }

  addTargetSource(fileName, target) {
    this.fileToTargetSource[fileName] = target;
  }

  add(fileName, content) {
    this.fileContent[fileName] = content;
  }

  addImportDependencies(importDependencies = {}) {
    // if some template returns a dependency with a specified version
    // it should have a priority over the same dependency with the `latest` version
    const specificDependencies = fromPairs(
      Object.keys(importDependencies)
        .map((name) => {
          const version =
            this.importDependencies[name] &&
            this.importDependencies[name] !== 'latest'
              ? this.importDependencies[name]
              : importDependencies[name];
          if (importDependencies[name]) {
            return [name, version];
          }

          return null;
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

  outputSources() {
    return toPairs(this.fileContent).map(([fileName, content]) => ({
      fileName,
      content,
    }));
  }
}

module.exports = SourceContainer;
