const { toPairs, fromPairs } = require('ramda');

class SourceContainer {
  constructor(sourceFiles) {
    this.fileToTargetSource = {};
    this.fileContent = fromPairs(sourceFiles.map(({ fileName, content }) => [fileName, content]));
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
    // todo: version validation
    this.importDependencies = {
      ...this.importDependencies,
      ...importDependencies
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
