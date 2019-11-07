const TargetSource = require("./TargetSource");

class SourceContainer {
  constructor(sourceFiles) {
    this.sourceFiles = sourceFiles || [];
    this.fileToTargetSource = {};
  }

  mergeSnippetToFile(snippet, fileName, content) {
    const targetSource = this.targetSourceByFile(fileName, content);
    snippet.mergeTo(targetSource);
  }

  targetSourceByFile(fileName, content) {
    let file = this.sourceFiles.find(f => f.fileName === fileName);
    if (!file) {
      file = { fileName, content };
    }
    if (!this.fileToTargetSource[fileName]) {
      this.fileToTargetSource[fileName] = new TargetSource(file.fileName, file.content);
    }
    return this.fileToTargetSource[fileName];
  }

  outputSources() {
    return Object.keys(this.fileToTargetSource).map(fileName => ({
      fileName, content: this.fileToTargetSource[fileName].formattedCode()
    }));
  }
}

module.exports = SourceContainer;
