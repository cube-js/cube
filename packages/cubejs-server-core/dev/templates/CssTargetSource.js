class CssTargetSource {
  constructor(fileName, source) {
    this.source = source;
    this.fileName = fileName;
  }

  formattedCode() {
    return this.source;
  }
}

module.exports = CssTargetSource;
