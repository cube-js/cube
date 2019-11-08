const R = require('ramda');
const AppContainer = require('./AppContainer');
const SourceSnippet = require('./SourceSnippet');
const CssSourceSnippet = require('./CssSourceSnippet');

class TemplatePackage {
  constructor({
    name,
    description,
    fileToSnippet,
    receives,
    requires,
    type,
    version
  }) {
    this.name = name;
    this.description = description;
    this.fileToSnippet = fileToSnippet || {};
    this.receives = receives;
    this.requires = requires;
    this.type = type;
    this.version = version;
  }

  async initSources() {
    const sources = await AppContainer.fileContentsRecursive(this.scaffoldingPath);
    this.templateSources = sources
      .map(({ fileName, content }) => ({ [fileName]: content }))
      .reduce((a, b) => ({ ...a, ...b }), {});
    Object.keys(this.fileToSnippet).forEach(file => {
      if (this.templateSources[file]) {
        this.fileToSnippet[file].source = this.templateSources[file];
      }
    });
  }

  async initSourceContainer(sourceContainer) {
    this.sourceContainer = sourceContainer;
  }

  mergeSources(sourceContainer) {
    R.uniq(
      Object.keys(this.templateSources).concat(Object.keys(this.fileToSnippet))
    ).forEach(scaffoldingFile => {
      sourceContainer.mergeSnippetToFile(
        this.fileToSnippet[scaffoldingFile] || this.createSourceSnippet(
          scaffoldingFile, this.templateSources[scaffoldingFile]
        ),
        scaffoldingFile,
        this.templateSources[scaffoldingFile]
      );
    });
  }

  createSourceSnippet(fileName, source) {
    if (fileName.match(/\.css$/)) {
      return new CssSourceSnippet(source);
    } else {
      return new SourceSnippet(source);
    }
  }

  async applyPackage(sourceContainer) {
    await this.initSourceContainer(sourceContainer);

    if ((await this.appContainer.getPackageVersions()[this.name]) !== this.version) {
      this.mergeSources(sourceContainer);
    }

    const toReceive = this.appContainer.packagesToReceive(this);
    await toReceive.map(p => () => this.receive(p)).reduce((a, b) => a.then(b), Promise.resolve());
    await this.appContainer.persistSources(this.sourceContainer, { [this.name]: this.version });
  }

  async receive(packageToApply) {
    await packageToApply.applyPackage(this.sourceContainer);
  }
}

module.exports = TemplatePackage;
