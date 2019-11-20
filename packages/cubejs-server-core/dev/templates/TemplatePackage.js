const R = require('ramda');
const semver = require('semver');
const AppContainer = require('./AppContainer');
const SourceSnippet = require('./SourceSnippet');
const CssSourceSnippet = require('./CssSourceSnippet');

const versionRegex = /\.([0-9-]+)\.(\w+)$/;

class TemplatePackage {
  constructor({
    name,
    description,
    fileToSnippet,
    receives,
    requires,
    type,
    version,
    multiPackage
  }) {
    this.name = name;
    this.description = description;
    this.fileToSnippet = fileToSnippet || {};
    this.receives = receives;
    this.requires = requires;
    this.type = type;
    this.version = version;
    this.multiPackage = multiPackage;
  }

  async initSources() {
    const sources = await AppContainer.fileContentsRecursive(this.scaffoldingPath, null, true);
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

  static versionGte(a, b) {
    const verA = a.replace('-', '.');
    const verB = b.replace('-', '.');
    return semver.gt(verA, verB) || verA === verB;
  }

  mergeSources(sourceContainer, currentVersion) {
    R.uniq(
      Object.keys(this.templateSources).concat(Object.keys(this.fileToSnippet))
    ).filter(f => !f.match(versionRegex)).forEach(scaffoldingFile => {
      const allFiles = Object.keys(this.templateSources)
        .filter(f => f.match(versionRegex) &&
          f.replace(versionRegex, '.$2') &&
          (!currentVersion || TemplatePackage.versionGte(currentVersion, f.match(versionRegex)[1])))
        .concat([scaffoldingFile]);

      (allFiles.length > 1 ? R.range(2, allFiles.length + 1) : [1]).forEach(
        toTake => {
          const files = R.take(toTake, allFiles);
          const historySnippets = R.dropLast(1, files).map(f => this.createSourceSnippet(f, this.templateSources[f]));
          const lastVersionFile = files[files.length - 1];

          sourceContainer.mergeSnippetToFile(
            this.fileToSnippet[scaffoldingFile] || this.createSourceSnippet(
              scaffoldingFile,
              this.templateSources[lastVersionFile],
              historySnippets
            ),
            scaffoldingFile,
            this.templateSources[lastVersionFile]
          );
        }
      );
    });
  }

  createSourceSnippet(fileName, source, historySnippets) {
    if (fileName.match(/\.css$/)) {
      return new CssSourceSnippet(source, historySnippets);
    } else {
      return new SourceSnippet(source, historySnippets);
    }
  }

  async applyPackage(sourceContainer) {
    await this.initSourceContainer(sourceContainer);

    const packageVersions = await this.appContainer.getPackageVersions();
    if (this.multiPackage || packageVersions[this.name] !== this.version) {
      this.mergeSources(sourceContainer, packageVersions[this.name]);
      await this.appContainer.persistSources(
        this.sourceContainer,
        this.multiPackage ? {} : { [this.name]: this.version }
      );
    }

    const toReceive = this.appContainer.packagesToReceive(this);
    await toReceive.map(p => () => this.receive(p)).reduce((a, b) => a.then(b), Promise.resolve());
  }

  async receive(packageToApply) {
    await packageToApply.applyPackage(this.sourceContainer);
  }
}

module.exports = TemplatePackage;
