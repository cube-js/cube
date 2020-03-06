const TemplatePackage = require('./TemplatePackage');

class RootTemplatePackage extends TemplatePackage {
  async initSourceContainer(sourceContainer) {
    this.sourceContainer = await this.appContainer.loadSources();
  }
}

module.exports = RootTemplatePackage;
