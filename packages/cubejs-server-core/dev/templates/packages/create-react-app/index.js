const RootTemplatePackage = require("../../RootTemplatePackage");

class CreateReactAppTemplate extends RootTemplatePackage {
  constructor() {
    super({
      name: 'create-react-app',
      description: 'Create react app',
      fileToSnippet: {},
      version: '0.0.1'
    });
  }

  async initSourceContainer(sourceContainer) {
    await super.initSourceContainer(this.appContainer, sourceContainer);
    if (!this.sourceContainer.sourceFiles.length) {
      await this.appContainer.executeCommand('npx', ['create-react-app', this.appContainer.appPath]).catch(e => {
        if (e.toString().indexOf('ENOENT') !== -1) {
          throw new Error(`npx is not installed. Please update your npm: \`$ npm install -g npm\`.`);
        }
        throw e;
      });
      await super.initSourceContainer(this.appContainer, sourceContainer);
    }
  }
}

module.exports = CreateReactAppTemplate;
