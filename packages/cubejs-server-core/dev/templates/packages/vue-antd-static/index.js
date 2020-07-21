const RootTemplatePackage = require('../../RootTemplatePackage');
const path = require('path');

class CreateReactAppTemplate extends RootTemplatePackage {
  constructor() {
    super({
      name: 'vue-antd-static',
      description: 'Vue Antd Static',
      fileToSnippet: {},
      version: '0.1.0',
    });
  }

  async initSourceContainer(sourceContainer) {
    await super.initSourceContainer(this.appContainer, sourceContainer);
    const template = path.join(__dirname, './template');
    if (!this.sourceContainer.sourceFiles.length) {
      await this.appContainer.executeCommand('vue', ['create', '-d', 'dashboard-app', '--preset', template, '-m', 'npm'])
        .catch(e => {
          if (e.toString()
            .indexOf('ENOENT') !== -1) {
            throw new Error(`npx is not installed. Please update your npm: \`$ npm install -g npm\`.`);
          }
          throw e;
        });
      await super.initSourceContainer(this.appContainer, sourceContainer);
    }
  }
}

module.exports = CreateReactAppTemplate;
