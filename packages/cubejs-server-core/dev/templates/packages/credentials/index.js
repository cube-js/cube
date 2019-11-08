const TemplatePackage = require("../../TemplatePackage");
const CredentialsSnippet = require("../../CredentialsSnippet");

class AppCredentialsTemplate extends TemplatePackage {
  constructor(playgroundContext) {
    super({
      name: 'credentials',
      fileToSnippet: {
        '/src/App.js': new CredentialsSnippet(playgroundContext)
      },
      version: '0.0.1'
    });
  }
}

module.exports = AppCredentialsTemplate;
