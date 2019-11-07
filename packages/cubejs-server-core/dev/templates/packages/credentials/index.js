const TemplatePackage = require("../../TemplatePackage");
const CredentialsSnippet = require("../../CredentialsSnippet");

class AppCredentialsTemplate extends TemplatePackage {
  constructor(playgroundContext) {
    super({
      name: 'credentials',
      fileToSnippet: {
        '/src/App.js': new CredentialsSnippet(playgroundContext)
      }
    });
  }
}

module.exports = AppCredentialsTemplate;
