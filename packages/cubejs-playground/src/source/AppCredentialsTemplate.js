import TemplatePackage from "./TemplatePackage";
import CredentialsSnippet from "./CredentialsSnippet";

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

export default AppCredentialsTemplate;
