const t = require("@babel/types");
const SourceSnippet = require("./SourceSnippet");

class CredentialsSnippet extends SourceSnippet {
  constructor({ apiUrl, cubejsToken }) {
    super();
    this.apiUrl = apiUrl;
    this.cubejsToken = cubejsToken;
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    this.replaceTokens(targetSource);
  }

  replaceTokens(targetSource) {
    const apiUrl = targetSource.definitions.find(d => d.get('id').node.name === 'API_URL');
    apiUrl.get('init').replaceWith(t.stringLiteral(this.apiUrl));

    const cubejsToken = targetSource.definitions.find(d => d.get('id').node.name === 'CUBEJS_TOKEN');
    cubejsToken.get('init').replaceWith(t.stringLiteral(this.cubejsToken));
  }
}

module.exports = CredentialsSnippet;
