import traverse from "@babel/traverse";
import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';
import * as t from "@babel/types";

class AppSnippet extends SourceSnippet {
  constructor({ apiUrl, cubejsToken }) {
    super(ScaffoldingSources['react/App.js']);
    this.apiUrl = apiUrl;
    this.cubejsToken = cubejsToken;
  }

  findDefinitions() {
    return super.findDefinitions().filter(path => path.get('declarations')[0].get('id').get('name').node !== 'App');
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const appPath = super.findDefinitions()
      .find(path => path.get('declarations')[0].get('id').get('name').node === 'App');

    const targetPath = this.insertAnchor(targetSource);

    targetPath.replaceWith(appPath);
    this.replaceTokens(targetSource);
  }

  insertAnchor(targetSource) {
    let appClass = null;
    traverse(targetSource.ast, {
      FunctionDeclaration: (path) => {
        if (path.get('id').node.name === 'App') {
          appClass = path;
        }
      }
    });
    if (!appClass) {
      throw new Error(`App class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
    return appClass;
  }

  replaceTokens(targetSource) {
    const apiUrl = targetSource.definitions.find(d => d.get('id').node.name === 'API_URL');
    apiUrl.get('init').replaceWith(t.stringLiteral(this.apiUrl));

    const cubejsToken = targetSource.definitions.find(d => d.get('id').node.name === 'CUBEJS_TOKEN');
    cubejsToken.get('init').replaceWith(t.stringLiteral(this.cubejsToken));
  }
}

export default AppSnippet;
