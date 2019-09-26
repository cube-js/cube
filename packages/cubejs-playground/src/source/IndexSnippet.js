import traverse from "@babel/traverse";
import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';
import * as t from "@babel/types";

class IndexSnippet extends SourceSnippet {
  constructor({ apiUrl, cubejsToken }) {
    super(ScaffoldingSources['react/index.js']);
    this.apiUrl = apiUrl;
    this.cubejsToken = cubejsToken;
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    this.replaceRouter(targetSource);
    this.replaceTokens(targetSource);
  }

  replaceRouter(targetSource) {
    let routerElement = null;
    traverse(targetSource.ast, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'Router') {
          routerElement = path;
        }
      }
    });

    if (!routerElement) {
      traverse(this.ast, {
        JSXOpeningElement: (path) => {
          if (path.get('name').get('name').node === 'Router') {
            routerElement = path;
          }
        }
      });

      if (!routerElement) {
        throw new Error(`Router element is not found`);
      }

      const targetPath = this.findAppOrRouter(targetSource);
      targetPath.replaceWith(routerElement.parentPath);
    }
  }

  replaceTokens(targetSource) {
    const apiUrl = targetSource.definitions.find(d => d.get('id').node.name === 'API_URL');
    apiUrl.get('init').replaceWith(t.stringLiteral(this.apiUrl));

    const cubejsToken = targetSource.definitions.find(d => d.get('id').node.name === 'CUBEJS_TOKEN');
    cubejsToken.get('init').replaceWith(t.stringLiteral(this.cubejsToken));
  }

  findAppOrRouter(targetSource) {
    let appElement = null;
    traverse(targetSource.ast, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'Router' || path.get('name').get('name').node === 'App') {
          appElement = path;
        }
      }
    });
    if (!appElement) {
      throw new Error(`App class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
    return appElement.parentPath;
  }

  insertAnchor(targetSource) {
    return this.findAppOrRouter(targetSource).parentPath.parentPath;
  }
}

export default IndexSnippet;
