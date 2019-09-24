import traverse from "@babel/traverse";
import SourceSnippet from './SourceSnippet';
import ScaffoldingSources from '../codegen/ScaffoldingSources';

class IndexSnippet extends SourceSnippet {
  constructor() {
    super(ScaffoldingSources['react/index.js']);
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
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

      const targetPath = this.insertAnchor(targetSource);
      targetPath.replaceWith(routerElement.parentPath);
    }
  }

  insertAnchor(targetSource) {
    let appElement = null;
    traverse(targetSource.ast, {
      JSXOpeningElement: (path) => {
        if (path.get('name').get('name').node === 'App') {
          appElement = path;
        }
      }
    });
    if (!appElement) {
      throw new Error(`App class not found. Can't parse dashboard app.  Please delete dashboard-app directory and try to create it again.`);
    }
    return appElement.parentPath;
  }
}

export default IndexSnippet;
