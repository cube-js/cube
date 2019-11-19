const traverse = require("@babel/traverse").default;
const SourceSnippet = require("./SourceSnippet");

class IndexSnippet extends SourceSnippet {
  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    this.replaceRouter(targetSource);
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

module.exports = IndexSnippet;
