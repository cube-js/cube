import traverse from "@babel/traverse";
import SourceSnippet from './SourceSnippet';

class AppSnippet extends SourceSnippet {
  findDefinitions() {
    return super.findDefinitions().filter(path => path.get('declarations')[0].get('id').get('name').node !== 'App');
  }

  mergeTo(targetSource) {
    super.mergeTo(targetSource);
    const appPath = super.findDefinitions()
      .find(path => path.get('declarations')[0].get('id').get('name').node === 'App');

    const targetPath = this.insertAnchor(targetSource);

    targetPath.replaceWith(appPath);
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
}

export default AppSnippet;
