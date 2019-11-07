const traverse = require("@babel/traverse").default;
const SourceSnippet = require("./SourceSnippet");

class AppSnippet extends SourceSnippet {
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
      return super.insertAnchor(targetSource);
    }
    return appClass;
  }

  handleExistingMerge(existingDefinition, newDefinition) {
    if (existingDefinition && existingDefinition.node.type === 'FunctionDeclaration') {
      existingDefinition.replaceWith(newDefinition);
    }
  }
}

module.exports = AppSnippet;
