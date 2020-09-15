const TYPE = {
  OBJECT_EXPRESSION: 'ObjectExpression'
};

class CubeCheckDuplicatePropTranspiler {
  traverseObject() {
    return {
      CallExpression: path => {
        if (path.node.callee.name === 'cube') {
          path.node.arguments.forEach(arg => {
            if (arg && arg.type === TYPE.OBJECT_EXPRESSION) this.checkExpression(arg);
          });
        }
      }
    };
  }

  checkExpression(astObjectExpression) {
    const unique = new Set();
    astObjectExpression.properties.forEach(prop => {
      const { value, key, loc } = prop || {};
      if (value && key) {
        if (value.type === TYPE.OBJECT_EXPRESSION) this.checkExpression(value);
        if (unique.has(key.name)) {
          const error = new SyntaxError(`Duplicate property parsing ${key.name}`);
          error.loc = loc.start;
          throw error;
        }
        unique.add(key.name);
      }
    });
  }
}

module.exports = CubeCheckDuplicatePropTranspiler;
