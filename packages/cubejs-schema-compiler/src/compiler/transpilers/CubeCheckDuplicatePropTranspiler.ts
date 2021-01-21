import { TranspilerInterface, TraverseObject } from './transpiler.interface';

const TYPE = {
  OBJECT_EXPRESSION: 'ObjectExpression',
  STRING_LITERAL: 'StringLiteral',
  IDENTIFIER: 'Identifier'
};

export class CubeCheckDuplicatePropTranspiler implements TranspilerInterface {
  public traverseObject(): TraverseObject {
    return {
      CallExpression: path => {
        // @ts-ignore @todo Unsafely?
        if (path.node.callee.name === 'cube') {
          path.node.arguments.forEach(arg => {
            if (arg && arg.type === TYPE.OBJECT_EXPRESSION) {
              this.checkExpression(arg);
            }
          });
        }
      }
    };
  }

  protected compileExpression(expr) {
    if (expr.type === TYPE.IDENTIFIER) {
      return expr.name;
    }

    if (expr.type === TYPE.STRING_LITERAL && expr.value) {
      return expr.value;
    }

    return null;
  }

  protected checkExpression(astObjectExpression) {
    const unique = new Set();

    astObjectExpression.properties.forEach(prop => {
      const { value, key, loc } = prop || {};
      if (value && key) {
        if (value.type === TYPE.OBJECT_EXPRESSION) {
          this.checkExpression(value);
        }

        const keyName = this.compileExpression(key);
        if (keyName) {
          if (unique.has(keyName)) {
            const error: any = new SyntaxError(`Duplicate property parsing ${keyName}`);
            error.loc = loc.start;
            throw error;
          }

          unique.add(keyName);
        }
      }
    });
  }
}
