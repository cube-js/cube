/* eslint-disable no-restricted-syntax */
import t from '@babel/types';

import { TranspilerInterface, TraverseObject } from './transpiler.interface';
import { ErrorReporter } from '../ErrorReporter';

export class CubeCheckDuplicatePropTranspiler implements TranspilerInterface {
  public traverseObject(reporter: ErrorReporter): TraverseObject {
    return {
      CallExpression: path => {
        // @ts-ignore @todo Unsafely?
        if (path.node.callee.name === 'cube') {
          path.node.arguments.forEach(arg => {
            if (arg && arg.type === 'ObjectExpression') {
              this.checkExpression(arg, reporter);
            }
          });
        }
      }
    };
  }

  protected compileExpression(expr: t.Expression) {
    if (expr.type === 'Identifier') {
      return expr.name;
    }

    if (expr.type === 'StringLiteral' && expr.value) {
      return expr.value;
    }

    return null;
  }

  protected checkExpression(astObjectExpression: t.ObjectExpression, reporter: ErrorReporter) {
    const unique = new Set();

    for (const prop of astObjectExpression.properties) {
      if (prop.type === 'ObjectProperty') {
        if (prop.value && prop.key) {
          if (prop.value.type === 'ObjectExpression') {
            this.checkExpression(prop.value, reporter);
          }

          const keyName = this.compileExpression(prop.key);
          if (keyName) {
            if (unique.has(keyName)) {
              reporter.syntaxError({
                message: `Duplicate property parsing ${keyName}`,
                loc: prop.key.loc,
              });
            }

            unique.add(keyName);
          }
        }
      }
    }
  }
}
