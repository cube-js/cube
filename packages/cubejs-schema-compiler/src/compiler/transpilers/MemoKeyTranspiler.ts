import * as t from '@babel/types';
import type { NodePath } from '@babel/traverse';
import type { TranspilerInterface, TraverseObject } from './transpiler.interface';
import type { ErrorReporter } from '../ErrorReporter';

/**
 * MemoKeyTranspiler lets `memo(fn)` omit its key: it inserts one made of the file name and the
 * position of the call, `memo('orders.js:12:8', fn)`. The key has to be the same every time the
 * file is evaluated, as each compile stage evaluates it again and looks the result up by it.
 */
export class MemoKeyTranspiler implements TranspilerInterface {
  public traverseObject(_reporter: ErrorReporter): TraverseObject {
    return {
      CallExpression: (path: NodePath<t.CallExpression>) => {
        const { callee, arguments: args, loc } = path.node;
        if (
          !t.isIdentifier(callee, { name: 'memo' }) ||
          // A local `memo` isn't the compile global
          path.scope.hasBinding('memo') ||
          // `memo(fn)`: a single argument can only be the function
          args.length !== 1 ||
          t.isSpreadElement(args[0]) ||
          !loc
        ) {
          return;
        }

        args.unshift(t.stringLiteral(`${loc.filename}:${loc.start.line}:${loc.start.column}`));
      }
    };
  }
}
