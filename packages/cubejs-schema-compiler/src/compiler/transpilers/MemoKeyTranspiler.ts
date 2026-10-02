import * as t from '@babel/types';
import type { NodePath } from '@babel/traverse';
import type { TranspilerInterface, TraverseObject } from './transpiler.interface';
import type { ErrorReporter } from '../ErrorReporter';

/**
 * Gives `memo(fn)` the key `{ callSite: 'orders.js:12:8' }`: it must be the same every time the file
 * is evaluated, as each compile stage evaluates it again and looks the result up by it.
 */
export class MemoKeyTranspiler implements TranspilerInterface {
  public static callSiteOf(key: unknown): string | undefined {
    if (typeof key !== 'object' || key === null) {
      return undefined;
    }
    const keys = Object.keys(key);
    const { callSite } = key as { callSite?: unknown };
    return keys.length === 1 && typeof callSite === 'string' ? callSite : undefined;
  }

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

        args.unshift(t.objectExpression([
          t.objectProperty(t.identifier('callSite'), t.stringLiteral(`${loc.filename}:${loc.start.line}:${loc.start.column}`)),
        ]));
      }
    };
  }
}
