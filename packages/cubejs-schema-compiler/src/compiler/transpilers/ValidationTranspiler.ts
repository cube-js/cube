/* eslint-disable no-restricted-syntax */
import { TranspilerInterface, TraverseObject } from './transpiler.interface';
import { ErrorReporter } from '../ErrorReporter';

// @todo It's not possible to do a warning inside CubeSymbols.resolveSymbol,
// because it doesnt have ErrorReporter, restructure?
export class ValidationTranspiler implements TranspilerInterface {
  public traverseObject(reporter: ErrorReporter): TraverseObject {
    return {
      Identifier: path => {
        if (path.node.name === 'USER_CONTEXT') {
          reporter.warning({
            message: 'USER_CONTEXT was deprecated in favor of SECURITY_CONTEXT.',
            loc: path.node.loc,
          });
        }
      }
    };
  }
}
