import type { TranspilerInterface, TraverseObject } from './transpiler.interface';
import type { ErrorReporter } from '../ErrorReporter';

export class ValidationTranspiler implements TranspilerInterface {
  public traverseObject(reporter: ErrorReporter): TraverseObject {
    return {
      Identifier: path => {
        if (path.node.name === 'USER_CONTEXT') {
          reporter.error(
            'Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT.',
            path.node.loc?.filename,
            path.node.loc?.start.line,
            path.node.loc?.start.column,
          );
        }
      }
    };
  }
}
