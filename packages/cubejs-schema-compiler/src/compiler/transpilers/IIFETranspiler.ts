import * as t from '@babel/types';
import type { NodePath } from '@babel/traverse';
import type { TranspilerInterface, TraverseObject } from './transpiler.interface';
import type { ErrorReporter } from '../ErrorReporter';

/**
 * IIFETranspiler wraps the entire file content in an Immediately Invoked Function Expression (IIFE).
 * This prevents:
 * - Variable redeclaration errors when multiple files define the same variables
 * - Global scope pollution between data model files
 * - Provides isolated execution context for each file
 */
export class IIFETranspiler implements TranspilerInterface {
  public traverseObject(_reporter: ErrorReporter): TraverseObject {
    return {
      Program: (path: NodePath<t.Program>) => {
        const { body } = path.node;

        if (body.length > 0) {
          // Create an IIFE that wraps all the existing statements
          const iife = t.callExpression(
            t.functionExpression(
              null, // anonymous function
              [],
              t.blockStatement(body)
            ),
            []
          );

          // Replace the program body with the IIFE
          path.node.body = [t.expressionStatement(iife)];
        }
      }
    };
  }
}
