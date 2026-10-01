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
          const fn = t.functionExpression(
            null, // anonymous function
            [],
            t.blockStatement(body)
          );
          // Sloppy code keeps the file's `this` (the global, or the compile's own global object in
          // a shared realm) instead of the realm global a plain call would give it
          const strict = path.node.directives.some((d) => d.value.value === 'use strict');
          const iife = strict
            ? t.callExpression(fn, [])
            : t.callExpression(t.memberExpression(fn, t.identifier('call')), [t.thisExpression()]);

          // Replace the program body with the IIFE
          path.node.body = [t.expressionStatement(iife)];
        }
      }
    };
  }
}
