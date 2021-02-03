import * as t from '@babel/types';
import { TranspilerInterface, TraverseObject } from './transpiler.interface';
import { ErrorReporter } from '../ErrorReporter';

export class ImportExportTranspiler implements TranspilerInterface {
  public traverseObject(reporter: ErrorReporter): TraverseObject {
    return {
      ImportDeclaration(path) {
        const specifiers = path.get('specifiers');
        // eslint-disable-next-line array-callback-return,consistent-return
        const declarations = specifiers.map(specifier => {
          if (specifier.node.type === 'ImportSpecifier') {
            return t.variableDeclarator(
              specifier.get('local').node,
              t.memberExpression(
                t.callExpression(t.identifier('require'), [path.get('source').node]),
                specifier.get('imported').node
              )
            );
          } else if (specifier.node.type === 'ImportDefaultSpecifier') {
            return t.variableDeclarator(
              specifier.get('local').node,
              t.callExpression(t.identifier('require'), [path.get('source').node])
            );
          } else {
            reporter.syntaxError({
              message: `'${specifier.node.type}' import not supported`,
              loc: specifier.node.loc,
            });
          }
        });
        path.replaceWith(t.variableDeclaration('const', declarations));
      },
      ExportNamedDeclaration(path) {
        const specifiers = path.get('specifiers');
        // eslint-disable-next-line array-callback-return,consistent-return
        const declarations = specifiers.map(specifier => {
          if (specifier.node.type === 'ExportSpecifier') {
            return t.objectProperty(
              specifier.get('exported').node,
              specifier.get('local').node
            );
          } else {
            reporter.syntaxError({
              message: `'${specifier.node.type}' export not supported`,
              loc: specifier.node.loc,
            });
          }
        });
        const addExportCall = t.callExpression(t.identifier('addExport'), [t.objectExpression(declarations)]);
        if (path.get('declaration')) {
          path.replaceWithMultiple([
            path.get('declaration').node,
            t.callExpression(t.identifier('addExport'), [
              t.objectExpression(
                path.get('declaration').get('declarations').map(d => t.objectProperty(
                  d.get('id').node,
                  d.get('id').node
                ))
              )
            ])
          ]);
        } else {
          path.replaceWith(addExportCall);
        }
      },
      ExportDefaultDeclaration(path) {
        path.replaceWith(t.callExpression(t.identifier('setExport'), [path.get('declaration').node]));
      }
    };
  }
}
