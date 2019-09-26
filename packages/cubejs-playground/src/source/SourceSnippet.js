import { parse } from '@babel/parser';
import traverse from "@babel/traverse";
import * as t from "@babel/types";

class SourceSnippet {
  constructor(source) {
    if (!source) {
      throw new Error('Empty source is provided');
    }
    this.ast = SourceSnippet.parse(source);
  }

  static parse(source) {
    return parse(source, {
      sourceType: 'module',
      plugins: [
        "jsx"
      ]
    });
  }

  mergeImport(targetSource, importDeclaration) {
    const sameSourceImport = targetSource.imports.find(
      i => i.get('source').node.value === importDeclaration.get('source').node.value
        && (
          i.get('specifiers')[0] && i.get('specifiers')[0].type
        ) === (
          importDeclaration.get('specifiers')[0] && importDeclaration.get('specifiers')[0].type
        )
    );
    if (!sameSourceImport) {
      targetSource.imports[targetSource.imports.length - 1].insertAfter(importDeclaration.node);
      targetSource.findAllImports();
    } else {
      importDeclaration.get('specifiers').forEach(toInsert => {
        const foundSpecifier = sameSourceImport.get('specifiers')
          .find(
            existing => (
              existing.get('imported').node && existing.get('imported').node.name
            ) === (
              toInsert.get('imported').node && toInsert.get('imported').node.name
            ) && (
              existing.get('local').node && existing.get('local').node.name
            ) === (
              toInsert.get('local').node && toInsert.get('local').node.name
            )
          );
        if (!foundSpecifier) {
          sameSourceImport.pushContainer('specifiers', toInsert.node);
        }
      });
    }
  }

  insertAnchor(targetSource) {
    return targetSource.defaultExport;
  }

  mergeDefinition(targetSource, constDef) {
    constDef.get('declarations').forEach(declaration => {
      const existingDefinition = targetSource.definitions.find(
        d => d.get('id').node.type === 'Identifier'
          && declaration.get('id').node.type === 'Identifier'
          && declaration.get('id').node.name === d.get('id').node.name
      );
      if (!existingDefinition) {
        this.insertAnchor(targetSource).insertBefore(t.variableDeclaration('const', [declaration.node]));
      }
    });
  }

  findImports() {
    return SourceSnippet.importsByAst(this.ast);
  }

  static importsByAst(ast) {
    const chartImports = [];

    traverse(ast, {
      ImportDeclaration: (path) => {
        chartImports.push(path);
      }
    });

    return chartImports;
  }

  findDefinitions() {
    return SourceSnippet.definitionsByAst(this.ast);
  }

  static definitionsByAst(ast) {
    const definitions = [];

    traverse(ast, {
      VariableDeclaration: (path) => {
        if (path.parent.type === 'Program') {
          definitions.push(path);
        }
      }
    });

    return definitions;
  }

  mergeTo(targetSource) {
    const chartImports = this.findImports();
    const definitions = this.findDefinitions();
    chartImports.forEach(i => this.mergeImport(targetSource, i));
    definitions.forEach(d => this.mergeDefinition(targetSource, d));
    targetSource.findAllImports();
    targetSource.findAllDefinitions();
  }
}

export default SourceSnippet;
