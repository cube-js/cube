import * as t from '@babel/types';
import R from 'ramda';
import { NodePath } from '@babel/traverse';

import { TranspilerInterface, TraverseObject } from './transpiler.interface';
import type { CubeSymbols } from '../CubeSymbols';
import type { CubeDictionary } from '../CubeDictionary';

export class CubePropContextTranspiler implements TranspilerInterface {
  public constructor(
    protected readonly cubeSymbols: CubeSymbols,
    protected readonly cubeDictionary: CubeDictionary,
  ) {
  }

  public traverseObject(): TraverseObject {
    return {
      CallExpression: (path) => {
        if (t.isIdentifier(path.node.callee)) {
          const args = path.get('arguments');
          if (path.node.callee.name === 'cube') {
            if (args?.[args.length - 1]) {
              const cubeName = args[0].node.type === 'StringLiteral' && args[0].node.value ||
                args[0].node.type === 'TemplateLiteral' &&
                args[0].node.quasis.length &&
                args[0].node.quasis[0].value.cooked;
              args[args.length - 1].traverse(this.sqlAndReferencesFieldVisitor(cubeName));
              args[args.length - 1].traverse(
                this.knownIdentifiersInjectVisitor('extends', name => this.cubeDictionary.resolveCube(name))
              );
            }
          } else if (path.node.callee.name === 'context') {
            args[args.length - 1].traverse(this.sqlAndReferencesFieldVisitor(null));
          }
        }
      }
    };
  }

  protected transformObjectProperty(path: NodePath<t.ObjectProperty>, resolveSymbol: (name: string) => void) {
    const knownIds = this.collectKnownIdentifiers(
      resolveSymbol,
      <any>path.get('value')
    );
    path.get('value').replaceWith(
      t.arrowFunctionExpression(
        knownIds.map(i => t.identifier(i)),
        // @todo Replace any with assert expression
        <any>path.node.value,
        false
      )
    );
  }

  protected sqlAndReferencesFieldVisitor(cubeName): TraverseObject {
    // Unique fields that doesnt match any system fields in schema
    const simpleFields = /^(sql|measureReferences|rollups|dimensionReferences|segmentReferences|timeDimensionReference|timeDimension|rollupReferences|drillMembers|drillMemberReferences|contextMembers|columns)$/;
    // Not unique fields, example: measures exists in cube and pre-aggregation, which we should handle
    const complexFields = /^(dimensions|segments|measures)$/;
    const resolveSymbol = n => this.cubeSymbols.resolveSymbol(cubeName, n) || this.cubeSymbols.isCurrentCube(n);

    return {
      ObjectProperty: (path) => {
        if (path.node.key.type === 'Identifier') {
          if (path.node.key.name.match(simpleFields)) {
            this.transformObjectProperty(path, resolveSymbol);

            return;
          }

          if (path.node.key.name.match(complexFields) && path.parentPath?.parent.type !== 'CallExpression') {
            this.transformObjectProperty(path, resolveSymbol);
          }
        }
      }
    };
  }

  protected knownIdentifiersInjectVisitor(field: RegExp|string, resolveSymbol: (name: string) => void): TraverseObject {
    return {
      ObjectProperty: (path) => {
        if (path.node.key.type === 'Identifier' && path.node.key.name.match(field)) {
          this.transformObjectProperty(path, resolveSymbol);
        }
      }
    };
  }

  protected collectKnownIdentifiers(resolveSymbol, path: NodePath) {
    const identifiers = [];

    if (path.node.type === 'Identifier') {
      this.matchAndPushIdentifier(path, resolveSymbol, identifiers);
    }

    path.traverse({
      Identifier: (p) => {
        this.matchAndPushIdentifier(p, resolveSymbol, identifiers);
      }
    });

    return R.uniq(identifiers);
  }

  protected matchAndPushIdentifier(path, resolveSymbol, identifiers) {
    if (
      (!path.parent ||
        (path.parent.type !== 'MemberExpression' || path.parent.type === 'MemberExpression' && path.key !== 'property')
      ) &&
      resolveSymbol(path.node.name)
    ) {
      identifiers.push(path.node.name);
    }
  }
}
