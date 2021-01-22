import {
  ImportDeclaration,
  ExportNamedDeclaration,
  ExportDefaultDeclaration,
  CallExpression,
  VariableDeclaration,
  Expression,
} from '@babel/types';

// @todo Replace with Real type when https://github.com/babel/babel/pull/12488 PR will be merged
export interface NodePath<T> {
  node: T,
  get(name: string): any,
  replaceWith(node: VariableDeclaration | Expression): void;
  replaceWithMultiple(node: VariableDeclaration[] | Expression): void;
}

export interface TraverseObject {
  ImportDeclaration?: (path: NodePath<ImportDeclaration>) => void,
  ExportNamedDeclaration?: (path: NodePath<ExportNamedDeclaration>) => void,
  ExportDefaultDeclaration?: (path: NodePath<ExportDefaultDeclaration>) => void,
  CallExpression?: (path: NodePath<CallExpression>) => void,
}

export interface TranspilerInterface {
  traverseObject(): TraverseObject;
}
