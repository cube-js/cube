import t from '@babel/types';
import { ErrorReporter } from '../ErrorReporter';

// @todo Replace with Real type when https://github.com/babel/babel/pull/12488 PR will be merged
export interface NodePath<T> {
  node: T,
  get(name: string): any,
  replaceWith(node: t.VariableDeclaration | t.Expression): void;
  replaceWithMultiple(node: t.VariableDeclaration[] | t.Expression): void;
}

export interface TraverseObject {
  ImportDeclaration?: (path: NodePath<t.ImportDeclaration>) => void,
  ExportNamedDeclaration?: (path: NodePath<t.ExportNamedDeclaration>) => void,
  ExportDefaultDeclaration?: (path: NodePath<t.ExportDefaultDeclaration>) => void,
  CallExpression?: (path: NodePath<t.CallExpression>) => void,
  Identifier?: (path: NodePath<t.Identifier>) => void,
}

export interface TranspilerInterface {
  traverseObject(reporter: ErrorReporter): TraverseObject;
}
