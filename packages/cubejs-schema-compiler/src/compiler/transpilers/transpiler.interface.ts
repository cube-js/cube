import t from '@babel/types';
import { NodePath } from '@babel/traverse';
import { ErrorReporter } from '../ErrorReporter';

export interface TraverseObject {
  ImportDeclaration?: (path: NodePath<t.ImportDeclaration>) => void,
  ExportNamedDeclaration?: (path: NodePath<t.ExportNamedDeclaration>) => void,
  ExportDefaultDeclaration?: (path: NodePath<t.ExportDefaultDeclaration>) => void,
  CallExpression?: (path: NodePath<t.CallExpression>) => void,
  Identifier?: (path: NodePath<t.Identifier>) => void,
  ObjectProperty?: (path: NodePath<t.ObjectProperty>) => void,
}

export interface TranspilerInterface {
  traverseObject(reporter: ErrorReporter): TraverseObject;
}
