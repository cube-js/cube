import { TraverseOptions } from '@babel/traverse';
import { ErrorReporter } from '../ErrorReporter';

export type TraverseObject = TraverseOptions;

export interface TranspilerInterface {
  traverseObject(reporter: ErrorReporter): TraverseObject;
}

export interface TranspilerSymbolResolver {
  resolveSymbol(cubeName, name): any;
  isCurrentCube(name): boolean;
}

export interface TranspilerCubeResolver {
  resolveCube(name): boolean;
}

export type SymbolResolver = (name: string) => any;
