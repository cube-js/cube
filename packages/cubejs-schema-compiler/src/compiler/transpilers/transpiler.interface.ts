import { TraverseOptions } from '@babel/traverse';
import { ErrorReporter } from '../ErrorReporter';

export type TraverseObject = TraverseOptions;

export interface TranspilerInterface {
  traverseObject(reporter: ErrorReporter): TraverseObject;
}

export interface TranspilerSymbolResolver {
  resolveSymbol(cubeName: string | null | undefined, name: string): any;
  isCurrentCube(name: string): boolean;
}

export interface TranspilerCubeResolver {
  resolveCube(name: string): any;
}

export type SymbolResolver = (name: string) => any;
