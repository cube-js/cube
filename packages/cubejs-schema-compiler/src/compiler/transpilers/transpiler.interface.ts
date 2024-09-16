import { TraverseOptions } from '@babel/traverse';
import { ErrorReporter } from '../ErrorReporter';

export type TraverseObject = TraverseOptions;

export interface TranspilerInterface {
  traverseObject(reporter: ErrorReporter): TraverseObject;
}
