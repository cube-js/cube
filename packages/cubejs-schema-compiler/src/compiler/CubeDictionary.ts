import type { ErrorReporter } from './ErrorReporter';
import { TranspilerCubeResolver } from './transpilers';
import { CompilerInterface } from './PrepareCompiler';

export interface Cube {
  name: string;
  [key: string]: any;
}

export class CubeDictionary implements TranspilerCubeResolver, CompilerInterface {
  public byId: Record<string, Cube>;

  public constructor() {
    this.byId = {};
  }

  public compile(cubes: Cube[], _errorReporter?: ErrorReporter): void {
    this.byId = {};
    for (const cube of cubes) {
      this.byId[cube.name] = cube;
    }
  }

  public resolveCube(name: string): boolean {
    return !!this.byId[name];
  }

  public free(): void {
    this.byId = {};
  }
}
