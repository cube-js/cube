import type { ErrorReporter } from './ErrorReporter';
import { TranspilerCubeResolver } from './transpilers';
import { CompilerInterface } from './PrepareCompiler';

export interface Cube {
  name: string;
  [key: string]: any;
}

export class CubeDictionary implements TranspilerCubeResolver, CompilerInterface {
  private byId: Map<string, Cube>;

  public constructor() {
    this.byId = new Map<string, Cube>();
  }

  public compile(cubes: Cube[], errorReporter?: ErrorReporter): void {
    this.byId = new Map<string, Cube>();
    for (const cube of cubes) {
      if (errorReporter && this.byId.has(cube.name)) {
        const existing = this.byId.get(cube.name)!;
        const existingType = existing.isView ? 'view' : 'cube';
        const newType = cube.isView ? 'view' : 'cube';
        if (existingType === newType) {
          errorReporter.error(`Found duplicate ${newType} name '${cube.name}'.`);
        } else {
          errorReporter.error(`Found conflicting cube and view name '${cube.name}'.`);
        }
      }
      this.byId.set(cube.name, cube);
    }
  }

  public resolveCube(name: string): Cube | undefined {
    return this.byId.get(name);
  }

  public free(): void {
    this.byId = new Map<string, Cube>();
  }

  public cubeNames(): string[] {
    return Array.from(this.byId.keys());
  }
}
