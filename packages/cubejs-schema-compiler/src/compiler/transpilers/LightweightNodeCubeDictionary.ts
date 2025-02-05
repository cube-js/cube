import { TranspilerCubeResolver } from './transpiler.interface';

export class LightweightNodeCubeDictionary implements TranspilerCubeResolver {
  public constructor(private cubeNames: string[] = []) {
  }

  public resolveCube(name: string): boolean {
    return this.cubeNames.includes(name);
  }

  public setCubeNames(cubeNames: string[]): void {
    this.cubeNames = cubeNames;
  }
}
