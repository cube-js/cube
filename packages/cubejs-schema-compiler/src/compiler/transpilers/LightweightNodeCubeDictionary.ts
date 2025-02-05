import { TranspilerCubeResolver } from './transpiler.interface';

export class LightweightNodeCubeDictionary implements TranspilerCubeResolver {
  public constructor(private readonly cubeNames: string[] = []) {
  }

  public resolveCube(name: string): boolean {
    return this.cubeNames.includes(name);
  }
}
