import type { DataSchemaCompiler } from '../compiler/DataSchemaCompiler';

export abstract class AbstractExtension {
  protected constructor(
    protected readonly cubeFactory: any,
    protected readonly compiler: DataSchemaCompiler,
    protected readonly cubes: any[],
  ) {
  }

  protected addCubeDefinition(name: string, cube: any) {
    this.cubes.push(Object.assign({}, cube, { name }));
  }
}
