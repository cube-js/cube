import type { DataSchemaCompiler } from '../compiler/DataSchemaCompiler';

export abstract class AbstractExtension {
  protected constructor(
    protected readonly cubeFactory: any,
    protected readonly compiler: DataSchemaCompiler,
  ) {
  }
}
