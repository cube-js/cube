export class CompileError extends Error {
  public constructor(
    protected readonly messages: string,
  ) {
    super(`Compile errors:\n${messages}`);
  }
}
