export class CompileError extends Error {
  public constructor(
    protected readonly messages: string,
    protected readonly plainMessages?: string,
  ) {
    super(`Compile errors:\n${messages}`);
  }
}
