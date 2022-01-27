export class DynamicReference {
  public constructor(
    public readonly memberNames,
    public readonly fn
  ) {
    this.memberNames = memberNames;
    this.fn = fn;
  }
}
