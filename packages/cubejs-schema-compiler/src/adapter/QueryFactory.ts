export class QueryFactory {
  public constructor(
    private cubeToQueryClass: Record<string, any>,
  ) {
  }

  public createQuery(cube: string, compilers: any, queryOptions: any) {
    if (!(cube in this.cubeToQueryClass)) {
      throw new Error(`Undefined cube '${cube}'`);
    }
    const QueryClass = this.cubeToQueryClass[cube];
    if (!QueryClass) {
      throw new Error(`Undefined dbType or dialectClass for '${cube}'`);
    }
    return new QueryClass(compilers, queryOptions);
  }
}
