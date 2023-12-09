const PARAMS_MATCH_REGEXP = /\$(\d+)\$/g;

export class ParamAllocator {
  protected readonly params: unknown[];

  public constructor(expressionParams?: unknown[]) {
    this.params = expressionParams || [];
  }

  public allocateParamsForQuestionString(sql: string, paramArray: unknown[]): string {
    let paramIndex = 0;
    return sql.replace(/\?/g, () => this.allocateParam(paramArray[paramIndex++]));
  }

  public hasParametersInSql(sql: string): boolean {
    return sql.match(PARAMS_MATCH_REGEXP) !== null;
  }

  public buildSqlAndParams(annotatedSql: string, exportAnnotatedSql?: boolean): [string, unknown[]] {
    const paramsInSqlOrder: unknown[] = [];

    return [
      annotatedSql.replace(PARAMS_MATCH_REGEXP, (match, paramIndex) => {
        paramsInSqlOrder.push(this.params[paramIndex]);
        return exportAnnotatedSql ? `$${paramsInSqlOrder.length - 1}$` : this.paramPlaceHolder(paramsInSqlOrder.length - 1);
      }),
      paramsInSqlOrder
    ];
  }

  public allocateParam(param) {
    const paramIndex = this.params.length;
    this.params.push(param);

    return `$${paramIndex}$`;
  }

  // eslint-disable-next-line no-unused-vars
  protected paramPlaceHolder(paramIndex) {
    return '?';
  }
}
