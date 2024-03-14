import { ParamAllocator } from './ParamAllocator';

export class OracleParamAllocator extends ParamAllocator {
  protected paramPlaceHolder(paramIndex: number) {
    return `:${paramIndex}`;
  }

  public buildSqlAndParams(annotatedSql: string, exportAnnotatedSql?: boolean): [string, unknown[]] {
    const [sql, params] = super.buildSqlAndParams(annotatedSql, exportAnnotatedSql);
    let newSql = sql;
    const parameterSubstitutions = [...sql.matchAll(/(:[0-9]+)/sg)];
    parameterSubstitutions.forEach(([, matched]) => {
      const value = params.shift();
      switch (typeof value) {
        case 'string':
          newSql = newSql.replace(matched, `'${value}'`);
          break;
        case 'boolean':
          newSql = newSql.replace(matched, `${value ? 1 : 0}`);
          break;
        default:
          newSql = newSql.replace(matched, value?.toString?.() || 'unknown');
      }
    });
    return [newSql, params];
  }
}
