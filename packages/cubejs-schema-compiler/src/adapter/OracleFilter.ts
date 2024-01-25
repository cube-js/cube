import { BaseFilter } from './BaseFilter';
import { OracleQuery } from './OracleQuery';

export class OracleFilter extends BaseFilter {
  public constructor(query: OracleQuery, filter: any) {
    super(<any>query, filter);
  }

  public castParameter() {
    return '?';
  }

  /**
     * @description Case-insensitive like requires collation statement
     */
  public likeIgnoreCase(column: string, not: string, param: unknown[], type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s} collate binary_ci`;
  }
}
