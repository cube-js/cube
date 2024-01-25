import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';
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
     * "ILIKE" doesn't support
     */
  public likeIgnoreCase(column: string, not: string, param: unknown[], type: string) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s}`;
  }

  // public allocateTimestampParams() {
  //   return this.filterParams().map((p, i) => {
  //     if (i > 1) {
  //       throw new Error(`Expected only 2 parameters for timestamp filter but got: ${this.filterParams()}`);
  //     }
  //     if (p === TO_PARTITION_RANGE || p === FROM_PARTITION_RANGE) {
  //       return this.allocateTimestampParam(p);
  //     }
  //     return (<any> this.query).timeStampInlineParam?.(p);
  //   });
  // }
}
