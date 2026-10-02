import { AbstractExtension } from './extension.abstract';

export class RefreshKeys extends AbstractExtension {
  public immutablePartitionedRollupKey = (scalarValue: string) => ({
    sql: (FILTER_PARAMS: any) => `SELECT ${this.compiler.contextQuery().caseWhenStatement([{
      sql: FILTER_PARAMS[
        this.compiler.contextQuery().timeDimensions[0].path()[0]
      ][
        this.compiler.contextQuery().timeDimensions[0].path()[1]
      ].filter(
        (from: string, to: string) => `${this.compiler.contextQuery().nowTimestampSql()} < ${this.compiler.contextQuery().timeStampCast(to)}`
      ),
      label: scalarValue
    }])}`
  });
}
