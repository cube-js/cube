import { AbstractExtension } from './extension.abstract';

export class RefreshKeys extends AbstractExtension {
  public immutablePartitionedRollupKey = (scalarValue) => ({
    sql: (FILTER_PARAMS) => `SELECT ${this.compiler.contextQuery().caseWhenStatement([{
      sql: FILTER_PARAMS[
        this.compiler.contextQuery().timeDimensions[0].path()[0]
      ][
        this.compiler.contextQuery().timeDimensions[0].path()[1]
      ].filter(
        (from, to) => `${this.compiler.contextQuery().nowTimestampSql()} < ${this.compiler.contextQuery().timeStampCast(to)}`
      ),
      label: scalarValue
    }])}`
  });
}
