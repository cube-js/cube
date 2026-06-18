import { PrestodbQuery } from './PrestodbQuery';

export class TrinoQuery extends PrestodbQuery {
  // Trino doesn't require odd prestodb manual datetime offset calculations
  // as it uses mature timestamps models
  public override convertTz(field) {
    return this.timezone ? `CAST((${field} AT TIME ZONE '${this.timezone}') AS TIMESTAMP)` : field;
  }
}
