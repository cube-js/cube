import { PrestodbQuery } from './PrestodbQuery';

export class AthenaQuery extends PrestodbQuery {
  public static override dbType(): string {
    return 'athena';
  }
}
