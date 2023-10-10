import { PrestoDriver } from '@cubejs-backend/prestodb-driver';
import { PrestodbQuery } from '@cubejs-backend/schema-compiler/dist/src/adapter/PrestodbQuery';

export class TrinoDriver extends PrestoDriver {
  public constructor(options: any) {
    super({ ...options, engine: 'trino' });
  }

  public static dialectClass() {
    return PrestodbQuery;
  }
}
