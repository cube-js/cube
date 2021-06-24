import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { ConnectionConfig } from 'mysql';

declare module '@cubejs-backend/mysql-driver' {
  export interface MySqlDriverConfiguration extends ConnectionConfig {
    readOnly?: boolean,
  }

  export default class MySqlDriver extends BaseDriver {
    public constructor(options?: MySqlDriverConfiguration);

    public release(): Promise<void>
  }
}
