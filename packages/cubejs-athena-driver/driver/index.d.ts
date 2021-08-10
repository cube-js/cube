import { BaseDriver } from '@cubejs-backend/query-orchestrator';
import { AthenaClientConfig } from '@aws-sdk/client-athena';

declare module '@cubejs-backend/athena-driver' {
  interface AthenaDriverOptions extends AthenaClientConfig {
    readOnly?: boolean,
    pollTimeout?: number,
    pollMaxInterval?: number,
  }

  export default class AthenaDriver extends BaseDriver {
    public constructor(options?: AthenaDriverOptions);
  }
}
