import { BaseDriver } from "@cubejs-backend/query-orchestrator";
import { ClientConfiguration } from "aws-sdk/clients/athena";

declare module "@cubejs-backend/athena-driver" {
  interface AthenaDriverOptions extends ClientConfiguration {
    readOnly?: boolean,
    pollTimeout?: number,
    pollMaxInterval?: number,
  }

  export default class AthenaDriver extends BaseDriver {
    constructor(options?: AthenaDriverOptions);
  }
}
