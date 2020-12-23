import { ClientConfiguration } from "aws-sdk/clients/athena";

declare module "@cubejs-backend/athena-driver" {
  interface AthenaDriverOptions extends ClientConfiguration {
    readOnly?: boolean
  }

  export default class AthenaDriver {
    constructor(options?: AthenaDriverOptions);
  }
}
