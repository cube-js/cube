import { ClientOptions } from "@elastic/elasticsearch";
import { BaseDriver } from "@cubejs-backend/base-driver";

declare module "@cubejs-backend/elasticsearch-driver" {
  export type ElasticSearchDriverOptions = Pick<ClientOptions, 'ssl' | 'auth' | 'cloud'> & {
    url?: string;
    queryFormat?: string;
    openDistro?: boolean;
  };

  export default class ElasticSearchDriver extends BaseDriver {
    constructor(options?: ElasticSearchDriverOptions);
  }
}
