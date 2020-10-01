// eslint-disable-next-line import/no-extraneous-dependencies
import { CreateOptions } from '@cubejs-backend/server-core';

// eslint-disable-next-line no-undef
const apiSecret: string = await (async () => 'secret')();

const configuration: CreateOptions = {
  dbType: 'mysql',
  apiSecret,
};

export default configuration;
