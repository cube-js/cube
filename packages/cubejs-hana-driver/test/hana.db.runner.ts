import { SapHanaDriver } from '../src/SapHanaDriver';

export const createDriver = (serverNode?: string, user?: string, password?: string) => new SapHanaDriver({
  serverNode: serverNode || process.env.TEST_DB_SERVER,
  uid: user || process.env.TEST_DB_USER,
  pwd: password || process.env.TEST_DB_PASSWORD,
});
