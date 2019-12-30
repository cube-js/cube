const Redshift = require('aws-sdk/clients/redshift');
const pg = require('pg');

const pkg = require('../package');
const Driver = require('./RedshiftDriver');

describe(pkg.name, () => {
  describe('Redshift Driver', () => {
    beforeAll(() => {
      process.env.AWS_REGION = 'redshift.amazonaws.com';
      process.env.CUBEJS_DB_HOST = 'redshift.amazonaws.com';
      process.env.CUBEJS_DB_USER = 'userFromEnv';
    });

    afterAll(() => {
      delete process.env.AWS_REGION;
      delete process.env.CUBEJS_DB_HOST;
      delete process.env.CUBEJS_DB_USER;
    });

    describe('#getCredentialsFromAWS', () => {
      beforeEach(() => {
        jest.spyOn(console, 'log').mockImplementation(jest.fn);
      });

      afterEach(() => {
        jest.clearAllMocks();
      });

      it('retrieves credentials successfully', async () => {
        const driver = new Driver();
        const credentials = await driver.getCredentialsFromAWS();
        expect(credentials).toHaveProperty('user');
        expect(credentials).toHaveProperty('password');
        expect(credentials.user).toEqual('userFromEnv');
        expect(credentials.password).toEqual('passwordFromAWS');
      });

      it('throws an error if credentials could not be retrieved', async () => {
        // eslint-disable-next-line no-underscore-dangle
        Redshift.__setResponseType('error');
        const driver = new Driver();
        try {
          await driver.getCredentialsFromAWS();
        } catch (e) {
          expect(e.message).toContain('Unable to retrieve Redshift credentials');
          expect(console.log).toHaveBeenCalled();
        }
      });
    });

    describe('#createPool', () => {
      it('uses the `CUBEJS_DB_PASS` environment variable if available ', async () => {
        process.env.CUBEJS_DB_PASS = 'passwordFromEnv';
        const driver = new Driver();
        await driver.createPool();

        expect(pg.Pool).toHaveBeenCalledWith(expect.objectContaining({
          user: process.env.CUBEJS_DB_USER,
          password: 'passwordFromEnv',
        }));

        delete process.env.CUBEJS_DB_PASS;
      });

      it('retrieves password from AWS if `CUBEJS_DB_PASS` variable is unavailable ', async () => {
        // eslint-disable-next-line no-underscore-dangle
        Redshift.__setResponseType('success');
        const driver = new Driver();
        await driver.createPool();

        expect(pg.Pool).toHaveBeenCalledWith(expect.objectContaining({
          user: process.env.CUBEJS_DB_USER,
          password: 'passwordFromAWS',
        }));
      });
    });
  });
});
