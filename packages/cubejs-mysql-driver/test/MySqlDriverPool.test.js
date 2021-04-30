/* globals describe, afterAll, beforeAll, test, expect, jest */
const { createDriver, startContainer } = require('./mysql.db.runner');

describe('MySqlDriver Pool', () => {
  jest.setTimeout(2 * 60 * 1000);

  test('database pool error', async () => {
    const poolErrorContainer = await startContainer();

    let databasePoolErrorLogged = false;

    const poolErrorDriver = createDriver(poolErrorContainer);
    poolErrorDriver.setLogger((msg, event) => {
      if (msg === 'Database Pool Error') {
        databasePoolErrorLogged = true;
      }
      console.log(`${msg}: ${JSON.stringify(event)}`);
    });

    try {
      await poolErrorDriver.createSchemaIfNotExists('test');
      await poolErrorDriver.query('DROP SCHEMA test');
      await poolErrorDriver.createSchemaIfNotExists('test');
      await poolErrorDriver.query('SELECT 1');
      await poolErrorContainer.stop();

      try {
        await poolErrorDriver.query('SELECT 1');
      } catch (e) {
        console.log(e);
        expect(e.toString()).toContain('ResourceRequest timed out');
      }

      expect(databasePoolErrorLogged).toBe(true);
    } finally {
      await poolErrorDriver.release();
    }
  });
});
