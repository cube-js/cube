import { CrateDriver } from '../src';

// CrateDriver replaces PostgresDriver's prepareConnection wholesale, to skip the
// session settings Crate does not support. That is exactly how the preamble
// would silently stop running, so pin it here rather than relying on the
// Postgres test covering the inherited path.
const driverWith = (preamble?: string) => {
  const driver = Object.create(CrateDriver.prototype) as any;
  const executed: string[] = [];

  const conn = {
    query: async (statement: unknown) => {
      if (typeof statement === 'string') {
        executed.push(statement);
      }
      return { rows: [], fields: [] };
    },
  };

  driver.config = {};
  driver.sqlPreamble = () => preamble;
  driver.loadUserDefinedTypes = async () => { /* no type discovery in the harness */ };

  return { driver, executed, conn };
};

describe('CrateDriver sql preamble', () => {
  test('runs the preamble even though the session settings are skipped', async () => {
    const { driver, executed, conn } = driverWith('SET a = 1');

    await driver.prepareConnection(conn, {});

    expect(executed).toEqual(['SET a = 1']);
  });

  test('runs nothing when no preamble is configured', async () => {
    const { driver, executed, conn } = driverWith(undefined);

    await driver.prepareConnection(conn, {});

    expect(executed).toEqual([]);
  });
});
