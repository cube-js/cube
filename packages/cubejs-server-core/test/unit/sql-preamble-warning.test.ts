/* eslint-disable @typescript-eslint/no-explicit-any */
import type { BaseDriver } from '@cubejs-backend/query-orchestrator';

import { CubejsServerCore } from '../../src/core/server';

/**
 * A preamble configured for a driver that ignores it is a no-op for queries,
 * while still moving the pre-aggregation version key — so it costs a full
 * rebuild and changes nothing. The warning makes that visible.
 *
 * It asks the driver (`supportsSqlPreamble()`) rather than consulting a list of
 * dbTypes, because the capability is inherited: `RedshiftDriver extends
 * PostgresDriver` and every JDBC-based driver answer correctly without listing
 * themselves. A list would go stale silently and the failure direction is the bad
 * one — telling someone their working preamble does nothing.
 */
class ServerCoreOpen extends CubejsServerCore {
  public readonly warnings: { message: string, props: any }[] = [];

  public constructor() {
    super({ dbType: 'postgres', apiSecret: 'secret' } as any);
    this.logger = ((message: string, props: any) => {
      this.warnings.push({ message, props });
    }) as any;
  }

  public callWarn(driver: BaseDriver, dataSource: string) {
    return this.warnUnsupportedSqlPreamble(driver, dataSource);
  }
}

const driver = (supports?: boolean): BaseDriver => (
  supports === undefined
    // An out-of-tree driver predating the capability: no method at all.
    ? {} as BaseDriver
    : { supportsSqlPreamble: () => supports } as unknown as BaseDriver
);

describe('the unapplied SQL preamble warning', () => {
  let core: ServerCoreOpen;

  beforeEach(() => {
    core = new ServerCoreOpen();
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE;
  });

  afterEach(() => {
    delete process.env.CUBEJS_DB_SQL_PREAMBLE;
    delete process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE;
  });

  test('warns when a preamble is set on a driver that does not apply it', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    core.callWarn(driver(false), 'default');

    expect(core.warnings).toHaveLength(1);
    expect(core.warnings[0].message).toEqual('SQL preamble not applied');
    expect(core.warnings[0].props.warning).toContain('does not apply it');
    expect(core.warnings[0].props.dataSource).toEqual('default');
  });

  // The false-negative direction is the harmful one: telling a Redshift or
  // generic-JDBC user their working config does nothing points them at removing
  // it. Both inherit the capability, so both must land here.
  test('stays quiet for a driver that does apply it', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    core.callWarn(driver(true), 'default');

    expect(core.warnings).toHaveLength(0);
  });

  test('stays quiet for a driver that predates the capability', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    core.callWarn(driver(undefined), 'default');

    expect(core.warnings).toHaveLength(0);
  });

  test('stays quiet when no preamble is configured', () => {
    core.callWarn(driver(false), 'default');

    expect(core.warnings).toHaveLength(0);
  });

  test('a whitespace-only preamble is not configured', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = '   ';
    core.callWarn(driver(false), 'default');

    expect(core.warnings).toHaveLength(0);
  });

  // A build-only preamble is just as much of a no-op on an unsupporting driver.
  test('also warns for a pre-aggregation-only preamble', () => {
    process.env.CUBEJS_PRE_AGGREGATIONS_DB_SQL_PREAMBLE = 'SET a = 1';
    core.callWarn(driver(false), 'default');

    expect(core.warnings).toHaveLength(1);
  });

  // Driver resolution runs per data source and retries on failure.
  test('warns once per data source, not once per resolution', () => {
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    core.callWarn(driver(false), 'default');
    core.callWarn(driver(false), 'default');
    core.callWarn(driver(false), 'default');

    expect(core.warnings).toHaveLength(1);
  });

  test('warns separately for each data source', () => {
    process.env.CUBEJS_DATASOURCES = 'default,analytics';
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';
    process.env.CUBEJS_DS_ANALYTICS_DB_SQL_PREAMBLE = 'SET b = 2';

    try {
      core.callWarn(driver(false), 'default');
      core.callWarn(driver(false), 'analytics');

      expect(core.warnings.map(w => w.props.dataSource)).toEqual(['default', 'analytics']);
    } finally {
      delete process.env.CUBEJS_DATASOURCES;
      delete process.env.CUBEJS_DS_ANALYTICS_DB_SQL_PREAMBLE;
    }
  });

  // An undeclared data source is reported elsewhere with a clearer message, so
  // this must not replace it with one about the preamble.
  test('says nothing for a data source missing from CUBEJS_DATASOURCES', () => {
    process.env.CUBEJS_DATASOURCES = 'default,analytics';
    process.env.CUBEJS_DB_SQL_PREAMBLE = 'SET a = 1';

    try {
      expect(() => core.callWarn(driver(false), 'nope')).not.toThrow();
      expect(core.warnings).toHaveLength(0);
    } finally {
      delete process.env.CUBEJS_DATASOURCES;
    }
  });
});
