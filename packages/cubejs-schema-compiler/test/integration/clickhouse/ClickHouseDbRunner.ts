import { createClient } from '@clickhouse/client';
import type { ClickHouseClient, ResponseJSON } from '@clickhouse/client';
import { GenericContainer } from 'testcontainers';
import type { StartedTestContainer } from 'testcontainers';
import { format as formatSql } from 'sqlstring';
import { v4 as uuidv4 } from 'uuid';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { BaseDbRunner } from '../utils/BaseDbRunner';

process.env.TZ = 'GMT';

export class ClickHouseDbRunner extends BaseDbRunner {
  public adapter: string = 'clickhouse';

  protected container: StartedTestContainer | null = null;

  protected clickHouseVersion: string = process.env.TEST_CLICKHOUSE_VERSION || '23.11';

  public supportsExtendedDateTimeResults: boolean = this.clickHouseVersion >= '22.9';

  protected allowExperimentalJoinCondition: boolean = this.clickHouseVersion >= '24.5';

  public override async tearDown(): Promise<void> {
    if (this.container) {
      await this.container.stop();
      this.container = null;
    }
  }

  protected async gutterDataSet(clickHouse: ClickHouseClient): Promise<void> {
    // let engine = 'MergeTree PARTITION BY id ORDER BY (id) SETTINGS index_granularity = 8192'
    const engine = 'Memory';

    await clickHouse.command({ query: `
      CREATE TEMPORARY TABLE visitors (id UInt64, amount UInt64, created_at DateTime, updated_at DateTime, status UInt64, source Nullable(String), latitude Float64, longitude Float64)
      ENGINE = ${engine}
    ` });

    await clickHouse.command({ query: `
      CREATE TEMPORARY TABLE visitor_checkins (id UInt64, visitor_id UInt64, created_at DateTime, source Nullable(String))
      ENGINE = ${engine}
    ` });

    await clickHouse.command({ query: `
      CREATE TEMPORARY TABLE cards (id UInt64, visitor_id UInt64, visitor_checkin_id UInt64)
      ENGINE = ${engine}
    ` });

    await clickHouse.command({ query: `
      CREATE TEMPORARY TABLE events (id UInt64, type String, name String, started_at DateTime64, ended_at Nullable(DateTime64))
      ENGINE = ${engine}
    ` });

    await clickHouse.command({ query: `
      CREATE TEMPORARY TABLE numbers (num Int)
      ENGINE = ${engine}
    ` });

    await clickHouse.command({ query: `
      INSERT INTO
      visitors
      (id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES
      (1, 100, '2017-01-02 16:00:00', '2017-01-29 16:00:00', 1, 'some', 120.120, 40.60),
      (2, 200, '2017-01-04 16:00:00', '2017-01-14 16:00:00', 1, 'some', 120.120, 58.60),
      (3, 300, '2017-01-05 16:00:00', '2017-01-19 16:00:00', 2, 'google', 120.120, 70.60),
      (4, 400, '2017-01-06 16:00:00', '2017-01-24 16:00:00', 2, null, 120.120, 10.60),
      (5, 500, '2017-01-06 16:00:00', '2017-01-24 16:00:00', 2, null, 120.120, 58.10),
      (6, 500, '2016-09-06 16:00:00', '2016-09-06 16:00:00', 2, null, 120.120, 58.10)
    ` });

    await clickHouse.command({ query: `
      INSERT INTO
      visitor_checkins
      (id, visitor_id, created_at, source) VALUES
      (1, 1, '2017-01-02 16:00:00', null),
      (2, 1, '2017-01-03 16:00:00', null),
      (3, 1, '2017-01-04 16:00:00', 'google'),
      (4, 2, '2017-01-04 16:00:00', null),
      (5, 2, '2017-01-04 16:00:00', null),
      (6, 3, '2017-01-05 16:00:00', null)
    ` });

    await clickHouse.command({ query: `
      INSERT INTO
      cards
      (id, visitor_id, visitor_checkin_id) VALUES
      (1, 1, 1),
      (2, 1, 2),
      (3, 3, 6)
    ` });

    await clickHouse.command({ query: `
      INSERT INTO
      events
      (id, type, name, started_at, ended_at) VALUES
      (1, 'moon_missions', 'Apollo 10', '1969-05-18 16:49:00', '1969-05-26 16:52:23'),
      (2, 'moon_missions', 'Apollo 11', '1969-07-16 13:32:00', '1969-07-24 16:50:35'),
      (3, 'moon_missions', 'Artemis I', '2021-11-16 06:32:00', '2021-12-11 18:50:00'),
      (4, 'private_missions', 'Axiom Mission 1', '2022-04-08 15:17:12', '2022-04-25 17:06:00')
    ` });

    await clickHouse.command({ query: `
      INSERT INTO
      numbers
      (num) VALUES
      (0), (1), (2), (3), (4), (5), (6), (7), (8), (9),
      (10), (11), (12), (13), (14), (15), (16), (17), (18), (19),
      (20), (21), (22), (23), (24), (25), (26), (27), (28), (29),
      (30), (31), (32), (33), (34), (35), (36), (37), (38), (39),
      (40), (41), (42), (43), (44), (45), (46), (47), (48), (49),
      (50), (51), (52), (53), (54), (55), (56), (57), (58), (59)
    ` });
  }

  public override async testQueries(queries: Array<[string, Array<unknown>]>, prepareDataSet?: ((client: ClickHouseClient) => Promise<void>) | null): Promise<Array<Array<Record<string, unknown>>>> {
    let host;
    let port;
    if (process.env.TEST_CLICKHOUSE_HOST) {
      host = process.env.TEST_CLICKHOUSE_HOST;
      port = 8123;
    } else {
      if (!this.container) {
        this.container = await new GenericContainer(`clickhouse/clickhouse-server:${this.clickHouseVersion}`)
          .withExposedPorts(this.port())
          .start();
      }
      host = 'localhost';
      port = this.container.getMappedPort(8123);
    }

    const clickHouse = createClient({
      url: `http://${host}:${port}`,

      // needed for tests to use temporary tables
      session_id: uuidv4(),
      max_open_connections: 1,
    });

    prepareDataSet = prepareDataSet || this.gutterDataSet;
    await prepareDataSet(clickHouse);

    // Controls whether functions return results with extended date and time ranges.
    //
    // 0 — Functions return Date or DateTime for all arguments (default).
    // 1 — Functions return Date32 or DateTime64 for those argument types, and Date or DateTime otherwise.
    //
    // Extended ranges apply to:
    // Date32: toStartOfYear, toStartOfISOYear, toStartOfQuarter, toStartOfMonth, toLastDayOfMonth, toStartOfWeek, toLastDayOfWeek, toMonday.
    // DateTime64: toStartOfDay, toStartOfHour, toStartOfMinute, toStartOfFiveMinutes, toStartOfTenMinutes, toStartOfFifteenMinutes, timeSlot.
    //
    // https://clickhouse.com/docs/en/operations/settings/settings#enable-extended-results-for-datetime-functions
    const extendedDateTimeResultsOptions = this.supportsExtendedDateTimeResults ? {
      enable_extended_results_for_datetime_functions: 1
    } as const : {};

    const requests = queries
      .map(async ([query, params]) => {
        const resultSet = await clickHouse.query({
          query: formatSql(query, params),
          format: 'JSON',
          clickhouse_settings: {
            join_use_nulls: 1,
            ...extendedDateTimeResultsOptions
          }
        });
        // Because we used JSON format we expect each row in result set to be a record of column name => value
        const result = await resultSet.json<Record<string, unknown>>();
        return result;
      });

    const results = await Promise.all(requests);

    return results.map(ClickHouseDbRunner._normaliseResponse);
  }

  public async testQuery(queryAndParams: [string, Array<unknown>], prepareDataSet?: ((client: ClickHouseClient) => Promise<void>) | null): Promise<Array<Record<string, unknown>>> {
    const res = await this.testQueries([queryAndParams], prepareDataSet);
    return res[0];
  }

  public override port(): number {
    return 8123;
  }

  protected override newTestQuery(compilers: unknown, query: unknown): ClickHouseQuery {
    return new ClickHouseQuery(compilers, query);
  }

  //
  //
  //  ClickHouse returns DateTime as strings in format "YYYY-DD-MM HH:MM:SS"
  //  cube.js expects them in format "YYYY-DD-MMTHH:MM:SS.000", so translate them based on the metadata returned
  //
  //  https://github.com/statsbotco/cube.js/pull/98#discussion_r279698399
  //
  protected static _normaliseResponse(res: ResponseJSON<Record<string, unknown>>): Array<Record<string, unknown>> {
    if (process.env.DEBUG_LOG === 'true') {
      console.log(res);
    }

    const { meta, data } = res;
    if (meta === undefined) {
      throw new Error('Unexpected missing meta');
    }

    data.forEach(row => {
      for (const [field, value] of Object.entries(row)) {
        if (value !== null) {
          const fieldMeta = meta.find(m => m.name === field);
          if (fieldMeta === undefined) {
            throw new Error(`Missing meta for field ${field}`);
          }
          if (fieldMeta.type.includes('DateTime')) {
            if (typeof value !== 'string') {
              throw new Error(`Unexpected value for ${field}`);
            }
            row[field] = `${value.substring(0, 10)}T${value.substring(11, 22)}.000`;
          } else if (fieldMeta.type.includes('Date')) {
            row[field] = `${value}T00:00:00.000`;
          } else if (fieldMeta.type.includes('Int') || fieldMeta.type.includes('Float')) {
            // convert all numbers into strings
            row[field] = `${value}`;
          }
        }
      }
    });
    return data;
  }
}
