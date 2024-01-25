import { GenericContainer, Wait, StartedTestContainer } from 'testcontainers';
import oracle, { BindParameters, Connection } from 'oracledb';
import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { FROM_PARTITION_RANGE, TO_PARTITION_RANGE } from '@cubejs-backend/shared';
import { BaseDbRunner } from '../postgres/BaseDbRunner';

export class OracleDbRunner extends BaseDbRunner {
    private _connection: Connection | undefined;

    public async connectionLazyInit(port: number): Promise<any> {
      // _connection is a singleton
      if (!this._connection) {
        this._connection = await oracle.getConnection({
          user: 'system',
          password: this.password(),
          connectString: `${this.host()}:${port}/${this.SID()}`
        });
        await this.prepareFixture(this._connection);
      }
      // connection is a singleton
      if (!this.connection) {
        this.connection = {
          close: async () => {
            await this._connection?.close();
            this._connection = undefined;
          },
          testQueries: async (queries: [string, BindParameters][]): Promise<Record<string, unknown>[]> => {
            const queryResults = queries.map((query) => this._connection?.execute<Record<string, unknown>>(query[0], query[1], { outFormat: oracle.OUT_FORMAT_OBJECT }));
            const finalResults = await Promise.all(queryResults);
            return JSON.parse(JSON.stringify(finalResults?.pop()?.rows || []));
          }
        };
      }

      return this.connection;
    }

    public tempTablePrefix = () => '';

    public replacePartitionName(
      query,
      desc,
      suffix,
      partitionGranularity: string | null = null,
      dateRange: string[] | null = null
    ) {
      const [toReplace, params] = query;
      const tableName = partitionGranularity && dateRange ? PreAggregationPartitionRangeLoader.partitionTableName(
        desc.tableName, partitionGranularity, dateRange
      ) : desc.tableName;
      let replaced = toReplace
        .replace(new RegExp(desc.tableName, 'g'), `${this.tempTablePrefix()}${tableName}_${suffix}`)
        .replace(/CREATE INDEX (?!i_)/, `CREATE INDEX i_${suffix}_`);
      if (dateRange?.length === 2) {
        replaced = replaced
          .replace(RegExp(FROM_PARTITION_RANGE, 'g'), PreAggregationPartitionRangeLoader.inDbTimeZone(desc, dateRange[0]))
          .replace(RegExp(TO_PARTITION_RANGE, 'g'), PreAggregationPartitionRangeLoader.inDbTimeZone(desc, dateRange[1]));
      }
      return [replaced, params];
    }

    public tempTableSql(desc: { loadSql?: string[] }) {
      return desc.loadSql?.[0].replace('CREATE TABLE', 'CREATE TABLE');
    }

    public async prepareFixture(conn: Connection, fixture?: unknown) {
      try {
        await conn.execute('DROP TABLE VISITORS');
      } catch {
        // ignore
      }
      try {
        await conn.execute('DROP TABLE VISITOR_CHECKINS');
      } catch {
        // ignore
      }
      try {
        await conn.execute('DROP TABLE CARDS');
      } catch {
        // ignore
      }
      await conn.execute('CREATE TABLE VISITORS (id INT, amount INT, created_at TIMESTAMP, updated_at TIMESTAMP, status INT, source VARCHAR(255), latitude DECIMAL, longitude DECIMAL)');
      await conn.execute('CREATE TABLE VISITOR_CHECKINS (id INT, visitor_id INT, created_at TIMESTAMP, source VARCHAR(255))');
      await conn.execute('CREATE TABLE CARDS (id INT, visitor_id INT, visitor_checkin_id INT)');
      const visitors = [
        [1, 100, '2017-01-03', '2017-01-30', 1, 'some', 120.120, 40.60],
        [2, 200, '2017-01-05', '2017-01-15', 1, 'some', 120.120, 58.60],
        [3, 300, '2017-01-06', '2017-01-20', 2, 'google', 120.120, 70.60],
        [4, 400, '2017-01-07', '2017-01-25', 2, null, 120.120, 10.60],
        [5, 500, '2017-01-07', '2017-01-25', 2, null, 120.120, 58.10],
        [6, 500, '2016-09-07', '2016-09-07', 2, null, 120.120, 58.10]
      ];
      const checkins: [number, number, string, string | null][] = [
        [1, 1, '2017-01-03', null],
        [2, 1, '2017-01-04', null],
        [3, 1, '2017-01-05', 'google'],
        [4, 2, '2017-01-05', null],
        [5, 2, '2017-01-05', null],
        [6, 3, '2017-01-06', null]
      ];
      const cards = [
        [1, 1, 1],
        [2, 1, 2],
        [3, 3, 6]
      ];
      let result = await conn.executeMany('INSERT INTO VISITORS(id, amount, created_at, updated_at, status, source, latitude, longitude) VALUES (:1, :2, TO_UTC_TIMESTAMP_TZ(:3), TO_UTC_TIMESTAMP_TZ(:4), :5, :6, :7, :8)', visitors);
      console.log(result?.rowsAffected, 'Visitor Rows Inserted');
      result = await conn.executeMany('INSERT INTO VISITOR_CHECKINS(id, visitor_id, created_at, source) VALUES (:1, :2, TO_UTC_TIMESTAMP_TZ(:3), :4)', checkins);
      console.log(result?.rowsAffected, 'Checkin Rows Inserted');
      result = await conn.executeMany('INSERT INTO CARDS(id, visitor_id, visitor_checkin_id) VALUES (:1, :2, :3)', cards);
      console.log(result?.rowsAffected, 'Card Rows Inserted');
      result = await conn.execute('SELECT * FROM VISITORS', {}, { outFormat: oracle.OUT_FORMAT_OBJECT });
      console.log(JSON.stringify(result));
    }

    public password() {
      return process.env.TEST_DB_PASSWORD || 'OracleDB1';
    }

    containerLazyInit = async () => {
      if (process.env.MAPPED_ORACLE_PORT) {
        return {
          getMappedPort(port: number) {
            return parseInt(process.env.MAPPED_ORACLE_PORT || '1521', 10);
          },
          getId() {
            return 'External, Local Oracle Instance';
          },
          getHost() {
            return process.env.ORACLE_HOST || 'localhost';
          },
          async stop() {
            return 'OK';
          },
          restart() {
            return 'OK';
          }
        };
      } else {
        // container is a singleton
        if (!this.container) {
          this.container = await new GenericContainer('aleanca/oracledb-21.3.0-ee:21.3.0-ee')
            .withEnv('ORACLE_PWD', this.password())
            .withStartupTimeout(60000 * 20) // takes close to 10 minutes for a laptop to start
            .withWaitStrategy(Wait.forLogMessage(/DATABASE IS READY TO USE!/))
            .withExposedPorts(this.port())
            .start();
        }
        return this.container;
      }
    }

    public port() {
      return 1521;
    }

    public SID() {
      return process.env.ORACLE_SID || 'ORCLCDB';
    }

    public host() {
      return process.env.ORACLE_HOST || 'localhost';
    }
}
