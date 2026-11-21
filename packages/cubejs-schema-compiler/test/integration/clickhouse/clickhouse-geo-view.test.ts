import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// A view member of type geo has only sql, and no latitude and longitude of its own. The cause is
// not in the dialect, and ClickHouse runs here only to read the generated SQL back.
describe('geo dimension in a view', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql: >
      SELECT 1 AS id, 'a' AS status, toNullable(10.5) AS lat, toNullable(20.5) AS lng
      UNION ALL
      SELECT 2 AS id, 'b' AS status, toNullable(30.5) AS lat, toNullable(40.5) AS lng
      UNION ALL
      SELECT 3 AS id, 'c' AS status, CAST(NULL AS Nullable(Float64)) AS lat, CAST(NULL AS Nullable(Float64)) AS lng
    dimensions:
      - name: id
        sql: "{CUBE}.id"
        type: number
        primary_key: true
        public: true
      - name: status
        sql: "{CUBE}.status"
        type: string
      - name: location
        type: geo
        latitude:
          sql: "{CUBE}.lat"
        longitude:
          sql: "{CUBE}.lng"
    measures:
      - name: count
        type: count

views:
  - name: user_places
    cubes:
      - join_path: users
        includes: "*"
      - join_path: users
        prefix: true
        includes:
          - name: location
            alias: place
`);

  beforeAll(async () => {
    await compilers.compile();
  });

  it.each([
    ['the cube member', 'users.location', ['10.5,20.5', '30.5,40.5']],
    ['the view member', 'user_places.location', ['10.5,20.5', '30.5,40.5']],
    ['the view member under a prefix and an alias', 'user_places.users_place', ['10.5,20.5', '30.5,40.5']],
  ])('selects %s', async (_name, member, expected) => {
    const query = compilers.createQuery(ClickHouseQuery, {
      dimensions: [member],
      filters: [{ member, operator: 'set' }],
      order: [{ id: member }],
      timezone: 'UTC',
    });
    // The API gateway flow reads the member SQL of the query on both planners
    expect(query.dimensions[0].dimensionSql()).toContain('lat');
    const [column] = query.buildSqlAndParams()[0].match(/`\w+__\w+`/) as RegExpMatchArray;
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);

    expect(rows.map((r: any) => r[column.replace(/`/g, '')])).toEqual(expected);
  });

  it('groups a measure by the view member and filters by another member', async () => {
    const query = compilers.createQuery(ClickHouseQuery, {
      measures: ['user_places.count'],
      dimensions: ['user_places.location'],
      filters: [{ member: 'user_places.status', operator: 'equals', values: ['b'] }],
      timezone: 'UTC',
    });
    const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);

    expect(rows).toEqual([{ user_places__location: '30.5,40.5', user_places__count: '1' }]);
  });

  it('gives a NULL coordinate of the view member as the cube member does', async () => {
    const values = async (member: string, column: string) => {
      const query = compilers.createQuery(ClickHouseQuery, {
        dimensions: [member],
        filters: [{ member: member.replace('location', 'status'), operator: 'equals', values: ['c'] }],
        timezone: 'UTC',
      });
      const rows = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);
      return rows.map((r: any) => r[column]);
    };

    expect(await values('user_places.location', 'user_places__location')).toEqual(
      await values('users.location', 'users__location')
    );
  });
});
