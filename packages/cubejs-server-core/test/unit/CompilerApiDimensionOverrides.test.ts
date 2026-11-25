import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

const model = `
cubes:
  - name: airports
    sql_table: airports
    dimensions:
      - name: code
        sql: code
        type: string
        primary_key: true
      - name: country
        sql: country
        type: string
    access_policy:
      - group: "*"
        member_level:
          includes: "*"
        row_level:
          filters:
            - member: "{CUBE}.country"
              operator: equals
              values: ["US"]

  - name: flights
    sql_table: flights
    joins:
      - name: airports
        alias: origin_airport
        sql: "{CUBE}.origin = {origin_airport.code}"
        relationship: many_to_one
      - name: airports
        alias: destination_airport
        sql: "{CUBE}.destination = {destination_airport.code}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: origin
        sql: origin
        type: string
      - name: destination
        sql: destination
        type: string
    measures:
      - name: count
        type: count

views:
  - name: airport_traffic
    cubes:
      - join_path: flights.origin_airport
        includes: [code]
      - join_path: flights
        alias: departures
        prefix: true
        includes: [count]
      - join_path: flights
        alias: arrivals
        prefix: true
        includes: [count]
        dimension_overrides:
          code: flights.destination_airport.code

  - name: route_ends
    cubes:
      - join_path: flights
        includes:
          - name: origin
            alias: airport
      - join_path: flights
        alias: arrivals
        prefix: true
        includes: [count]
        dimension_overrides:
          airport: flights.destination
`;

describe('CompilerApi row level security with view dimension overrides', () => {
  let compilerApi: CompilerApi;
  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.yml', content: model }]),
  };
  const dbType: DbTypeInternalFn = async () => 'postgres';
  const context: any = { securityContext: {}, requestId: 'test' };

  beforeEach(() => {
    compilerApi = new CompilerApi(repository, dbType, {
      logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      contextToGroups: async () => [],
    });
  });

  afterEach(() => compilerApi.dispose());

  const applyRls = async (query: any) => {
    const normalized = { timezone: 'UTC', filters: [], ...query };
    return compilerApi.applyRowLevelSecurity(normalized, normalized, context);
  };

  test('rejects an override of a dimension whose cube has access policies', async () => {
    await expect(applyRls({
      measures: ['airport_traffic.arrivals_count'],
      dimensions: ['airport_traffic.code'],
    })).rejects.toThrow(
      "Measure 'airport_traffic.arrivals_count' overrides 'airport_traffic.code', whose cube has access policies, which dimension overrides don't support yet"
    );
  });

  test('applies policies to a measure whose override the query does not use', async () => {
    const { query, denied } = await applyRls({
      measures: ['airport_traffic.arrivals_count'],
    });
    expect(denied).toBe(false);
    expect(query.filters).toEqual([]);
  });

  test('allows an override within a cube without access policies', async () => {
    const { denied } = await applyRls({
      measures: ['route_ends.arrivals_count'],
      dimensions: ['route_ends.airport'],
    });
    expect(denied).toBe(false);
  });
});
