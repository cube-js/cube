import { SchemaFileRepository } from '@cubejs-backend/shared';
import { CompilerApi } from '../../src/core/CompilerApi';
import { DbTypeInternalFn } from '../../src/core/types';

const airportsPolicy = `    access_policy:
      - group: "*"
        member_level:
          includes: "*"
        row_level:
          filters:
            - member: "{CUBE}.country"
              operator: equals
              values: ["US"]
`;

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
${airportsPolicy}
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
        dimension_map:
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
        dimension_map:
          airport: flights.destination
`;

// The fact's own policy instead of the airports' one.
const modelWithFactPolicy = model
  .replace(airportsPolicy, '')
  .replace(`    measures:
      - name: count
        type: count
`, `    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes: "*"
        row_level:
          filters:
            - member: "{CUBE}.origin"
              operator: equals
              values: ["SFO"]
`);

describe('CompilerApi row level security with view dimension mappings', () => {
  let compilerApi: CompilerApi;
  let content = model;
  const repository: SchemaFileRepository = {
    localPath: () => '/mock/path',
    dataSchemaFiles: () => Promise.resolve([{ fileName: 'model.yml', content }]),
  };
  const dbType: DbTypeInternalFn = async () => 'postgres';
  const context: any = { securityContext: {}, requestId: 'test' };

  beforeEach(() => {
    compilerApi = new CompilerApi(repository, dbType, {
      logger: () => {}, // eslint-disable-line @typescript-eslint/no-empty-function
      contextToGroups: async () => [],
    });
  });

  afterEach(() => {
    compilerApi.dispose();
    content = model;
  });

  const applyRls = async (query: any) => {
    const normalized = { timezone: 'UTC', filters: [], ...query };
    return compilerApi.applyRowLevelSecurity(normalized, normalized, context);
  };

  test('rejects a mapping of a dimension whose cube restricts the user\'s rows', async () => {
    await expect(applyRls({
      measures: ['airport_traffic.arrivals_count'],
      dimensions: ['airport_traffic.code'],
    })).rejects.toThrow(
      "Measure 'airport_traffic.arrivals_count' maps 'airport_traffic.code', whose cube restricts rows or masks members by access policies, which dimension mappings don't support yet"
    );
  });

  test('applies policies to a measure whose mapping the query does not use', async () => {
    const { query, denied } = await applyRls({
      measures: ['airport_traffic.arrivals_count'],
    });
    expect(denied).toBe(false);
    expect(query.filters).toEqual([]);
  });

  test('allows a mapping within a cube without access policies', async () => {
    const { denied } = await applyRls({
      measures: ['route_ends.arrivals_count'],
      dimensions: ['route_ends.airport'],
    });
    expect(denied).toBe(false);
  });

  test('applies the fact\'s own policy to a mapping within the fact', async () => {
    content = modelWithFactPolicy;
    const { query, denied } = await applyRls({
      measures: ['route_ends.arrivals_count'],
      dimensions: ['route_ends.airport'],
    });
    expect(denied).toBe(false);
    expect(JSON.stringify(query.filters)).toContain('"member":"flights.origin","operator":"equals","values":["SFO"]');
  });

  test.each([
    ['grants members only', `    access_policy:
      - group: "*"
        member_level:
          includes: "*"
`],
    ['allows all rows', `    access_policy:
      - group: "*"
        row_level:
          allow_all: true
`],
  ])('allows a mapping of a dimension whose cube\'s policy %s', async (_name, policy) => {
    content = model.replace(airportsPolicy, policy);
    const { denied } = await applyRls({
      measures: ['airport_traffic.arrivals_count'],
      dimensions: ['airport_traffic.code'],
    });
    expect(denied).toBe(false);
  });

  test('rejects a mapping within a fact whose policy masks members', async () => {
    content = model.replace(airportsPolicy, '').replace(`    measures:
      - name: count
        type: count
`, `    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes: [count]
        member_masking:
          includes: "*"
`);
    await expect(applyRls({
      measures: ['route_ends.arrivals_count'],
      dimensions: ['route_ends.airport'],
    })).rejects.toThrow(
      "Measure 'route_ends.arrivals_count' maps 'route_ends.airport', whose cube restricts rows or masks members by access policies"
    );
  });

  test('rejects a mapping to another instance of the fact under a row filter', async () => {
    content = `
cubes:
  - name: tickets
    sql_table: tickets
    joins:
      - name: tickets
        alias: parent
        sql: "{CUBE}.parent_id = {parent.id}"
        relationship: many_to_one
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        row_level:
          filters:
            - member: "{CUBE}.status"
              operator: equals
              values: ["open"]

views:
  - name: ticket_tree
    cubes:
      - join_path: tickets
        includes: [status]
      - join_path: tickets
        alias: children
        prefix: true
        includes: [count]
        dimension_map:
          status: tickets.parent.status
`;
    await expect(applyRls({
      measures: ['ticket_tree.children_count'],
      dimensions: ['ticket_tree.status'],
    })).rejects.toThrow(
      "Measure 'ticket_tree.children_count' maps 'ticket_tree.status', whose cube restricts rows or masks members by access policies"
    );
  });
});
