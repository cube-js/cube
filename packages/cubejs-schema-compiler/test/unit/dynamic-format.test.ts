import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('Dynamic measure format', () => {
  const schema = `
cubes:
  - name: accounts
    sql_table: accounts

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: type
        sql: type
        type: string

    measures:
      - name: value
        sql: amount
        type: sum
        format:
          sql: "CASE WHEN {min_type} = 'Ratio' AND {max_type} = 'Ratio' THEN '.1%' ELSE '$,.0f' END"

      - name: min_type
        sql: type
        type: min

      - name: max_type
        sql: type
        type: max

views:
  - name: accounts_view
    cubes:
      - join_path: accounts
        includes:
          - name: value
            alias: amount
          - type
`;

  it('should create a synthetic format measure', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compile();

    const formatDef = compilers.cubeEvaluator.measureByPath('accounts.value___format');
    expect(formatDef.type).toBe('string');
    expect((formatDef as any).synthetic).toBe(true);
  });

  it('should make the format measure multi-stage when the format asks for it', async () => {
    const compilers = prepareYamlCompiler(schema.replace(
      "THEN '.1%' ELSE '$,.0f' END\"",
      "THEN '.1%' ELSE '$,.0f' END\"\n          multi_stage: true"
    ));
    await compilers.compile();

    expect((compilers.cubeEvaluator.measureByPath('accounts.value___format') as any).multiStage).toBe(true);
  });

  it('should evaluate the format SQL when the format measure is queried', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compile();

    const query = compilers.createQuery(PostgresQuery, {
      measures: ['accounts.value', 'accounts.value___format'],
      dimensions: ['accounts.type'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(sql).toContain('\'.1%\'');
    expect(sql).toMatch(/min\(.*type.*\) = 'Ratio'/i);
  });

  it('should expose the sql format and the synthetic flag in meta', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts')!.config;

    const value = measures.find((m: any) => m.name === 'accounts.value')!;
    expect(value.format).toEqual({ type: 'sql' });
    expect(value.formatDescription).toBeUndefined();
    expect(value.synthetic).toBeUndefined();

    const valueFormat = measures.find((m: any) => m.name === 'accounts.value___format')!;
    expect(valueFormat.synthetic).toBe(true);
    expect(valueFormat.public).toBe(true);
  });

  it('should include the format measure in views under the alias of the measure', async () => {
    const compilers = prepareYamlCompiler(schema);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts_view')!.config;

    const amount = measures.find((m: any) => m.name === 'accounts_view.amount')!;
    expect(amount.format).toEqual({ type: 'sql' });

    const amountFormat = measures.find((m: any) => m.name === 'accounts_view.amount___format')!;
    expect(amountFormat.synthetic).toBe(true);
    expect(amountFormat.aliasMember).toBe('accounts.value___format');
  });

  it('should not include the format measure when a view overrides the format', async () => {
    const compilers = prepareYamlCompiler(`${schema}
  - name: accounts_static_view
    cubes:
      - join_path: accounts
        includes:
          - name: value
            format: currency
`);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts_static_view')!.config;
    expect(measures.map((m: any) => m.name)).toEqual(['accounts_static_view.value']);
    expect(measures[0].format).toBe('currency');
  });

  it('should create the format measure of a measure defined in a view', async () => {
    const compilers = prepareYamlCompiler(`${schema}
  - name: accounts_own_view
    cubes:
      - join_path: accounts
        includes:
          - type
    measures:
      - name: total
        sql: "{accounts.value}"
        type: number
        format:
          sql: "CASE WHEN {accounts.min_type} = 'Ratio' THEN '.1%' END"
`);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts_own_view')!.config;
    expect(measures.map((m: any) => m.name)).toEqual([
      'accounts_own_view.total',
      'accounts_own_view.total___format',
    ]);
  });

  it('should exclude the format measure together with the measure', async () => {
    const compilers = prepareYamlCompiler(`${schema}
  - name: accounts_excluding_view
    cubes:
      - join_path: accounts
        includes: "*"
        excludes:
          - value
`);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts_excluding_view')!.config;
    expect(measures.map((m: any) => m.name)).toEqual([
      'accounts_excluding_view.min_type',
      'accounts_excluding_view.max_type',
    ]);
  });

  it('should keep an object format set by a view include as is', async () => {
    const compilers = prepareYamlCompiler(`${schema}
  - name: accounts_link_view
    cubes:
      - join_path: accounts
        includes:
          - name: value
            format:
              type: link
              label: Open
`);
    await compilers.compile();

    const { cubes } = compilers.metaTransformer;
    const { measures } = cubes.find((c: any) => c.config.name === 'accounts_link_view')!.config;
    expect(measures[0].format).toEqual({ type: 'link', label: 'Open' });
  });

  it('should apply access policies to the format measure like to any other member', async () => {
    const compilers = prepareYamlCompiler(`${schema}
  - name: accounts_policy_view
    cubes:
      - join_path: accounts
        includes:
          - value
          - type
    access_policy:
      - group: listed
        member_level:
          includes:
            - value
            - value___format
      - group: not_listed
        member_level:
          includes:
            - value
      - group: wildcard
        member_level:
          includes: "*"
`);
    await compilers.compile();

    const [listed, notListed, wildcard] = compilers.cubeEvaluator.cubeFromPath('accounts_policy_view').accessPolicy!;
    expect(listed.memberLevel!.includesMembers).toContain('accounts_policy_view.value___format');
    expect(notListed.memberLevel!.includesMembers).not.toContain('accounts_policy_view.value___format');
    expect(wildcard.memberLevel!.includesMembers).toContain('accounts_policy_view.value___format');
  });

  it('should serve the format measure from a rollup of the measures it references', async () => {
    const compilers = prepareYamlCompiler(schema.replace('\nviews:', `
    pre_aggregations:
      - name: by_type
        measures:
          - value
          - min_type
          - max_type
        dimensions:
          - type

views:`));
    await compilers.compile();

    for (const dimensions of [['accounts.type'], []]) {
      const query = compilers.createQuery(PostgresQuery, {
        measures: ['accounts.value', 'accounts.value___format'],
        dimensions,
        timezone: 'UTC',
        preAggregationsSchema: '',
      });
      const preAggregations: any[] = query.preAggregations?.preAggregationsDescription() || [];
      const [sql] = query.buildSqlAndParams();

      expect(preAggregations.map(p => p.tableName)).toEqual(['accounts_by_type']);
      expect(sql).toContain('\'.1%\'');
    }
  });

  it('should reject a format measure that collides with a user-defined measure', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: accounts
    sql_table: accounts

    measures:
      - name: value
        sql: amount
        type: sum
        format:
          sql: "'.1%'"

      - name: value___format
        sql: "'.2%'"
        type: string
`);

    await expect(compilers.compile()).rejects.toThrow(
      'Dynamic format of measure \'value\' conflicts with existing measure \'value___format\''
    );
  });
});
