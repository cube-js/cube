import { prepareYamlCompiler } from './PrepareCompiler';

describe('Yaml Schema Testing', () => {
  it('members must be defined as arrays', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      cubes:
      - name: Products
        sql: "select * from tbl"
        dimensions:
          name: Title
          sql: name
          type: string
      `
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('dimensions must be defined as array');
    }
  });

  it('pre-aggregations - success', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
          - name: created_at
            sql: created_at
            type: time
          - name: completed_at
            sql: completed_at
            type: time
        measures:
          - name: count
            type: count
        pre_aggregations:
          - name: multiple_time_dimensions
            measures:
              - count
            time_dimensions:
              - dimension: created_at
                granularity: day
              - dimension: completed_at
                granularity: day
            partition_granularity: day
            build_range_start:
              sql: SELECT NOW() - INTERVAL '600 day'
            build_range_end:
              sql: SELECT NOW()
            refresh_key:
              every: '1 day'
      `
    );

    await compiler.compile();
  });

  it('commented file crash', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      #cubes:
      #- name: Products
      #  sql: "select * from tbl"
      #  dimensions:
      #    name: Title
      #    sql: name
      #    type: string
      `
    );

    await compiler.compile();
  });

  it('escapes backticks', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
  - name: Users
    sql: SELECT * FROM e2e.users
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
      - name: c2
        sql: "{CUBE}.\`C2\`"
        type: string
      `
    );

    await compiler.compile();
  });

  it('empty string - issue#7126', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
  - name: Users
    title: ''`
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('Users cube: (title = null) must be a string');
    }
  });

  it('null for string field', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
  - name: Users
    title: null`
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('Unexpected input during yaml transpiling: null');
    }
  });

  it('unammed measure', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
  - name: Users
    sql: SELECT * FROM e2e.users
    dimensions:
      - sql: id
        type: number
        primaryKey: true
      `
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('name isn\'t defined for dimension: ');
    }
  });

  it('accepts cube meta', async () => {
    const { compiler } = prepareYamlCompiler(
      `
      cubes:
      - name: Users
        sql: SELECT * FROM e2e.users
        meta:
          scalars:
            example_string: "foo"
            example_integer: 1
            example_float: 1.0
            example_boolean: true
            example_null: null
          sequence:
            - 1
            - 2
            - 3
          mixed_sequence:
            - 1
            - "foo"
            - 3
        dimensions:
          - name: id
            sql: id
            type: number
            primaryKey: true
      `
    );

    await compiler.compile();
  });
});
