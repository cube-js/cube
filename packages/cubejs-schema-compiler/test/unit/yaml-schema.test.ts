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

  it('empty file', async () => {
    const { compiler } = prepareYamlCompiler(
      '   '
    );

    await compiler.compile();
  });

  it('empty cubes in file', async () => {
    const { compiler } = prepareYamlCompiler(
      'cubes:   '
    );

    await compiler.compile();
  });

  it('empty views in file', async () => {
    const { compiler } = prepareYamlCompiler(
      'views:   '
    );

    await compiler.compile();
  });

  it('Unexpected keys', async () => {
    const { compiler } = prepareYamlCompiler(
      'circles:   '
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('Unexpected YAML key');
    }
  });

  it('can\'t parse error', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
      - name: Products
        sql: select { "string"+123 } from tbl
        dimensions:
    `
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('Can\'t parse python expression');
    }
  });

  it('unnamed measure', async () => {
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

  describe('Escaping and quoting', () => {
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

    it('escape double quotes', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
  - name: Users
    sql: SELECT * FROM e2e.users
    dimensions:
      - name: id
        sql: id
        type: number
        primaryKey: true
      - name: id_str
        sql: "ID"
        type: string
      `
      );

      await compiler.compile();
    });

    it('escape curly braces', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
  - name: Users
    sql: SELECT 1 AS id, CAST('\\{"key":"value"\\}'::JSON AS TEXT) AS json_col
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

  describe('Parsing edge cases: ', () => {
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
        expect(e.message).toContain('Users cube: "title" must be a string');
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

    it('empty (null) dimensions', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      dimensions:
      `
      );

      await compiler.compile();
    });

    it('empty (null) measures', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      measures:
      `
      );

      await compiler.compile();
    });

    it('empty (null) segments', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      segments:
      `
      );

      await compiler.compile();
    });

    it('empty (null) preAggregations', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      dimensions: []
      measures: []
      segments: []
      preAggregations:
      joins: []
      hierarchies: []
      `
      );

      await compiler.compile();
    });

    it('empty (null) joins', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      joins:
      `
      );

      await compiler.compile();
    });

    it('empty (null) hierarchies', async () => {
      const { compiler } = prepareYamlCompiler(
        `cubes:
    - name: Users
      sql: SELECT * FROM e2e.users
      hierarchies:
      `
      );

      await compiler.compile();
    });
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

  it('descriptions', async () => {
    const { compiler, metaTransformer } = prepareYamlCompiler(
      `
      cubes:
      - name: CubeA
        description: "YAML schema test cube"
        sql: "select * from tbl"
        dimensions:
        - name: id
          description: "id dimension from YAML test cube"
          sql: id
          type: number
        measures:
        - name: count
          description: "count measure from YAML test cube"
          type: count
        segments:
        - name: sfUsers
          description: "SF users segment from createCubeSchema"
          sql: "{CUBE}.location = 'San Francisco'"
      `
    );

    await compiler.compile();

    const { description, dimensions, measures, segments } = metaTransformer.cubes[0].config;

    expect(description).toBe('YAML schema test cube');

    expect(dimensions).toBeDefined();
    expect(dimensions.length).toBeGreaterThan(0);
    expect(dimensions.find((dimension) => dimension.name === 'CubeA.id').description).toBe('id dimension from YAML test cube');

    expect(measures).toBeDefined();
    expect(measures.length).toBeGreaterThan(0);
    expect(measures.find((measure) => measure.name === 'CubeA.count').description).toBe('count measure from YAML test cube');

    expect(segments).toBeDefined();
    expect(segments.length).toBeGreaterThan(0);
    expect(segments.find((segment) => segment.name === 'CubeA.sfUsers').description).toBe('SF users segment from createCubeSchema');
  });

  describe('Custom dimension granularities: ', () => {
    it('no granularity name', async () => {
      const { compiler } = prepareYamlCompiler(
        `
        cubes:
        - name: Orders
          sql: "select * from tbl"
          dimensions:
            - name: created_at
              sql: created_at
              type: time
              granularities:
                - interval: 6 months
            - name: status
              sql: status
              type: string
          measures:
            - name: count
              type: count
        `
      );

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain('name isn\'t defined for dimension.granularity');
      }
    });

    it('incorrect granularity name', async () => {
      const { compiler } = prepareYamlCompiler(
        `
        cubes:
        - name: Orders
          sql: "select * from tbl"
          dimensions:
            - name: created_at
              sql: created_at
              type: time
              granularities:
                - name: 6_months
            - name: status
              sql: status
              type: string
          measures:
            - name: count
              type: count
        `
      );

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain('(dimensions.created_at.granularities.6_months = [object Object]) is not allowed');
      }
    });

    it('granularities as object ', async () => {
      const { compiler } = prepareYamlCompiler(
        `
        cubes:
        - name: Orders
          sql: "select * from tbl"
          dimensions:
            - name: created_at
              sql: created_at
              type: time
              granularities:
                name: half_year
            - name: status
              sql: status
              type: string
          measures:
            - name: count
              type: count
        `
      );

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain('dimension.granularitys must be defined as array');
      }
    });

    it('4 correct granularities', async () => {
      const { compiler } = prepareYamlCompiler(
        `
        cubes:
        - name: Orders
          sql: "select * from tbl"
          dimensions:
            - name: created_at
              sql: created_at
              type: time
              granularities:
                - name: six_months
                  interval: 6 months
                  title: 6 month intervals
                - name: three_months_offset
                  interval: 3 months
                  offset: 2 weeks
                - name: fiscal_year_1st_april
                  title: Fiscal year by Apr
                  interval: 1 year
                  origin: >
                    2024-04-01
                - name: timestamp_offseted_3_weeks
                  interval: 3 weeks
                  origin: "2024-02-15 10:15:25"
            - name: status
              sql: status
              type: string
          measures:
            - name: count
              type: count
        `
      );

      await compiler.compile();
    });
  });

  describe('Access policy: ', () => {
    it('defines a correct accessPolicy', async () => {
      const { compiler } = prepareYamlCompiler(
        `
        cubes:
        - name: Orders
          sql: "select * from tbl"
          dimensions:
            - name: created_at
              sql: created_at
              type: time
            - name: status
              sql: status
              type: string
            - name: is_true
              sql: is_true
              type: boolean
          measures:
            - name: count
              type: count
          accessPolicy:
            - role: admin
              conditions:
                - if: "{ !security_context.isBlocked }"
              rowLevel:
                filters:
                  - member: status
                    operator: equals
                    values: ["completed"]
                  - or:
                    - member: "{CUBE}.created_at"
                      operator: notInDateRange
                      values:
                        - 2022-01-01
                        - "{ security_context.currentDate }"
                    - member: "created_at"
                      operator: equals
                      values:
                        - "{ securityContext.currentDate }"
                    - member: "count"
                      operator: equals
                      values:
                        - 123
                    - member: "is_true"
                      operator: equals
                      values:
                        - true
              memberLevel:
                includes:
                  - status
            - role: manager
              memberLevel:
                excludes:
                  - status
        `
      );

      await compiler.compile();
    });
  });

  it('calling cube\'s sql()', async () => {
    const { compiler } = prepareYamlCompiler(
      `cubes:
  - name: simple_orders
    sql: >
      SELECT 1 AS id, 100 AS amount, 'new' status, now() AS created_at

    measures:
      - name: count
        type: count
      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: status
        sql: status
        type: string

  - name: simple_orders_sql_ext

    sql: >
      SELECT * FROM {simple_orders.sql()} as q
      WHERE status = 'processed'

    measures:
      - name: count
        type: count

      - name: total_amount
        sql: amount
        type: sum

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time
    `
    );

    await compiler.compile();
  });
});
