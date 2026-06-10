import { prepareYamlCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src';

describe('Yaml Schema Testing', () => {
  describe('Duplicate member detection', () => {
    it('detects duplicate measures', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
      - name: count
        type: count
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate measure 'count' in cube 'orders'");
      }
    });

    it('detects duplicate dimensions', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: id
        sql: id
        type: number
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate dimension 'id' in cube 'orders'");
      }
    });

    it('detects duplicate segments', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    segments:
      - name: active
        sql: "{CUBE}.status = 'active'"
      - name: active
        sql: "{CUBE}.status = 'active'"
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate segment 'active' in cube 'orders'");
      }
    });

    it('detects multiple duplicates', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
      - name: count
        type: count
      - name: total
        type: sum
        sql: amount
      - name: total
        type: sum
        sql: amount
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate measure 'count' in cube 'orders'");
        expect(e.message).toContain("Found duplicate measure 'total' in cube 'orders'");
      }
    });

    it('detects duplicate pre-aggregation indexes', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
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
    pre_aggregations:
      - name: main
        measures:
          - count
        dimensions:
          - status
        indexes:
          - name: status_idx
            columns:
              - status
          - name: status_idx
            columns:
              - id
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate preAggregation.index 'status_idx' in pre-aggregation 'main' in cube 'orders'");
      }
    });

    it('detects duplicate cube names', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: orders
    sql_table: orders_v2
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate cube name 'orders'");
      }
    });

    it('detects duplicate view names', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - id

  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - status
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate view name 'orders_view'");
      }
    });

    it('detects duplicate dimension granularities', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: created_at
        sql: created_at
        type: time
        granularities:
          - name: fiscal_year
            interval: 1 year
            origin: "2024-04-01"
          - name: fiscal_year
            interval: 1 year
            origin: "2024-01-01"
    measures:
      - name: count
        type: count
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate dimension.granularity 'fiscal_year' in time dimension 'created_at' in cube 'orders'");
      }
    });

    it('detects duplicate folder names in views', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: category
        sql: category
        type: string
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes: "*"
    folders:
      - name: Details
        includes:
          - id
          - status
      - name: Details
        includes:
          - category
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain(
          "Folder names must be unique within a view. Found duplicate folder 'Details' in view 'orders_view'."
        );
      }
    });

    it('detects duplicate dimension time shifts', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: fiscal_calendar
    sql: "SELECT 1"
    dimensions:
      - name: date_key
        sql: date_key
        type: time
        primary_key: true
      - name: date
        sql: calendar_date
        type: time
        time_shift:
          - name: prior_year
            type: prior
            interval: 1 year
          - name: prior_year
            sql: "{CUBE}.prior_year_date"
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain(
          "Time shift names must be unique within a dimension. Found duplicate time shift 'prior_year' in dimension 'date' in cube 'fiscal_calendar'."
        );
      }
    });
  });

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
        sql: select { "string"+123 } as a1, { 123abc } as a2 from tbl
        dimensions:
    `
    );

    try {
      await compiler.compile();

      throw new Error('compile must return an error');
    } catch (e: any) {
      expect(e.message).toContain('Failed to parse Python expression');
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
      expect(e.message).toContain('name isn\'t defined for dimension');
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
        expect(e.message).toMatch(
          /Users cube: "title" (must be a string|is not allowed to be empty)/
        );
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
    expect(dimensions.find((dimension) => dimension.name === 'CubeA.id')?.description).toBe('id dimension from YAML test cube');

    expect(measures).toBeDefined();
    expect(measures.length).toBeGreaterThan(0);
    expect(measures.find((measure) => measure.name === 'CubeA.count')?.description).toBe('count measure from YAML test cube');

    expect(segments).toBeDefined();
    expect(segments.length).toBeGreaterThan(0);
    expect(segments.find((segment) => segment.name === 'CubeA.sfUsers')?.description).toBe('SF users segment from createCubeSchema');
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
        expect(e.message).toContain('must be defined as array');
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
            - group: admin
              conditions:
                - if: "{ security_context.isNotBlocked }"
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
            - group: manager
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

  describe('Currency property', () => {
    it('measure with currency in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        measures:
        - name: total_amount
          sql: amount
          type: sum
          format: currency
          currency: usd
      `);

      await compiler.compile();

      const { measures } = metaTransformer.cubes[0].config;
      const totalAmount = measures.find((m) => m.name === 'Orders.total_amount');
      expect(totalAmount).toBeDefined();
      expect(totalAmount!.currency).toBe('USD');
      expect(totalAmount!.format).toBe('currency');
    });

    it('number dimension with currency in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        - name: price
          sql: price
          type: number
          currency: eur
      `);

      await compiler.compile();

      const { dimensions } = metaTransformer.cubes[0].config;
      const price = dimensions.find((d) => d.name === 'Orders.price');
      expect(price).toBeDefined();
      expect(price!.currency).toBe('EUR');
    });

    it('non-number dimension with currency in YAML - error', async () => {
      const { compiler } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        - name: status
          sql: status
          type: string
          currency: usd
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain('"currency" property can only be used with dimensions of type "number"');
      }
    });
  });

  describe('Named numeric formats', () => {
    it('measure with named format in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        measures:
        - name: total_amount
          sql: amount
          type: sum
          format: accounting_2
        - name: bytes
          sql: bytes
          type: sum
          format: abbr_3
      `);

      await compiler.compile();

      const { measures } = metaTransformer.cubes[0].config;
      const totalAmount = measures.find((m) => m.name === 'Orders.total_amount');
      expect(totalAmount).toBeDefined();
      expect(totalAmount!.format).toEqual({ type: 'custom-numeric', value: '(,.2~f', alias: 'accounting_2' });

      const bytes = measures.find((m) => m.name === 'Orders.bytes');
      expect(bytes).toBeDefined();
      expect(bytes!.format).toEqual({ type: 'custom-numeric', value: '.3~s', alias: 'abbr_3' });
    });

    it('number dimension with named format in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        - name: price
          sql: price
          type: number
          format: currency_1
        - name: population
          sql: population
          type: number
          format: abbr
      `);

      await compiler.compile();

      const { dimensions } = metaTransformer.cubes[0].config;
      const price = dimensions.find((d) => d.name === 'Orders.price');
      expect(price).toBeDefined();
      expect(price!.format).toEqual({ type: 'custom-numeric', value: '$,.1~f', alias: 'currency_1' });

      const population = dimensions.find((d) => d.name === 'Orders.population');
      expect(population).toBeDefined();
      expect(population!.format).toEqual({ type: 'custom-numeric', value: '.2~s', alias: 'abbr' });
    });

    it('formatDescription for all format variants', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        - name: price
          sql: price
          type: number
          format: currency_1
        - name: status
          sql: status
          type: string
        - name: created_at
          sql: created_at
          type: time
        measures:
        - name: count
          sql: id
          type: count
        - name: revenue
          sql: amount
          type: sum
          format: currency
          currency: eur
        - name: rate
          sql: rate
          type: number
          format: percent
        - name: total
          sql: amount
          type: sum
          format: number
        - name: bytes
          sql: bytes
          type: sum
          format: abbr_3
        - name: balance
          sql: balance
          type: sum
          format: accounting_2
        - name: order_id
          sql: id
          type: number
          format: id
        - name: custom_amount
          sql: amount
          type: sum
          format: "$,.0f"
      `);

      await compiler.compile();

      const { measures, dimensions } = metaTransformer.cubes[0].config;

      const pick = (list: any[], name: string) => {
        const m = list.find((x) => x.name === name);
        return { format: m?.format, formatDescription: m?.formatDescription, currency: m?.currency };
      };

      expect({
        measures: {
          count_no_format: pick(measures, 'Orders.count'),
          revenue_currency: pick(measures, 'Orders.revenue'),
          rate_percent: pick(measures, 'Orders.rate'),
          total_number: pick(measures, 'Orders.total'),
          bytes_abbr_3: pick(measures, 'Orders.bytes'),
          balance_accounting_2: pick(measures, 'Orders.balance'),
          order_id_id: pick(measures, 'Orders.order_id'),
          custom_amount_d3: pick(measures, 'Orders.custom_amount'),
        },
        dimensions: {
          id_number_no_format: pick(dimensions, 'Orders.id'),
          price_currency_1: pick(dimensions, 'Orders.price'),
          status_string: pick(dimensions, 'Orders.status'),
          created_at_time: pick(dimensions, 'Orders.created_at'),
        },
      }).toMatchSnapshot();
    });

    it('invalid named format in YAML - error', async () => {
      const { compiler } = prepareYamlCompiler(`
      cubes:
      - name: Orders
        sql: "select * from tbl"
        dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
        measures:
        - name: total_amount
          sql: amount
          type: sum
          format: unknown_format
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain('format');
      }
    });
  });

  describe('View groups', () => {
    it('standalone view_groups definition', async () => {
      const { compiler, metaTransformer, viewGroupEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
    title: Sales
    description: Sales related views
    views:
      - revenue
      `);

      await compiler.compile();

      expect(viewGroupEvaluator.viewGroupList).toEqual(['sales']);
      expect(metaTransformer.viewGroups).toEqual([{
        name: 'sales',
        title: 'Sales',
        description: 'Sales related views',
        views: ['revenue'],
      }]);

      const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
      expect(revenueView?.config.viewGroups).toEqual(['sales']);
    });

    it('view_group property on individual views', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    view_group: sales
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
      `);

      await compiler.compile();

      expect(metaTransformer.viewGroups).toEqual([{
        name: 'sales',
        views: ['revenue'],
      }]);

      const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
      expect(revenueView?.config.viewGroups).toEqual(['sales']);
    });

    it('merges standalone view_groups with view-level view_group', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: customers
    sql_table: customers
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    view_group: sales
    cubes:
      - join_path: orders
        includes: '*'

  - name: customers_view
    cubes:
      - join_path: customers
        includes: '*'

view_groups:
  - name: sales
    title: Sales
    description: Sales related views
    views:
      - customers_view
      `);

      await compiler.compile();

      const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
      expect(salesGroup).toBeDefined();
      expect(salesGroup?.title).toBe('Sales');
      expect(salesGroup?.description).toBe('Sales related views');
      expect(salesGroup?.views).toContain('customers_view');
      expect(salesGroup?.views).toContain('revenue');
    });

    it('multiple view_groups', async () => {
      const { compiler, metaTransformer, viewGroupEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

  - name: users
    sql_table: users
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: orders
        includes: '*'

  - name: users_view
    cubes:
      - join_path: users
        includes: '*'

view_groups:
  - name: sales
    title: Sales
    views:
      - revenue
  - name: people
    title: People
    description: People related views
    views:
      - users_view
      `);

      await compiler.compile();

      expect(viewGroupEvaluator.viewGroupList).toEqual(['sales', 'people']);
      expect(metaTransformer.viewGroups).toHaveLength(2);

      const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
      expect(salesGroup?.title).toBe('Sales');
      expect(salesGroup?.views).toEqual(['revenue']);

      const peopleGroup = metaTransformer.viewGroups.find(g => g.name === 'people');
      expect(peopleGroup?.title).toBe('People');
      expect(peopleGroup?.description).toBe('People related views');
      expect(peopleGroup?.views).toEqual(['users_view']);
    });

    it('detects duplicate view group names', async () => {
      const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

view_groups:
  - name: sales
    views:
      - some_view
  - name: sales
    views:
      - other_view
      `);

      try {
        await compiler.compile();
        throw new Error('compile must return an error');
      } catch (e: any) {
        expect(e.message).toContain("Found duplicate view group name 'sales'");
      }
    });

    it('empty view_groups section', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

view_groups:
      `);

      await compiler.compile();
      expect(metaTransformer.viewGroups).toEqual([]);
    });

    it('view_group without title or description', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
    views:
      - revenue
      `);

      await compiler.compile();

      expect(metaTransformer.viewGroups).toEqual([{
        name: 'sales',
        views: ['revenue'],
      }]);
    });

    it('cubes are not assigned to view groups', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
    views:
      - revenue
      `);

      await compiler.compile();

      const ordersCube = metaTransformer.cubes.find(c => c.config.name === 'orders');
      expect(ordersCube?.config.viewGroups).toBeUndefined();
    });

    it('plural view_groups property on view in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    view_groups:
      - sales
      - finance
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
    title: Sales
  - name: finance
    title: Finance
      `);

      await compiler.compile();

      const salesGroup = metaTransformer.viewGroups.find(g => g.name === 'sales');
      expect(salesGroup?.title).toBe('Sales');
      expect(salesGroup?.views).toContain('revenue');

      const financeGroup = metaTransformer.viewGroups.find(g => g.name === 'finance');
      expect(financeGroup?.title).toBe('Finance');
      expect(financeGroup?.views).toContain('revenue');

      const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
      expect(revenueView?.config.viewGroups).toEqual(['sales', 'finance']);
    });

    it('singular view_group sets viewGroups', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    view_group: sales
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
      `);

      await compiler.compile();

      expect(metaTransformer.viewGroups).toHaveLength(1);

      const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
      expect(revenueView?.config.viewGroups).toEqual(['sales']);
    });

    it('merges singular view_group and plural view_groups in YAML', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    measures:
      - name: count
        type: count
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

views:
  - name: revenue
    view_group: sales
    view_groups:
      - finance
    cubes:
      - join_path: orders
        includes: '*'

view_groups:
  - name: sales
  - name: finance
      `);

      await compiler.compile();

      expect(metaTransformer.viewGroups).toHaveLength(2);

      const revenueView = metaTransformer.cubes.find(c => c.config.name === 'revenue');
      expect(revenueView?.config.viewGroups).toEqual(['sales', 'finance']);
      expect(metaTransformer.viewGroups.find(g => g.name === 'sales')?.views).toContain('revenue');
      expect(metaTransformer.viewGroups.find(g => g.name === 'finance')?.views).toContain('revenue');
    });
  });

  describe('Mask SQL with shorthand', () => {
    it('userAttributes shorthand in mask sql should compile and resolve', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
        mask:
          sql: "CASE WHEN { userAttributes.hasStatusAccess } THEN {CUBE}.status ELSE '***' END"
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes: []
        member_masking:
          includes: "*"
      `);

      await compilers.compiler.compile();

      const dim = compilers.cubeEvaluator.cubeFromPath('orders').dimensions.status;
      const maskSql = (dim as any).mask.sql.toString();
      expect(maskSql).toContain('SECURITY_CONTEXT.cubeCloud.userAttributes.hasStatusAccess');
      expect(maskSql).toContain('CUBE');
      expect(maskSql).not.toMatch(/[^.}]userAttributes\.hasStatusAccess/);

      const query = new PostgresQuery(
        compilers,
        {
          measures: ['orders.count'],
          dimensions: ['orders.status'],
          maskedMembers: [{ member: 'orders.status' }],
          contextSymbols: {
            securityContext: { cubeCloud: { userAttributes: { hasStatusAccess: true } } }
          }
        }
      );
      const sql = query.buildSqlAndParams();
      expect(sql[0]).toContain('"orders".status');
      expect(sql[0]).toContain('CASE WHEN');
    });

    it('user_attributes shorthand in mask sql should compile and resolve', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
        mask:
          sql: "CASE WHEN { user_attributes.hasStatusAccess } THEN {CUBE}.status ELSE '***' END"
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes: []
        member_masking:
          includes: "*"
      `);

      await compilers.compiler.compile();

      const dim = compilers.cubeEvaluator.cubeFromPath('orders').dimensions.status;
      const maskSql = (dim as any).mask.sql.toString();
      expect(maskSql).toContain('SECURITY_CONTEXT.cubeCloud.userAttributes.hasStatusAccess');
    });

    it('groups shorthand in mask sql should compile and resolve', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: secret
        sql: price
        type: number
        mask:
          sql: "CASE WHEN {CUBE}.product_id IN ({groups}) THEN {CUBE}.price ELSE -1 END"
    measures:
      - name: count
        type: count
    access_policy:
      - group: "*"
        member_level:
          includes: []
        member_masking:
          includes: "*"
      `);

      await compilers.compiler.compile();

      const dim = compilers.cubeEvaluator.cubeFromPath('orders').dimensions.secret;
      const maskSql = (dim as any).mask.sql.toString();
      expect(maskSql).toContain('SECURITY_CONTEXT.cubeCloud.groups');
    });

    // Regression tests for mask.sql authored with different cube reference
    // styles on a cube member that is re-exposed through a prefixed view.
    // The mask must compile against the owning cube, not the view, so:
    //   * {cube.member}, {CUBE.member}, {CUBE}.column and {cube}.column
    //     all resolve to the underlying cube's alias.
    //   * The view's prefixed member ("users_city") does not collide with
    //     mask-sql references authored as "city" on the underlying cube.
    //   * Empty groups arrays in {groups.filter(...)} do not emit invalid
    //     `IN ()` SQL.
    describe('Mask SQL through prefixed view (cross-cube references)', () => {
      const buildMaskViewCompilers = (maskThen: string) => prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: public.users
    public: false
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: city_sensitive_masked
        sql: city
        mask:
          sql: |
            CASE
              WHEN {groups.filter("'sensitive_data_access'")}
              THEN ${maskThen}
              ELSE '***MASKED***'
            END
        type: string
    measures:
      - name: count
        type: count

views:
  - name: users_secure_view
    public: true
    cubes:
      - join_path: users
        prefix: true
        includes:
          - id
          - city
          - city_sensitive_masked
          - count
    access_policy:
      - group: "*"
        member_level:
          includes:
            - users_id
            - users_count
        member_masking:
          includes: '*'
      `);

      const runBaseQuery = async (maskThen: string, groups: string[], useNativeSqlPlanner: boolean) => {
        const compilers = buildMaskViewCompilers(maskThen);
        await compilers.compiler.compile();
        const query = new PostgresQuery(compilers, {
          measures: ['users_secure_view.users_count'],
          dimensions: ['users_secure_view.users_city_sensitive_masked'],
          maskedMembers: [{ member: 'users_secure_view.users_city_sensitive_masked' }],
          contextSymbols: {
            securityContext: { cubeCloud: { groups } }
          },
          ...(useNativeSqlPlanner ? { useNativeSqlPlanner: true } : {})
        });
        return query.buildSqlAndParams();
      };

      const maskReferenceStyles = [
        ['{users.city}', 'cube.member'],
        ['{CUBE.city}', 'CUBE.member'],
        ['{CUBE}.city', 'CUBE.column'],
        ['{users}.city', 'cube.column'],
      ] as const;

      for (const [maskBody, label] of maskReferenceStyles) {
        it(`legacy BaseQuery: ${label} (${maskBody}) resolves through view`, async () => {
          const [sql] = await runBaseQuery(maskBody, ['sensitive_data_access'], false);
          expect(sql).toContain('"users".city');
        });

        it(`tesseract: ${label} (${maskBody}) resolves through view`, async () => {
          const [sql] = await runBaseQuery(maskBody, ['sensitive_data_access'], true);
          expect(sql).toContain('"users".city');
        });
      }

      it('legacy BaseQuery: empty groups array does not produce IN ()', async () => {
        const [sql] = await runBaseQuery('{users.city}', [], false);
        expect(sql).not.toMatch(/IN\s*\(\s*\)/);
      });

      it('tesseract: empty groups array does not produce IN ()', async () => {
        const [sql] = await runBaseQuery('{users.city}', [], true);
        expect(sql).not.toMatch(/IN\s*\(\s*\)/);
      });
    });
  });

  describe('Conditional masking with row-level filters (memberMaskFilters)', () => {
    it('generates CASE WHEN with row filter for masked members that have conditional full access', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: public.users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: data_region
        sql: data_region
        type: string
    measures:
      - name: count
        type: count
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['users.count'],
        dimensions: ['users.city'],
        maskedMembers: [{
          member: 'users.city',
          filter: {
            member: 'users.data_region',
            operator: 'equals',
            values: ['RESEARCH', 'DEMO'],
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toMatch(/CASE\s+WHEN/);
      expect(sql).toMatch(/WHEN.*data_region.*THEN.*city.*ELSE.*NULL.*END/s);
    });

    it('generates CASE WHEN with AND row filter for multiple filter conditions', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: public.users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
      - name: data_region
        sql: data_region
        type: string
      - name: region_lock
        sql: region_lock
        type: number
    measures:
      - name: count
        type: count
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['users.count'],
        dimensions: ['users.city'],
        maskedMembers: [{
          member: 'users.city',
          filter: {
            and: [
              {
                member: 'users.data_region',
                operator: 'equals',
                values: ['RESEARCH'],
              },
              {
                member: 'users.region_lock',
                operator: 'equals',
                values: ['0'],
              }
            ]
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toMatch(/CASE\s+WHEN/);
      expect(sql).toMatch(/WHEN.*AND.*THEN.*city.*ELSE.*NULL.*END/s);
    });

    it('uses mask.sql as the ELSE branch when dimension has a custom mask', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: public.users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
        mask:
          sql: "'***MASKED***'"
      - name: data_region
        sql: data_region
        type: string
    measures:
      - name: count
        type: count
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['users.count'],
        dimensions: ['users.city'],
        maskedMembers: [{
          member: 'users.city',
          filter: {
            member: 'users.data_region',
            operator: 'equals',
            values: ['RESEARCH'],
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toMatch(/CASE\s+WHEN/);
      expect(sql).toMatch(/WHEN.*data_region.*THEN.*city.*ELSE.*MASKED.*END/s);
    });

    it('applies regular masking (no CASE WHEN) when no memberMaskFilters', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: users
    sql_table: public.users
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: city
        sql: city
        type: string
    measures:
      - name: count
        type: count
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['users.count'],
        dimensions: ['users.city'],
        maskedMembers: [{ member: 'users.city' }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).not.toMatch(/CASE\s+WHEN/);
      expect(sql).toContain('NULL');
    });

    it('renders the mask value (no CASE WHEN) for aggregate measures when the filter member is not in the group by', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: transactions
    sql_table: public.transactions
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: string
      - name: owner_id
        sql: owner_id
        type: string
    measures:
      - name: total_cost
        sql: cost
        type: sum
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['transactions.total_cost'],
        dimensions: ['transactions.org_id'],
        maskedMembers: [{
          member: 'transactions.total_cost',
          filter: {
            member: 'transactions.owner_id',
            operator: 'equals',
            values: ['42'],
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      // The aggregate measure must not be wrapped in a row-grain CASE WHEN that
      // references the (ungrouped) filter column. It should fall back to the mask.
      expect(sql).not.toMatch(/CASE\s+WHEN/);
      expect(sql).not.toMatch(/owner_id\s*=/);
      expect(sql).toContain('NULL');
    });

    it('uses the static mask value for aggregate measures when the filter member is not in the group by', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: transactions
    sql_table: public.transactions
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: string
      - name: owner_id
        sql: owner_id
        type: string
    measures:
      - name: total_cost
        sql: cost
        type: sum
        mask: -1
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['transactions.total_cost'],
        dimensions: ['transactions.org_id'],
        maskedMembers: [{
          member: 'transactions.total_cost',
          filter: {
            member: 'transactions.owner_id',
            operator: 'equals',
            values: ['42'],
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).not.toMatch(/CASE\s+WHEN/);
      expect(sql).not.toMatch(/owner_id\s*=/);
      expect(sql).toMatch(/-1\s+"transactions__total_cost"/);
    });

    it('still applies conditional CASE WHEN masking for aggregate measures when the filter member is in the group by', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: transactions
    sql_table: public.transactions
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: org_id
        sql: org_id
        type: string
      - name: owner_id
        sql: owner_id
        type: string
    measures:
      - name: total_cost
        sql: cost
        type: sum
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['transactions.total_cost'],
        dimensions: ['transactions.org_id', 'transactions.owner_id'],
        maskedMembers: [{
          member: 'transactions.total_cost',
          filter: {
            member: 'transactions.owner_id',
            operator: 'equals',
            values: ['42'],
          }
        }],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toMatch(/CASE\s+WHEN/);
      expect(sql).toMatch(/owner_id/);
    });

    it('does not recurse when filter member is also masked', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: items
    sql_table: public.items
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: product_id
        sql: product_id
        type: number
      - name: price
        sql: price
        type: number
        mask: -1
    measures:
      - name: count
        type: count
      `);

      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: ['items.count'],
        dimensions: ['items.product_id', 'items.price'],
        maskedMembers: [
          {
            member: 'items.product_id',
            filter: { member: 'items.product_id', operator: 'lte', values: ['3'] }
          },
          {
            member: 'items.price',
            filter: { member: 'items.product_id', operator: 'lte', values: ['3'] }
          },
        ],
      });
      const [sql] = query.buildSqlAndParams();
      expect(sql).toMatch(/CASE\s+WHEN/);
      expect(sql).toMatch(/product_id/);
      expect(sql).not.toMatch(/Maximum call stack/);
    });
  });

  describe('Multi-stage filter directive', () => {
    it('compiles YAML with filter on multi-stage measure', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: public.orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
      - name: city
        sql: city
        type: string
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: revenue
        sql: amount
        type: sum
      - name: revenue_filtered
        sql: "{revenue}"
        multi_stage: true
        type: number
        filter:
          mode: relative
          exclude:
            - CUBE.status
          include:
            - member: orders.created_at
              operator: afterDate
              values:
                - "2024-01-01"
      `);

      await compilers.compiler.compile();

      const measure = compilers.cubeEvaluator.cubeFromPath('orders').measures.revenue_filtered as any;
      expect(measure.multiStage).toBe(true);
      expect(measure.filter).toBeDefined();
      expect(measure.filter.mode).toBe('relative');
      expect(typeof measure.filter.exclude).toBe('function');
      expect(Array.isArray(measure.filter.include)).toBe(true);
      expect(measure.filter.include[0].member).toBe('orders.created_at');
      expect(measure.filter.include[0].operator).toBe('afterDate');
      expect(measure.filter.include[0].values).toEqual(['2024-01-01']);
    });
  });
});
