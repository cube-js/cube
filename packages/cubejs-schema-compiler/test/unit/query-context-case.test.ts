import { PostgresQuery } from '../../src';
import { CubeSymbols } from '../../src/compiler/CubeSymbols';
import { QueryContext, QueryContextData } from '../../src/compiler/QueryContext';
import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

const yamlModel = (amountCase: string, extraMeasures = '') => `
cubes:
  - name: orders
    sql: SELECT * FROM orders
    joins:
      - name: customers
        sql: "{CUBE}.customer_id = {customers}.id"
        relationship: many_to_one
    measures:
      - name: count
        type: count
      - name: amount_raw
        sql: amount
        type: sum
      - name: amount_at_depth_3
        sql: amount_3
        type: sum
      - name: amount_at_depth_0
        sql: amount_0
        type: sum
      - name: total
        sql: total
        type: sum
      - name: amount
        multi_stage: true
        type: number
        case:
${amountCase}
${extraMeasures}
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: account_depth
        sql: account_depth
        type: number
      - name: account_level1
        sql: account_level1
        type: string
      - name: account_level9
        sql: account_level9
        type: string
      - name: created_at
        sql: created_at
        type: time
  - name: customers
    sql: SELECT * FROM customers
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: region
        sql: region
        type: string
views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - amount
          - total
          - account_level9
          - account_depth
`;

const validCase = `
          when:
            - context: "{QUERY_CONTEXT.filtered(account_depth)}"
              sql: "{amount_raw}"
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level9) and not QUERY_CONTEXT.has_measure(CUBE.total)}"
              sql: "{amount_at_depth_3}"
            - context: "{QUERY_CONTEXT.single_value(customers.region) or QUERY_CONTEXT.grouped(CUBE.created_at)}"
              sql: "{amount_at_depth_0}"
          else:
            sql: "{amount_at_depth_0}"
`;

const emptyQuery: QueryContextData = { dimensions: [], timeDimensions: [], measures: [], filters: [] };

const query = (overrides: Partial<QueryContextData>): QueryContextData => ({ ...emptyQuery, ...overrides });

describe('QUERY_CONTEXT conditions in measure case', () => {
  describe('YAML model', () => {
    it('compiles and exposes the condition functions with resolved references', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase));
      await compilers.compile();

      const { cubeEvaluator } = compilers;
      const measure = cubeEvaluator.measureByPath('orders.amount');
      const { when } = measure.case!;
      expect(when).toHaveLength(3);
      expect(measure.case!.else!.sql).toEqual(expect.any(Function));

      expect(when[0].context).toEqual(expect.any(Function));
      expect(cubeEvaluator.funcArguments(when[0].context!)).toEqual(['QUERY_CONTEXT', 'account_depth']);
      expect(when[0].contextReferences).toEqual(['orders.account_depth']);

      expect(cubeEvaluator.funcArguments(when[1].context!)).toEqual(['QUERY_CONTEXT', 'CUBE']);
      expect(when[1].contextReferences).toEqual(['orders.account_level9', 'orders.total']);

      expect(cubeEvaluator.funcArguments(when[2].context!)).toEqual(['QUERY_CONTEXT', 'customers', 'CUBE']);
      expect(when[2].contextReferences).toEqual(['customers.region', 'orders.created_at']);
    });

    it('evaluates the conditions against a query', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase));
      await compilers.compile();

      const { cubeEvaluator } = compilers;
      const { when } = cubeEvaluator.measureByPath('orders.amount').case!;
      const evaluate = (index: number, data: QueryContextData) => cubeEvaluator
        .evaluateQueryContextCondition('orders', when[index].context!, new QueryContext(data));

      expect(evaluate(0, emptyQuery)).toBe(false);
      expect(evaluate(0, query({ filters: [{ member: 'orders.account_depth', operator: 'gte', values: ['3'] }] }))).toBe(true);

      expect(evaluate(1, query({ dimensions: ['orders.account_level9'] }))).toBe(true);
      expect(evaluate(1, query({ dimensions: ['orders.account_level9'], measures: ['orders.total'] }))).toBe(false);
      expect(evaluate(1, query({ dimensions: ['orders.account_level1'] }))).toBe(false);

      expect(evaluate(2, query({ filters: [{ member: 'customers.region', operator: 'equals', values: ['EU'] }] }))).toBe(true);
      expect(evaluate(2, query({ filters: [{ member: 'customers.region', operator: 'equals', values: ['EU', 'US'] }] }))).toBe(false);
      expect(evaluate(2, query({ filters: [{ member: 'customers.region', operator: 'contains', values: ['EU'] }] }))).toBe(false);
      expect(evaluate(2, query({
        filters: [
          { member: 'customers.region', operator: 'equals', values: ['EU'] },
          { member: 'customers.region', operator: 'notEquals', values: ['US'] },
        ]
      }))).toBe(false);
      expect(evaluate(2, query({ timeDimensions: [{ dimension: 'orders.created_at', granularity: 'month' }] }))).toBe(true);
      expect(evaluate(2, query({ timeDimensions: [{ dimension: 'orders.created_at', dateRange: ['2024-01-01', '2024-12-31'] }] }))).toBe(false);
    });

    it('reads a time dimension as grouped only with a granularity and as filtered only with a date range', () => {
      const grouped = new QueryContext(query({ timeDimensions: [{ dimension: 'orders.created_at', granularity: 'day' }] }));
      expect(grouped.grouped('orders.created_at')).toBe(true);
      expect(grouped.filtered('orders.created_at')).toBe(false);

      const ranged = new QueryContext(query({ timeDimensions: [{ dimension: 'orders.created_at', dateRange: ['2024-01-01', '2024-12-31'] }] }));
      expect(ranged.grouped('orders.created_at')).toBe(false);
      expect(ranged.filtered('orders.created_at')).toBe(true);
      expect(ranged.single_value('orders.created_at')).toBe(false);
    });

    it('is reachable through the query object the native planner holds', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase));
      await compilers.compile();

      const { when } = compilers.cubeEvaluator.measureByPath('orders.amount').case!;
      const baseQuery = compilers.createQuery(PostgresQuery, { measures: ['orders.count'], timezone: 'UTC' });
      expect(baseQuery.evaluateQueryContextCondition(when[1].context!, 'orders', query({ dimensions: ['orders.account_level9'] }))).toBe(true);
      expect(baseQuery.evaluateQueryContextCondition(when[1].context!, 'orders', emptyQuery)).toBe(false);
    });

    it('rejects an unknown member', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level5)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
      `));
      await expect(compilers.compile()).rejects.toThrow(
        /Measure 'orders.amount' case.when\[0\].context: orders.account_level5 cannot be resolved. There's no such member or cube/
      );
    });

    it('reports every member of the wrong kind, on both sides of a short-circuit', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - context: "{QUERY_CONTEXT.grouped(CUBE.total) and QUERY_CONTEXT.has_measure(CUBE.account_depth)}"
              sql: "{amount_at_depth_3}"
            - context: "{QUERY_CONTEXT.has_measure(CUBE.total) or QUERY_CONTEXT.grouped(CUBE.account_level9) or QUERY_CONTEXT.single_value(CUBE.total)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
      `));
      try {
        await compilers.compile();

        throw new Error('compile should fail');
      } catch (e: any) {
        expect(e.message).toMatch(/case.when\[0\].context: QUERY_CONTEXT.grouped\(\) expects a dimension, got measure 'orders.total'/);
        expect(e.message).toMatch(/case.when\[0\].context: QUERY_CONTEXT.has_measure\(\) expects a measure, got dimension 'orders.account_depth'/);
        expect(e.message).not.toMatch(/case.when\[1\]/);
      }
    });

    it('rejects QUERY_CONTEXT in a nested member function', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase, `
      - name: labelled
        multi_stage: true
        type: number
        case:
          when:
            - sql: "{amount_raw}"
              label:
                sql: "{QUERY_CONTEXT.grouped(CUBE.account_depth)}"
          else:
            label: other
      `));
      await expect(compilers.compile()).rejects.toThrow(
        /Member 'orders.labelled': QUERY_CONTEXT is only supported in case.when\[\].context/
      );
    });

    it('rejects an unknown predicate', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - context: "{QUERY_CONTEXT.selected(CUBE.account_level9)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
      `));
      await expect(compilers.compile()).rejects.toThrow(
        /QUERY_CONTEXT.selected is not supported. Available predicates: grouped, filtered, single_value, has_measure/
      );
    });

    it('rejects other context symbols inside the condition', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level9) and SECURITY_CONTEXT.admin}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
      `));
      await expect(compilers.compile()).rejects.toThrow(
        /Only QUERY_CONTEXT and member references are allowed in case.when\[\].context, found SECURITY_CONTEXT/
      );
    });

    it('rejects QUERY_CONTEXT outside of case.when[].context', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase, `
      - name: depth_amount
        sql: "{QUERY_CONTEXT.grouped(CUBE.account_level9)}"
        type: sum
      `));
      await expect(compilers.compile()).rejects.toThrow(
        /Member 'orders.depth_amount': QUERY_CONTEXT is only supported in case.when\[\].context/
      );
    });

    it('requires multi_stage for a context case', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase).replace(/^\s*multi_stage: true\n/m, ''));
      await expect(compilers.compile()).rejects.toThrow(/"measures.amount.case" is not allowed/);
    });

    it('rejects a when item that mixes value and context', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - value: EUR
              context: "{QUERY_CONTEXT.grouped(CUBE.account_level9)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
      `));
      await expect(compilers.compile()).rejects.toThrow(/"measures.amount.case" does not match any of the allowed types/);
    });

    it('requires an else branch', async () => {
      const compilers = prepareYamlCompiler(yamlModel(`
          when:
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level9)}"
              sql: "{amount_at_depth_3}"
      `));
      await expect(compilers.compile()).rejects.toThrow(/"measures.amount.case" does not match any of the allowed types/);
    });

    it('refuses the legacy planner for a direct and a transitive reference', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase, `
      - name: amount_share
        multi_stage: true
        sql: "{amount} / {total}"
        type: number
      `));
      await compilers.compile();

      for (const measure of ['orders.amount', 'orders.amount_share']) {
        const buildLegacy = () => compilers.createQuery(PostgresQuery, {
          measures: [measure],
          dimensions: ['orders.account_level9'],
          timezone: 'UTC',
          useNativeSqlPlanner: false,
        }).buildSqlAndParams();
        expect(buildLegacy).toThrow(
          /Measure 'orders.amount' uses QUERY_CONTEXT conditions, which require the Tesseract SQL planner/
        );
      }
    });

    it('lets Tesseract pick the branch from the query', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase));
      await compilers.compile();

      const buildSql = (options: Record<string, unknown>) => compilers.createQuery(PostgresQuery, {
        ...options,
        timezone: 'UTC',
        useNativeSqlPlanner: true,
      }).buildSqlAndParams()[0];

      const byLevel9 = buildSql({ measures: ['orders.amount'], dimensions: ['orders.account_level9'] });
      expect(byLevel9).toContain('amount_3');
      expect(byLevel9).not.toContain('amount_0');

      const withTotal = buildSql({ measures: ['orders.amount', 'orders.total'], dimensions: ['orders.account_level9'] });
      expect(withTotal).toContain('amount_0');
      expect(withTotal).not.toContain('amount_3');

      const filtered = buildSql({
        measures: ['orders.amount'],
        filters: [{ member: 'orders.account_depth', operator: 'gte', values: ['3'] }],
      });
      expect(filtered).toMatch(/sum\("orders"\.amount\)/);
      expect(filtered).not.toMatch(/amount_[03]/);
    });

    it('resolves view members to the cube members the conditions name', async () => {
      const compilers = prepareYamlCompiler(yamlModel(validCase));
      await compilers.compile();

      expect(compilers.cubeEvaluator.resolveQueryContextMembers(query({
        dimensions: ['orders_view.account_level9'],
        measures: ['orders_view.amount', 'customers.region'],
        filters: [{ member: 'orders_view.account_depth', operator: 'gte', values: ['3'] }],
      }))).toEqual(query({
        dimensions: ['orders.account_level9'],
        measures: ['orders.amount', 'customers.region'],
        filters: [{ member: 'orders.account_depth', operator: 'gte', values: ['3'] }],
      }));

      const buildSql = (options: Record<string, unknown>) => compilers.createQuery(PostgresQuery, {
        ...options,
        timezone: 'UTC',
        useNativeSqlPlanner: true,
      }).buildSqlAndParams()[0];

      expect(buildSql({ measures: ['orders_view.amount'], dimensions: ['orders_view.account_level9'] })).toContain('amount_3');
      expect(buildSql({ measures: ['orders_view.amount', 'orders_view.total'], dimensions: ['orders_view.account_level9'] })).toContain('amount_0');
    });

    it('keeps the resolved references of each cube under extends', async () => {
      const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: SELECT * FROM orders
    measures:
      - name: amount_raw
        sql: amount
        type: sum
      - name: amount
        multi_stage: true
        type: number
        case:
          when:
            - context: "{QUERY_CONTEXT.filtered(CUBE.account_depth)}"
              sql: "{amount_raw}"
          else:
            sql: "{amount_raw}"
    dimensions:
      - name: account_depth
        sql: account_depth
        type: number
  - name: orders_ext
    extends: orders
    sql: SELECT * FROM orders_ext
      `);
      await compilers.compile();

      const { cubeEvaluator } = compilers;
      expect(cubeEvaluator.measureByPath('orders.amount').case!.when[0].contextReferences).toEqual(['orders.account_depth']);
      expect(cubeEvaluator.measureByPath('orders_ext.amount').case!.when[0].contextReferences).toEqual(['orders_ext.account_depth']);
    });

    it('keeps the unavailable QUERY_CONTEXT inert except for predicate access', () => {
      const unavailable = CubeSymbols.queryContextUnavailable() as Record<string, unknown>;
      expect(() => JSON.stringify(unavailable)).not.toThrow();
      expect(unavailable.then).toBeUndefined();
      expect(() => unavailable.grouped).toThrow(/QUERY_CONTEXT is only available in measure case.when\[\].context/);
    });
  });

  describe('JS model', () => {
    const jsModel = (amountCase: string) => `
      cube('orders', {
        sql: \`SELECT * FROM orders\`,
        measures: {
          count: { type: 'count' },
          amount_at_depth_3: { sql: 'amount_3', type: 'sum' },
          amount_at_depth_0: { sql: 'amount_0', type: 'sum' },
          total: { sql: 'total', type: 'sum' },
          amount: {
            multiStage: true,
            type: 'number',
            case: ${amountCase},
            meta: { helper: class Helper {} },
          },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          account_level9: { sql: 'account_level9', type: 'string' },
        },
      });
    `;

    it('wraps a bare expression into a condition function', async () => {
      const compilers = prepareJsCompiler(jsModel(`{
        when: [
          { context: QUERY_CONTEXT.grouped(CUBE.account_level9) && !QUERY_CONTEXT.hasMeasure(total), sql: \`\${CUBE.amount_at_depth_3}\` },
        ],
        else: { sql: \`\${CUBE.amount_at_depth_0}\` },
      }`));
      await compilers.compile();

      const { cubeEvaluator } = compilers;
      const { when } = cubeEvaluator.measureByPath('orders.amount').case!;
      expect(cubeEvaluator.funcArguments(when[0].context!)).toEqual(['QUERY_CONTEXT', 'CUBE', 'total']);
      expect(when[0].contextReferences).toEqual(['orders.account_level9', 'orders.total']);

      const evaluate = (data: QueryContextData) => cubeEvaluator
        .evaluateQueryContextCondition('orders', when[0].context!, new QueryContext(data));
      expect(evaluate(query({ dimensions: ['orders.account_level9'] }))).toBe(true);
      expect(evaluate(query({ dimensions: ['orders.account_level9'], measures: ['orders.total'] }))).toBe(false);
    });

    it('rejects a template string condition', async () => {
      const compilers = prepareJsCompiler(jsModel(`{
        when: [
          { context: \`\${QUERY_CONTEXT.grouped(CUBE.account_level9)}\`, sql: \`\${CUBE.amount_at_depth_3}\` },
        ],
        else: { sql: \`\${CUBE.amount_at_depth_0}\` },
      }`));
      await expect(compilers.compile()).rejects.toThrow(
        /case.when\[0\].context: case.when\[\].context must return a boolean, got string/
      );
    });
  });
});
