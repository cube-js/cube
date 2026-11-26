import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Number filter parameter type', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: amounts
    sql: >
      SELECT * FROM (VALUES
        (1, 1::real, 1::double precision, 1::numeric(10, 2), 1::int, 1::bigint, 1::money, '1'::text, 'a', '2024-01-01'::timestamp, TRUE),
        (2, 2::real, 2::double precision, 2::numeric(10, 2), 2::int, 9007199254740993::bigint, 2::money, '2'::text, '2', '2024-01-02'::timestamp, FALSE),
        (3, 3::real, 3::double precision, 3::numeric(10, 2), 3::int, 9223372036854775807::bigint, 3::money, '3'::text, 'c', '2024-01-03'::timestamp, TRUE)
      ) AS t(id, real_amount, double_amount, numeric_amount, int_amount, bigint_amount, money_amount, text_amount, label, created_at, flag)

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
        public: true

      - name: real_amount
        sql: real_amount
        type: number

      - name: double_amount
        sql: double_amount
        type: number

      - name: numeric_amount
        sql: numeric_amount
        type: number

      - name: int_amount
        sql: int_amount
        type: number

      - name: bigint_amount
        sql: bigint_amount
        type: number

      - name: money_amount
        sql: money_amount
        type: number

      - name: text_amount
        sql: text_amount
        type: number

    measures:
      - name: count
        type: count

      - name: real_total
        sql: real_amount
        type: sum

      - name: money_total
        sql: money_amount
        type: sum

      - name: labels
        sql: "string_agg(label, ',')"
        type: string

      - name: last_created_at
        sql: "max(created_at)"
        type: time

      - name: any_flag
        sql: "bool_or(flag)"
        type: boolean

      - name: max_label
        sql: label
        type: max

      - name: max_created_at
        sql: created_at
        type: max
  `);

  async function expectIds(filters: any[], ids: number[]) {
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, {
      measures: ['amounts.count'],
      dimensions: ['amounts.id'],
      filters,
      order: [{ id: 'amounts.id' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });
    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res.map((row: any) => row.amounts__id)).toEqual(ids);
  }

  const filter = (member: string, operator: string, ...values: string[]) => ({
    member: `amounts.${member}`,
    operator,
    values,
  });

  describe('real column', () => {
    it('gte with a bound float4 rounds down', async () => {
      await expectIds([filter('real_amount', 'gte', '1.0000001')], [2, 3]);
    });

    it('lte with a bound float4 rounds up', async () => {
      await expectIds([filter('real_amount', 'lte', '2.9999999')], [1, 2]);
    });

    it('equals with a bound float4 rounds to a stored value', async () => {
      await expectIds([filter('real_amount', 'equals', '1.0000001')], []);
    });

    it('equals with several values', async () => {
      await expectIds([filter('real_amount', 'equals', '1.0000001', '2')], [2]);
    });

    it('whole bound', async () => {
      await expectIds([filter('real_amount', 'gt', '1')], [2, 3]);
    });

    it('measure filter', async () => {
      await expectIds([filter('real_total', 'gte', '1.0000001')], [2, 3]);
    });
  });

  describe('double precision column', () => {
    it('fractional bound', async () => {
      await expectIds([filter('double_amount', 'gte', '1.0000001')], [2, 3]);
    });

    it('whole bound', async () => {
      await expectIds([filter('double_amount', 'lte', '2')], [1, 2]);
    });
  });

  describe('numeric column', () => {
    it('bound with more digits than the column scale', async () => {
      await expectIds([filter('numeric_amount', 'gte', '1.001')], [2, 3]);
    });

    it('whole bound', async () => {
      await expectIds([filter('numeric_amount', 'equals', '2')], [2]);
    });
  });

  describe('integer column', () => {
    it('whole bound', async () => {
      await expectIds([filter('int_amount', 'gte', '2')], [2, 3]);
    });

    it('fractional bound', async () => {
      await expectIds([filter('int_amount', 'gt', '1.5')], [2, 3]);
    });

    it('several values', async () => {
      await expectIds([filter('int_amount', 'equals', '1', '3')], [1, 3]);
    });

    it('bound in exponent notation', async () => {
      await expectIds([filter('int_amount', 'gte', '2e0')], [2, 3]);
    });

    it('whole bound with leading zeros', async () => {
      await expectIds([filter('int_amount', 'equals', '03')], [3]);
    });
  });

  describe('measure that is not a number', () => {
    it('string measure', async () => {
      await expectIds([filter('labels', 'equals', 'a')], [1]);
    });

    it('string measure with a value that reads as a number', async () => {
      await expectIds([filter('labels', 'equals', '2')], [2]);
    });

    it('time measure', async () => {
      await expectIds([filter('last_created_at', 'equals', '2024-01-02T00:00:00.000')], [2]);
    });

    it('boolean measure', async () => {
      await expectIds([filter('any_flag', 'equals', 'true')], [1, 3]);
    });

    it('max over a text column', async () => {
      await expectIds([filter('max_label', 'equals', 'c')], [3]);
    });

    it('max over a text column with a value that reads as a number', async () => {
      await expectIds([filter('max_label', 'equals', '2')], [2]);
    });

    it('max over a timestamp column', async () => {
      await expectIds([filter('max_created_at', 'gt', '2024-01-01T00:00:00.000')], [2, 3]);
    });
  });

  describe('bigint column', () => {
    it('bound past the float8 integer range', async () => {
      await expectIds([filter('bigint_amount', 'equals', '9007199254740993')], [2]);
    });

    it('int64 maximum', async () => {
      await expectIds([filter('bigint_amount', 'gte', '9223372036854775807')], [3]);
    });

    it('bound past int64', async () => {
      await expectIds([filter('bigint_amount', 'lt', '9223372036854775808')], [1, 2, 3]);
    });
  });

  // Whole bounds stay untyped: neither type has an operator against bigint or numeric.
  describe('column without a numeric operator', () => {
    it('money column', async () => {
      await expectIds([filter('money_amount', 'gte', '2')], [2, 3]);
    });

    it('money measure', async () => {
      await expectIds([filter('money_total', 'lt', '3')], [1, 2]);
    });

    it('text column', async () => {
      await expectIds([filter('text_amount', 'equals', '2')], [2]);
    });
  });
});
