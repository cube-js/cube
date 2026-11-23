import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Postgres gives an untyped parameter the type of the column it is compared with. A `real`
// column then rounds the bound to float4, so `x >= 1.0000001` runs as `x >= 1`, and an
// integer column rejects a fractional bound outright.
describe('Number filter parameter type', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: amounts
    sql: >
      SELECT * FROM (VALUES
        (1, 1::real, 1::double precision, 1::numeric(10, 2), 1::int, 1::bigint),
        (2, 2::real, 2::double precision, 2::numeric(10, 2), 2::int, 9007199254740993::bigint),
        (3, 3::real, 3::double precision, 3::numeric(10, 2), 3::int, 9223372036854775807::bigint)
      ) AS t(id, real_amount, double_amount, numeric_amount, int_amount, bigint_amount)

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

    measures:
      - name: count
        type: count

      - name: real_total
        sql: real_amount
        type: sum
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

    it('bound past int4', async () => {
      await expectIds([filter('int_amount', 'lt', '3000000000')], [1, 2, 3]);
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
});
