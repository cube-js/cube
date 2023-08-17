import moment from 'moment-timezone';
import { ClickHouseQuery } from '../../src/adapter/ClickHouseQuery';
import { prepareCompiler } from './PrepareCompiler';
import { BaseDimension, BaseMeasure } from '../../src';

interface HashOptions {
  id: string,
}

describe('ClickHouseQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      }
    });
    `);

  const getQuery = async (overrides = {}) => {
    await compiler.compile();

    return new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [
        'visitors.createdAt',
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30'],
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.createdAt',
      }, {
        id: 'visitors.count',
      }],
      ...overrides,
    });
  };

  const getDimensionHash = (query: ClickHouseQuery): HashOptions => query.order[0];
  const getMeasureHash = (query: ClickHouseQuery): HashOptions => query.order[1];

  describe('field methods', () => {
    it('gets the correct field alias for dimensions and measures', async () => {
      const query = await getQuery();

      expect(query.getFieldAlias(getDimensionHash(query).id)).toBe('`visitors__created_at`');
      expect(query.getFieldAlias(getMeasureHash(query).id)).toBe('`visitors__count`');
    });

    it('gets the correct field for dimensions and measures', async () => {
      const query = await getQuery();

      const dimensionField: BaseDimension = query.getField(getDimensionHash(query).id);
      const measureField: BaseMeasure = query.getField(getMeasureHash(query).id);

      expect(dimensionField.dimension).toBe(query.dimensions[0].dimension);
      expect(measureField.measure).toBe(query.measures[0].measure);
    });

    it('gets the correct field type for dimensions and measures', async () => {
      const query = await getQuery();

      const dimensionField: BaseDimension = query.getField(getDimensionHash(query).id);
      const measureField: BaseMeasure = query.getField(getMeasureHash(query).id);

      expect(query.getFieldType(dimensionField)).toBe('time');
      expect(query.getFieldType(measureField)).toBe('count');
    });
  });

  describe('environment', () => {
    afterEach(() => {
      delete process.env.CUBEJS_DB_CLICKHOUSE_WITHFILL;
    });

    it.each([true, false])('withFill returns the correct value given CUBEJS_DB_CLICKHOUSE_WITHFILL is %s', async (enabled) => {
      process.env.CUBEJS_DB_CLICKHOUSE_WITHFILL = `${enabled}`;

      const query = await getQuery();

      expect(query.withFill).toBe(enabled);
    });
  });

  describe('WITH FILL methods', () => {
    describe('maximumDateRange', () => {
      it.each`
        description | queryOverrides | expectedDateRange
        ${'no date range if the date range filters do not exist'} | ${
  {
    timeDimensions: [{
      dimension: 'visitors.createdAt',
      granularity: 'day',
    }],
  }
} | ${{ start: null, end: null }}
        ${'no date range if the date range filters do not have the dates set'} | ${
  {
    timeDimensions: [{
      dimension: 'visitors.createdAt',
      granularity: 'day',
    }],
    filters: [{
      member: 'visitors.createdAt',
      operator: 'inDateRange',
      values: [],
    }],
  }
} | ${{ start: null, end: null }}
        ${'returns the correct date range if the filter is applied'} | ${
  {
    timeDimensions: [{
      dimension: 'visitors.createdAt',
      granularity: 'day',
    }],
    filters: [{
      member: 'visitors.createdAt',
      operator: 'inDateRange',
      values: ['2017-01-01', '2017-01-30'],
    }],
  }
} | ${{ start: '2017-01-01T00:00:00.000', end: '2017-01-30T00:00:00.000' }}
        ${'returns the correct date range if the time dimension filter is applied'} | ${
  {}
} | ${{ start: '2017-01-01T00:00:00.000', end: '2017-01-30T00:00:00.000' }}
        ${'returns the maximum date range if the time dimension filter and filter property is applied'} | ${
  {
    timeDimensions: [{
      dimension: 'visitors.createdAt',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-30'],
    }],
    filters: [{
      member: 'visitors.createdAt',
      operator: 'inDateRange',
      values: ['2017-01-01', '2017-05-01'],
    }],
  }
} | ${{ start: '2016-12-01T00:00:00.000', end: '2017-05-01T00:00:00.000' }}
      `('$description', async ({ queryOverrides, expectedDateRange }) => {
        const query = await getQuery(queryOverrides);

        const dateRange = query.maximumDateRange();

        expect({
          start: dateRange.start ? dateRange.start.format(moment.HTML5_FMT.DATETIME_LOCAL_MS) : null,
          end: dateRange.end ? dateRange.end.format(moment.HTML5_FMT.DATETIME_LOCAL_MS) : null
        }).toStrictEqual(expectedDateRange);
      });
    });

    describe('withFillInterval', () => {
      it.each`
        granularity | result
        ${null} | ${''}
        ${'unknown'} | ${''}
        ${'quarter'} | ${' STEP INTERVAL 1 QUARTER'}
      `('returns $result if the granularity $granularity is given', async ({ granularity, result }) => {
        const query = await getQuery();

        expect(query.withFillInterval(granularity)).toBe(result);
      });
    });

    describe('withFillRange', () => {
      it.each`
        description | queryOverrides | expectedFillRange
        ${'returns a blank string if no date range filter is applied'} | ${
  {
    timeDimensions: [{
      dimension: 'visitors.createdAt',
      granularity: 'day',
    }],
  }
} | ${''}
                    ${'returns the correct fill range when in ASC order'} | ${
  {
    order: [{
      id: 'visitors.createdAt',
      desc: false,
    }, {
      id: 'visitors.count',
    }],
  }
} | ${' FROM parseDateTimeBestEffort(\'2017-01-01T00:00:00.000\') TO parseDateTimeBestEffort(\'2017-01-30T00:00:00.000\')'}
        ${'returns the correct fill range when in DESC order'} | ${
  {
    order: [{
      id: 'visitors.createdAt',
      desc: true,
    }, {
      id: 'visitors.count',
    }],
  }
} | ${' FROM parseDateTimeBestEffort(\'2017-01-30T00:00:00.000\') TO parseDateTimeBestEffort(\'2017-01-01T00:00:00.000\')'}
      `('$description', async ({ queryOverrides, expectedFillRange }) => {
        const query = await getQuery(queryOverrides);

        expect(query.withFillRange(getDimensionHash(query))).toBe(expectedFillRange);
      });
    });
  });

  describe('orderHashToString', () => {
    afterEach(() => {
      delete process.env.CUBEJS_DB_CLICKHOUSE_WITHFILL;
    });

    it.each`
        description | order | envEnabled | hash | expectedOrderString
        ${'returns null if no hash is given'} | ${
  {}
} | ${'true'} | ${(query) => null} | ${null}
        ${'returns null if the field cannot be found'} | ${
  {}
} | ${'true'} | ${(query) => ({ id: 'unknown' })} | ${null}
        ${'returns the normal order by string when withFill is disabled'} | ${
  {}
} | ${'false'} | ${(query) => getDimensionHash(query)} | ${'`visitors__created_at` ASC'}
        ${'returns the normal order by string when the field is not of type time'} | ${
  {
    order: [{
      id: 'visitors.createdAt',
    }, {
      id: 'visitors.count',
      desc: true,
    }],
  }
} | ${'true'} | ${(query) => getMeasureHash(query)} | ${'`visitors__count` DESC'}
        ${'returns the with fill order by string when withFill is enabled and the field is of type time'} | ${
  {
    order: [{
      id: 'visitors.createdAt',
      desc: true,
    }, {
      id: 'visitors.count',
    }],
  }
} | ${'true'} | ${(query) => getDimensionHash(query)} | ${'`visitors__created_at` DESC WITH FILL FROM parseDateTimeBestEffort(\'2017-01-30T00:00:00.000\') TO parseDateTimeBestEffort(\'2017-01-01T00:00:00.000\')'}
        `('$description', async ({ order, envEnabled, hash, expectedOrderString }) => {
      process.env.CUBEJS_DB_CLICKHOUSE_WITHFILL = envEnabled;

      const query = await getQuery(order);

      expect(query.orderHashToString(hash(query))).toBe(expectedOrderString);
    });
  });
});
