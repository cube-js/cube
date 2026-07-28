import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

const DremioQuery = require('../../driver/DremioQuery');

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

describe('DremioQuery', () => {

  jest.setTimeout(10 * 60 * 1000); // Engine needs to spin up

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
    `
cube(\`sales\`, {
  sql: \` select * from public.sales \`,

  measures: {
    count: {
      type: 'count'
    }
  },
  dimensions: {
    category: {
      type: 'string',
      sql: 'category'
    },
    salesDatetime: {
      type: 'time',
      sql: 'sales_datetime'
    },
    isShiped: {
      type: 'boolean',
      sql: 'is_shiped',
    },
  }
});
`,
  );

  it('should use DATE_TRUNC for time granularity dimensions', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        timeDimensions: [
          {
            dimension: 'sales.salesDatetime',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'sales.salesDatetime',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      'DATE_TRUNC(\'day\', CONVERT_TIMEZONE(\'America/Los_Angeles\', "sales".sales_datetime))'
    );
  }));

  it('should cast BOOLEAN', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        filters: [
          {
            member: 'sales.isShiped',
            operator: 'equals',
            values: ['true']
          }
        ]
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      '("sales".is_shiped = CAST(? AS BOOLEAN))'
    );

    expect(queryAndParams[1]).toEqual(['true']);
  }));

  it('should cast timestamp', () => compiler.compile().then(() => {
    const query = new DremioQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        timeDimensions: [
          {
            dimension: 'sales.salesDatetime',
            granularity: 'day',
            dateRange: ['2017-01-01', '2017-01-02'],
          },
        ],
        timezone: 'America/Los_Angeles',
        order: [
          {
            id: 'sales.salesDatetime',
          },
        ],
      }
    );

    const queryAndParams = query.buildSqlAndParams();

    expect(queryAndParams[0]).toContain(
      '("sales".sales_datetime >= TO_TIMESTAMP(?, \'YYYY-MM-DD"T"HH24:MI:SS.FFF\') AND "sales".sales_datetime <= TO_TIMESTAMP(?, \'YYYY-MM-DD"T"HH24:MI:SS.FFF\'))'
    );
  }));

  const likeQuery = (operator: string, value: string) => new DremioQuery(
    { joinGraph, cubeEvaluator, compiler },
    {
      measures: ['sales.count'],
      filters: [
        {
          member: 'sales.category',
          operator,
          values: [value],
        },
      ],
    }
  );

  // Dremio's ILIKE is a function: `NOT` cannot sit inside the argument list and the escape
  // character is the third argument, without which the `\` BaseFilter.escapeWildcardChars binds
  // would stay a literal backslash
  it('should call ILIKE as a function with an escape character', () => compiler.compile().then(() => {
    const [sql, params] = likeQuery('contains', 'demo').buildSqlAndParams();

    expect(sql).toContain('ILIKE("sales".category, CONCAT(\'%\', ?, \'%\'), \'\\\')');
    expect(params).toEqual(['demo']);
  }));

  it('should negate the whole ILIKE call', () => compiler.compile().then(() => {
    const [sql] = likeQuery('notContains', 'demo').buildSqlAndParams();

    expect(sql).toContain('NOT ILIKE("sales".category, CONCAT(\'%\', ?, \'%\'), \'\\\')');
  }));

  it('should escape LIKE wildcards in filter parameters', () => compiler.compile().then(() => {
    expect(likeQuery('contains', 'a_b%').buildSqlAndParams()[1]).toEqual(['a\\_b\\%']);
    expect(likeQuery('startsWith', '100%').buildSqlAndParams()[1]).toEqual(['100\\%']);
    expect(likeQuery('endsWith', 'c:\\users').buildSqlAndParams()[1]).toEqual(['c:\\\\users']);
  }));

  it('should pass the escape character to ILIKE for the native planner too', () => compiler.compile().then(() => {
    const templates = likeQuery('contains', 'demo').sqlTemplates();

    expect(templates.filters.like_escape_char).toEqual('\\');
    expect(templates.tesseract.ilike).toEqual(
      '{% if negated %}NOT {% endif %}ILIKE({{ expr }}, {{ pattern }}, \'\\\')'
    );
  }));
});
