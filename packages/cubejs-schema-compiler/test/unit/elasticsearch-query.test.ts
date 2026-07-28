/* eslint-disable no-restricted-syntax, quotes */
import { ElasticSearchQuery } from '../../src/adapter/ElasticSearchQuery';
import { AWSElasticSearchQuery } from '../../src/adapter/AWSElasticSearchQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('ElasticSearchQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`SELECT * FROM visitors\`,
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        },
        source: {
          sql: \`source\`,
          type: 'string'
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });
    `);

  const filterParams = async (QueryClass: any, operator: string, value: string) => {
    await compiler.compile();

    const query = new QueryClass({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      filters: [
        {
          member: 'visitors.source',
          operator,
          values: [value],
        },
      ],
      timezone: 'UTC',
    });

    return query.buildSqlAndParams()[1];
  };

  // `contains` is rendered as a full-text MATCH(), where `%`/`_` are not wildcards and a
  // backslash is ordinary data, so an escaped value only corrupts the search term
  it('leaves filter values alone, there is nothing to escape against MATCH()', async () => {
    expect(await filterParams(ElasticSearchQuery, 'contains', 'a_b%')).toEqual(['a_b%']);
    expect(await filterParams(ElasticSearchQuery, 'contains', 'c:\\users')).toEqual(['c:\\users']);
    expect(await filterParams(ElasticSearchQuery, 'startsWith', '100%')).toEqual(['100%']);
  });

  it('declares no escape character for the native planner either', async () => {
    await compiler.compile();

    const query = new ElasticSearchQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timezone: 'UTC',
    });

    expect(query.sqlTemplates().filters.like_escape_char).toBeUndefined();
  });

  // The Open Distro / OpenSearch dialect is LIKE-based and follows MySQL, where `\` is the
  // default escape character, so it keeps the base escaping
  it('keeps escaping for the Open Distro dialect', async () => {
    expect(await filterParams(AWSElasticSearchQuery, 'contains', 'a_b%')).toEqual(['a\\_b\\%']);
    expect(await filterParams(AWSElasticSearchQuery, 'startsWith', '100%')).toEqual(['100\\%']);
  });

  it('keeps the escape character for the Open Distro dialect on the native planner', async () => {
    await compiler.compile();

    const query = new AWSElasticSearchQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      timezone: 'UTC',
    });

    expect(query.sqlTemplates().filters.like_escape_char).toEqual('\\');
  });
});
