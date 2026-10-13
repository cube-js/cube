import { registerInterface, shutdownInterface, sql4sql, SqlInterfaceInstance } from '@cubejs-backend/native';
import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';

export type SourceQuery = [string, (string | null)[]];
export type CompilationRequest = {
  useNativeSqlPlanner: boolean;
  candidateQueries: string[];
};
export type CompilationResponse = { candidateQueries: SourceQuery[] } | { error: string };

const compileQueries = async (request: CompilationRequest): Promise<CompilationResponse> => {
  const compilers = prepareJsCompiler(`
    cube('Visitors', {
      sql_table: '##visitors',
      measures: { count: { type: 'count' } },
      dimensions: { id: { sql: 'id', type: 'number', primaryKey: true } }
    })
  `, { adapter: 'mssql' });
  await compilers.compiler.compile();
  const options = { measures: [], timeDimensions: [], filters: [], useNativeSqlPlanner: request.useNativeSqlPlanner };
  const memberExpression = (member: string) => {
    if (!member.startsWith('{')) {
      return member;
    }
    const definition = JSON.parse(member);
    if (definition.expr.type !== 'SqlFunction') {
      throw new Error('Unexpected test member expression');
    }
    return {
      cubeName: definition.cubeName,
      name: definition.alias,
      expressionName: definition.alias,
      expression: Function.constructor.apply(null, [...definition.expr.cubeParams, `return \`${definition.expr.sql}\``]),
      definition: member,
    };
  };
  const createInterface = async () => registerInterface({
    contextToApiScopes: async () => ['data', 'meta', 'sql'],
    checkAuth: async () => ({ securityContext: {} }),
    checkSqlAuth: async () => ({ password: null, superuser: true, securityContext: {} }),
    meta: async () => ({ compilerId: compilers.compilerId, cubes: compilers.metaTransformer.cubes.map(cube => cube.config || cube) }),
    sqlGenerators: async () => ({
      cubeNameToDataSource: { Visitors: 'default' },
      memberToDataSource: { 'Visitors.count': 'default', 'Visitors.id': 'default' },
      dataSourceToSqlGenerator: { default: new MssqlQuery(compilers, options) },
    }),
    sql: async ({ query, memberToAlias, expressionParams }) => {
      const normalized = {
        ...query,
        measures: (query.measures || []).map(memberExpression),
        dimensions: (query.dimensions || []).map(memberExpression),
        segments: (query.segments || []).map(memberExpression),
      };
      return { sql: { sql: new MssqlQuery(compilers, { ...options, ...normalized, memberToAlias, expressionParams }).buildSqlAndParams(true) } };
    },
    preAggregationMatch: async ({ queries }) => ({ matches: queries.map(() => false) }),
    stream: async () => { throw new Error('Compilation must not execute a Cube query'); },
    sqlApiLoad: async () => { throw new Error('Compilation must not execute source SQL'); },
    logLoadEvent: async () => undefined,
    canSwitchUserForSession: async () => true,
  });
  const compile = async (instance: SqlInterfaceInstance, query: string): Promise<SourceQuery> => {
    const response: unknown = await sql4sql(instance, query, true);
    if (!response || typeof response !== 'object' || !('status' in response) || response.status !== 'ok' ||
      !('query_type' in response) || response.query_type !== 'pushdown' || !('sql' in response) ||
      !Array.isArray(response.sql) || typeof response.sql[0] !== 'string' || !Array.isArray(response.sql[1])) {
      throw new Error(`SQL compilation did not return a complete source query: ${JSON.stringify(response)}`);
    }
    const parameters = response.sql[1].map((value: unknown) => {
      if (value !== null && typeof value !== 'string') {
        throw new Error('Unexpected SQL parameter');
      }
      return value;
    });
    return [response.sql[0], parameters];
  };
  const server = await createInterface();

  try {
    const candidateQueries: SourceQuery[] = [];

    for (const query of request.candidateQueries) {
      candidateQueries.push(await compile(server, query));
    }
    return { candidateQueries };
  } finally {
    await shutdownInterface(server, 'fast');
  }
};

process.once('message', async (request: CompilationRequest) => {
  let response: CompilationResponse;
  let exitCode = 0;

  try {
    response = await compileQueries(request);
  } catch (error) {
    response = { error: error instanceof Error ? error.message : String(error) };
    exitCode = 1;
  }
  // Native callbacks retain referenced channels after shutdown. Keep that
  // lifetime inside this worker so the integration runner exits normally.
  if (process.send) {
    process.send(response, () => process.exit(exitCode));
  } else {
    process.exit(1);
  }
});
