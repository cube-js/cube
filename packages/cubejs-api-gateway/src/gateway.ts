/* eslint-disable no-restricted-syntax */
import * as stream from 'stream';
import { assertNever } from 'assert-never';
import jwt, { Algorithm as JWTAlgorithm } from 'jsonwebtoken';
import R from 'ramda';
import bodyParser from 'body-parser';
import { graphqlHTTP } from 'express-graphql';
import structuredClone from '@ungap/structured-clone';
import {
  getEnv,
  getRealType,
  parseUtcIntoLocalDate,
  QueryAlias,
} from '@cubejs-backend/shared';
import {
  ResultArrayWrapper,
  ResultMultiWrapper,
  ResultWrapper,
} from '@cubejs-backend/native';
import type {
  Application as ExpressApplication,
  ErrorRequestHandler,
  NextFunction,
  RequestHandler,
  Request as ExpressRequest,
  Response as ExpressResponse,
} from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';

import {
  QueryType,
  ApiScopes,
} from './types/strings';
import {
  QueryType as QueryTypeEnum, ResultType
} from './types/enums';
import {
  BaseRequest,
  RequestContext,
  ExtendedRequestContext,
  Request,
  QueryRewriteFn,
  SecurityContextExtractorFn,
  ExtendContextFn,
  ResponseResultFn,
  QueryRequest,
  PreAggsJobsRequest,
  PreAggsSelector,
  PreAggJob,
  PreAggJobStatusItem,
  PreAggJobStatusResponse,
  SqlApiRequest, MetaResponseResultFn,
} from './types/request';
import {
  CheckAuthInternalOptions,
  JWTOptions,
  CheckAuthFn,
  ContextToApiScopesFn,
} from './types/auth';
import {
  Query,
  NormalizedQuery,
  MemberExpression,
  ParsedMemberExpression,
} from './types/query';
import {
  UserBackgroundContext,
  ApiGatewayOptions,
} from './types/gateway';
import {
  RequestLoggerMiddlewareFn,
  ContextRejectionMiddlewareFn,
  ContextAcceptorFn,
} from './interfaces';
import { getRequestIdFromRequest, requestParser } from './requestParser';
import { UserError } from './UserError';
import { CubejsHandlerError } from './CubejsHandlerError';
import { SubscriptionServer, WebSocketSendMessageFn } from './SubscriptionServer';
import { LocalSubscriptionStore } from './LocalSubscriptionStore';
import {
  getPivotQuery,
  getQueryGranularity,
  normalizeQuery,
  normalizeQueryCancelPreAggregations,
  normalizeQueryPreAggregationPreview,
  normalizeQueryPreAggregations,
  parseInputMemberExpression,
  preAggsJobsRequestSchema,
  remapToQueryAdapterFormat,
} from './query';
import { cachedHandler } from './cached-handler';
import { createJWKsFetcher } from './jwk';
import { SQLServer, SQLServerConstructorOptions } from './sql-server';
import { getJsonQueryFromGraphQLQuery, makeSchema } from './graphql';
import { ConfigItem, prepareAnnotation } from './helpers/prepareAnnotation';
import {
  transformCube,
  transformMeasure,
  transformDimension,
  transformSegment,
  transformJoins,
  transformPreAggregations,
} from './helpers/transformMetaExtended';

type HandleErrorOptions = {
    e: any,
    res: ResponseResultFn,
    context?: any,
    query?: any,
    requestStarted?: Date
};

function userAsyncHandler(handler: (req: Request & { context: ExtendedRequestContext }, res: ExpressResponse) => Promise<void>) {
  return (req: ExpressRequest, res: ExpressResponse, next: NextFunction) => {
    handler(req as any, res).catch(next);
  };
}

function systemAsyncHandler(handler: (req: Request & { context: ExtendedRequestContext }, res: ExpressResponse) => Promise<void>) {
  return (req: ExpressRequest, res: ExpressResponse, next: NextFunction) => {
    handler(req as any, res).catch(next);
  };
}

// Prepared CheckAuthFn, default or from config: always async
type PreparedCheckAuthFn = (ctx: any, authorization?: string) => Promise<{
  securityContext: any;
}>;

class ApiGateway {
  protected readonly refreshScheduler: any;

  protected readonly scheduledRefreshContexts: ApiGatewayOptions['scheduledRefreshContexts'];

  protected readonly scheduledRefreshTimeZones: ApiGatewayOptions['scheduledRefreshTimeZones'];

  protected readonly basePath: string;

  protected readonly queryRewrite: QueryRewriteFn;

  protected readonly subscriptionStore: any;

  protected readonly enforceSecurityChecks: boolean;

  protected readonly standalone: boolean;

  protected readonly extendContext?: ExtendContextFn;

  protected readonly dataSourceStorage: any;

  public readonly checkAuthFn: PreparedCheckAuthFn;

  public readonly checkAuthSystemFn: PreparedCheckAuthFn;

  public readonly contextToApiScopesFn: ContextToApiScopesFn;

  public readonly contextToApiScopesDefFn: ContextToApiScopesFn =
    async () => ['graphql', 'meta', 'data', 'sql'];

  protected readonly requestLoggerMiddleware: RequestLoggerMiddlewareFn;

  protected readonly securityContextExtractor: SecurityContextExtractorFn;

  protected readonly contextRejectionMiddleware: ContextRejectionMiddlewareFn;

  protected readonly wsContextAcceptor: ContextAcceptorFn;

  protected readonly releaseListeners: (() => any)[] = [];

  protected readonly playgroundAuthSecret?: string;

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected readonly event: (name: string, props?: object) => void;

  protected readonly sqlServer: SQLServer;

  public constructor(
    protected readonly apiSecret: string,
    protected readonly compilerApi: (ctx: RequestContext) => Promise<any>,
    protected readonly adapterApi: (ctx: RequestContext) => Promise<any>,
    protected readonly logger: any,
    protected readonly options: ApiGatewayOptions,
  ) {
    this.dataSourceStorage = options.dataSourceStorage;
    this.refreshScheduler = options.refreshScheduler;
    this.scheduledRefreshContexts = options.scheduledRefreshContexts;
    this.scheduledRefreshTimeZones = options.scheduledRefreshTimeZones;
    this.standalone = options.standalone;
    this.basePath = options.basePath;
    this.playgroundAuthSecret = options.playgroundAuthSecret;

    this.queryRewrite = options.queryRewrite || (async (query) => query);
    this.subscriptionStore = options.subscriptionStore || new LocalSubscriptionStore();
    this.enforceSecurityChecks = options.enforceSecurityChecks || (process.env.NODE_ENV === 'production');
    this.extendContext = options.extendContext;

    this.checkAuthFn = this.createCheckAuthFn(options);
    this.checkAuthSystemFn = this.createCheckAuthSystemFn();
    this.contextToApiScopesFn = this.createContextToApiScopesFn(options);
    this.securityContextExtractor = this.createSecurityContextExtractor(options.jwt);
    this.requestLoggerMiddleware = options.requestLoggerMiddleware || this.requestLogger;
    this.contextRejectionMiddleware = options.contextRejectionMiddleware || (async (req, res, next) => next());
    this.wsContextAcceptor = options.wsContextAcceptor || (() => ({ accepted: true }));
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    this.event = options.event || function dummyEvent() {};
    this.sqlServer = this.createSQLServerInstance({
      gatewayPort: options.gatewayPort,
    });
  }

  public getSQLServer(): SQLServer {
    return this.sqlServer;
  }

  protected createSQLServerInstance(options: SQLServerConstructorOptions): SQLServer {
    return new SQLServer(this, {
      gatewayPort: options.gatewayPort,
    });
  }

  public initApp(app: ExpressApplication) {
    const userMiddlewares: RequestHandler[] = [
      this.checkAuth,
      this.requestContextMiddleware,
      this.contextRejectionMiddleware,
      this.logNetworkUsage,
      this.requestLoggerMiddleware
    ];

    /** **************************************************************
     * No scope                                              *
     *************************************************************** */

    // @todo Should we pass requestLoggerMiddleware?

    const guestMiddlewares = [];

    app.get('/readyz', guestMiddlewares, cachedHandler(this.readiness));
    app.get('/livez', guestMiddlewares, cachedHandler(this.liveness));

    /** **************************************************************
     * graphql scope                                                 *
     *************************************************************** */

    app.post(`${this.basePath}/v1/graphql-to-json`, userMiddlewares, async (req: any, res) => {
      const { query, variables } = req.body;
      const compilerApi = await this.getCompilerApi(req.context);

      const metaConfig = await compilerApi.metaConfig(req.context, {
        requestId: req.context.requestId,
      });

      let schema = compilerApi.getGraphQLSchema();
      if (!schema) {
        schema = makeSchema(metaConfig);
        compilerApi.setGraphQLSchema(schema);
      }

      try {
        const jsonQuery = getJsonQueryFromGraphQLQuery(query, metaConfig, variables);
        res.json({ jsonQuery });
      } catch (e: any) {
        const stack = getEnv('devMode') ? e.stack : undefined;
        this.logger('GraphQL to JSON error', {
          error: (stack || e).toString(),
        });
        res.json({ jsonQuery: null });
      }
    });

    app.use(
      `${this.basePath}/graphql`,
      userMiddlewares,
      userAsyncHandler(async (req, res) => {
        await this.assertApiScope(
          'graphql',
          req?.context?.securityContext
        );

        const compilerApi = await this.getCompilerApi(req.context);
        let schema = compilerApi.getGraphQLSchema();
        if (!schema) {
          let metaConfig = await compilerApi.metaConfig(req.context, {
            requestId: req.context.requestId,
          });
          metaConfig = this.filterVisibleItemsInMeta(req.context, metaConfig);
          schema = makeSchema(metaConfig);
          compilerApi.setGraphQLSchema(schema);
        }
        return graphqlHTTP({
          schema,
          context: {
            req,
            apiGateway: this
          },
          graphiql: getEnv('nodeEnv') !== 'production'
            ? { headerEditorEnabled: true }
            : false,
        })(req, res);
      })
    );

    /** **************************************************************
     * data scope                                                    *
     *************************************************************** */

    app.get(`${this.basePath}/v1/load`, userMiddlewares, userAsyncHandler(async (req: any, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType,
      });
    }));

    const jsonParser = bodyParser.json({ limit: '1mb' });
    app.post(`${this.basePath}/v1/load`, jsonParser, userMiddlewares, userAsyncHandler(async (req, res) => {
      await this.load({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.body.queryType
      });
    }));

    app.get(`${this.basePath}/v1/subscribe`, userMiddlewares, userAsyncHandler(async (req: any, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType
      });
    }));

    app.get(`${this.basePath}/v1/sql`, userMiddlewares, userAsyncHandler(async (req: any, res) => {
      // TODO parse req.query with zod/joi/...

      if (req.query.format === 'sql') {
        await this.sql4sql({
          query: req.query.query,
          disablePostProcessing: req.query.disable_post_processing === 'true',
          context: req.context,
          res: this.resToResultFn(res)
        });
        return;
      }

      await this.sql({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.post(`${this.basePath}/v1/sql`, jsonParser, userMiddlewares, userAsyncHandler(async (req, res) => {
      // TODO parse req.body with zod/joi/...

      if (req.body.format === 'sql') {
        await this.sql4sql({
          query: req.body.query,
          disablePostProcessing: req.body.disable_post_processing,
          context: req.context,
          res: this.resToResultFn(res)
        });
        return;
      }

      await this.sql({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/dry-run`, userMiddlewares, userAsyncHandler(async (req: any, res) => {
      await this.dryRun({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.post(`${this.basePath}/v1/dry-run`, jsonParser, userMiddlewares, userAsyncHandler(async (req, res) => {
      await this.dryRun({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    /** **************************************************************
     * meta scope                                                    *
     *************************************************************** */

    app.get(
      `${this.basePath}/v1/meta`,
      userMiddlewares,
      userAsyncHandler(async (req, res) => {
        if ('extended' in req.query) {
          await this.metaExtended({
            context: req.context,
            res: this.resToResultFn(res),
          });
        } else {
          await this.meta({
            context: req.context,
            res: this.resToResultFn(res),
          });
        }
      })
    );

    app.post(
      `${this.basePath}/v1/cubesql`,
      userMiddlewares,
      userAsyncHandler(async (req, res) => {
        const { query } = req.body;

        const requestStarted = new Date();

        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Transfer-Encoding', 'chunked');

        try {
          await this.assertApiScope('data', req.context?.securityContext);

          await this.sqlServer.execSql(req.body.query, res, req.context?.securityContext);
        } catch (e: any) {
          this.handleError({
            e,
            query: {
              sql: query,
            },
            context: req.context,
            res: this.resToResultFn(res),
            requestStarted
          });
        }
      })
    );

    // Used by Rollup Designer
    app.post(
      `${this.basePath}/v1/pre-aggregations/can-use`,
      userMiddlewares,
      userAsyncHandler(async (req, res) => {
        await this.assertApiScope(
          'meta',
          req?.context?.securityContext
        );

        const { transformedQuery, references } = req.body;
        const compilerApi = await this.getCompilerApi(req.context as RequestContext);
        const canUsePreAggregationForTransformedQuery = compilerApi.canUsePreAggregationForTransformedQuery(
          transformedQuery,
          references,
        );

        res.json({ canUsePreAggregationForTransformedQuery });
      })
    );

    /** **************************************************************
     * jobs scope                                                    *
     *************************************************************** */

    app.post(
      `${this.basePath}/v1/pre-aggregations/jobs`,
      userMiddlewares,
      userAsyncHandler(this.preAggregationsJobs.bind(this)),
    );

    /** **************************************************************
     * Private API (no scopes)                                       *
     *************************************************************** */

    if (this.playgroundAuthSecret) {
      const systemMiddlewares: RequestHandler[] = [
        this.checkAuthSystemMiddleware,
        this.requestContextMiddleware,
        this.contextRejectionMiddleware,
        this.requestLoggerMiddleware
      ];

      app.get('/cubejs-system/v1/context', systemMiddlewares, this.createSystemContextHandler(this.basePath));

      app.get('/cubejs-system/v1/pre-aggregations', systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.getPreAggregations({
          cacheOnly: !!req.query.cacheOnly,
          metaOnly: !!req.query.metaOnly,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.get('/cubejs-system/v1/pre-aggregations/security-contexts', systemMiddlewares, systemAsyncHandler(async (req, res) => {
        const contexts = this.scheduledRefreshContexts ? await this.scheduledRefreshContexts() : [];
        this.resToResultFn(res)({
          securityContexts: contexts
            .map(ctx => ctx && (ctx.securityContext || ctx.authInfo))
            .filter(ctx => ctx)
        });
      }));

      app.get('/cubejs-system/v1/pre-aggregations/timezones', systemMiddlewares, systemAsyncHandler(async (req, res) => {
        this.resToResultFn(res)({
          timezones: this.scheduledRefreshTimeZones ? this.scheduledRefreshTimeZones(req.context) : []
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/partitions', jsonParser, systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.getPreAggregationPartitions({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/preview', jsonParser, systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.getPreAggregationPreview({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/build', jsonParser, systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.buildPreAggregations({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/queue', jsonParser, systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.getPreAggregationsInQueue({
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/cancel', jsonParser, systemMiddlewares, systemAsyncHandler(async (req, res) => {
        await this.cancelPreAggregationsFromQueue({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));
    }

    if (getEnv('nativeApiGateway')) {
      this.enableNativeApiGateway(app);
    }

    app.use(this.handleErrorMiddleware);
  }

  protected enableNativeApiGateway(app: ExpressApplication) {
    const proxyMiddleware = createProxyMiddleware<Request, Response>({
      target: `http://127.0.0.1:${this.sqlServer.getNativeGatewayPort()}/v2`,
      changeOrigin: true,
    });

    app.use(
      `${this.basePath}/v2`,
      proxyMiddleware as any
    );
  }

  public initSubscriptionServer(sendMessage: WebSocketSendMessageFn) {
    return new SubscriptionServer(this, sendMessage, this.subscriptionStore, this.wsContextAcceptor);
  }

  protected duration(requestStarted) {
    return requestStarted && (new Date().getTime() - requestStarted.getTime());
  }

  private filterVisibleItemsInMeta(context: RequestContext, cubes: any[]) {
    const isDevMode = getEnv('devMode');
    function visibilityFilter(item) {
      return isDevMode || context.signedWithPlaygroundAuthSecret || item.isVisible;
    }

    return cubes
      .map((cube) => ({
        config: {
          ...cube.config,
          measures: cube.config.measures?.filter(visibilityFilter),
          dimensions: cube.config.dimensions?.filter(visibilityFilter),
          segments: cube.config.segments?.filter(visibilityFilter),
        },
      })).filter(cube => cube.config.measures?.length || cube.config.dimensions?.length || cube.config.segments?.length);
  }

  public async meta({ context, res, includeCompilerId, onlyCompilerId }: {
    context: RequestContext,
    res: MetaResponseResultFn,
    includeCompilerId?: boolean,
    onlyCompilerId?: boolean
  }) {
    const requestStarted = new Date();

    try {
      await this.assertApiScope('meta', context.securityContext);
      const compilerApi = await this.getCompilerApi(context);
      const metaConfig = await compilerApi.metaConfig(context, {
        requestId: context.requestId,
        includeCompilerId: includeCompilerId || onlyCompilerId
      });
      if (onlyCompilerId) {
        const response: { cubes: any[], compilerId?: string } = {
          cubes: [],
          compilerId: metaConfig.compilerId
        };
        res(response);
        return;
      }
      const cubesConfig = includeCompilerId ? metaConfig.cubes : metaConfig;
      const cubes = this.filterVisibleItemsInMeta(context, cubesConfig).map(cube => cube.config);
      const response: { cubes: any[], compilerId?: string } = { cubes };
      if (includeCompilerId) {
        response.compilerId = metaConfig.compilerId;
      }
      res(response);
    } catch (e: any) {
      this.handleError({
        e,
        context,
        // @ts-ignore
        res,
        requestStarted,
      });
    }
  }

  public async metaExtended({ context, res }: { context: ExtendedRequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    try {
      await this.assertApiScope('meta', context.securityContext);
      const compilerApi = await this.getCompilerApi(context);
      const metaConfigExtended = await compilerApi.metaConfigExtended(context, {
        requestId: context.requestId,
      });
      const { metaConfig, cubeDefinitions } = metaConfigExtended;

      const cubes = this.filterVisibleItemsInMeta(context, metaConfig)
        .map((meta) => meta.config)
        .map((cube) => ({
          ...transformCube(cube, cubeDefinitions),
          measures: cube.measures?.map((measure) => ({
            ...transformMeasure(measure, cubeDefinitions),
          })),
          dimensions: cube.dimensions?.map((dimension) => ({
            ...transformDimension(dimension, cubeDefinitions),
          })),
          segments: cube.segments?.map((segment) => ({
            ...transformSegment(segment, cubeDefinitions),
          })),
          joins: transformJoins(cubeDefinitions[cube.name]?.joins),
          preAggregations: transformPreAggregations(cubeDefinitions[cube.name]?.preAggregations),
        }));
      res({ cubes });
    } catch (e: any) {
      this.handleError({
        e,
        context,
        res,
        requestStarted,
      });
    }
  }

  public async getPreAggregations({ cacheOnly, metaOnly, context, res }: { cacheOnly?: boolean, metaOnly?: boolean, context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();
    try {
      const compilerApi = await this.getCompilerApi(context);
      const preAggregations = await compilerApi.preAggregations();

      const refreshTimezones = this.scheduledRefreshTimeZones ? await this.scheduledRefreshTimeZones(context) : [];
      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          normalizeQueryPreAggregations(
            {
              timezones: refreshTimezones.length > 0 ? refreshTimezones : undefined,
              preAggregations: preAggregations.map(p => ({
                id: p.id,
                cacheOnly,
                metaOnly
              }))
            },
          )
        );

      res({ preAggregations: preAggregationPartitions.map(({ preAggregation }) => preAggregation) });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  public async getPreAggregationPartitions(
    { query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      const refreshTimezones = this.scheduledRefreshTimeZones ? await this.scheduledRefreshTimeZones(context) : [];
      query = normalizeQueryPreAggregations(
        this.parseQueryParam(query),
        { timezones: refreshTimezones.length > 0 ? refreshTimezones : undefined }
      );
      const orchestratorApi = await this.getAdapterApi(context);
      const compilerApi = await this.getCompilerApi(context);

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          query
        );

      const preAggregationPartitionsWithoutError = preAggregationPartitions.filter(p => !p?.errors?.length);

      const versionEntriesResult = preAggregationPartitions &&
        await orchestratorApi.getPreAggregationVersionEntries(
          context,
          preAggregationPartitionsWithoutError,
          compilerApi.preAggregationsSchema
        );

      const checkExpand = (path: string | RegExp) => !query.expand ||
        (path instanceof RegExp
          ? query.expand.some((p: string) => path.test(p))
          : query.expand.includes(path));

      const mergePartitionsAndVersionEntries = () => ({ errors, preAggregation, partitions, invalidateKeyQueries, timezones }) => ({
        errors,
        invalidateKeyQueries,
        preAggregation,
        timezones,
        partitions: partitions.map(partition => ({
          ...(checkExpand('partitions.details') ? partition : {}),
          ...(checkExpand('partitions.meta') ? {
            dataSource: partition.dataSource,
            preAggregationId: partition.preAggregationId,
            tableName: partition.tableName,
            type: partition.type,
          } : {}),
          ...(checkExpand('partitions.versions') ? {
            versionEntries: versionEntriesResult?.versionEntriesByTableName[partition?.tableName] || [],
            structureVersion: versionEntriesResult?.structureVersionsByTableName[partition?.tableName] || [],
          } : {}),
        })),
      });

      res({
        preAggregationPartitions: preAggregationPartitions.map(mergePartitionsAndVersionEntries())
      });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  public async getPreAggregationPreview(
    { query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      query = normalizeQueryPreAggregationPreview(this.parseQueryParam(query));
      const { preAggregationId, versionEntry, timezone } = query;

      const orchestratorApi = await this.getAdapterApi(context);

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          {
            timezones: [timezone],
            preAggregations: [{ id: preAggregationId }]
          }
        );
      const { partitions } = (preAggregationPartitions?.[0] || {});
      const preAggregationPartition = partitions?.find(p => p?.tableName === versionEntry.table_name);

      res({
        preview: preAggregationPartition && await orchestratorApi.getPreAggregationPreview(
          context,
          preAggregationPartition
        )
      });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  public async buildPreAggregations(
    { query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      query = normalizeQueryPreAggregations(this.parseQueryParam(query));
      const result = await this.refreshScheduler()
        .buildPreAggregations(
          context,
          query
        );

      res({ result });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  /**
   * Entry point for the `/cubejs-system/v1/pre-aggregations/jobs` endpoint.
   * Post object example:
   * ```
   * {
   *   "action": "post",
   *   "selector": {
   *     "contexts": [
   *       {"securityContext": {"tenant": "t1"}},
   *       {"securityContext": {"tenant": "t2"}}
   *     ],
   *     "timezones": ["UTC"],
   *     "dataSources": ["default"],
   *     "cubes": ["Events"],
   *     "preAggregations": ["Events.TemporaryData"]
   *   }
   * }
   * // or
   * {
   *   "action": "get",
   *   "tokens": [
   *     "ec1232ea3356f04f8be313fecf3deb4d",
   *     "48b75d5c466fa579c936dc451f498f69",
   *     "76509837091396dc204abb1016c48e75",
   *     "52264769f81f6ff62062a93d6f6fbdb2"
   *   ]
   * }
   * // or
   * {
   *   "action": "get",
   *   "resType": "object",
   *   "tokens": [
   *     "ec1232ea3356f04f8be313fecf3deb4d",
   *     "48b75d5c466fa579c936dc451f498f69",
   *     "76509837091396dc204abb1016c48e75",
   *     "52264769f81f6ff62062a93d6f6fbdb2"
   *   ]
   * }
   * ```
   */
  private async preAggregationsJobs(req: Request, res: ExpressResponse) {
    const response = this.resToResultFn(res);
    const requestStarted = new Date();
    const context = <RequestContext>req.context;
    const query = <PreAggsJobsRequest>req.body;
    let result;
    try {
      await this.assertApiScope('jobs', req?.context?.securityContext);

      if (!query || Object.keys(query).length === 0) {
        throw new UserError('No job description provided');
      }

      const { error } = preAggsJobsRequestSchema.validate(query);
      if (error) {
        throw new UserError(`Invalid Job query format: ${error.message || error.toString()}`);
      }

      switch (query.action) {
        case 'post':
          result = await this.preAggregationsJobsPOST(
            context,
            <PreAggsSelector>query.selector
          );
          if (result.length === 0) {
            throw new UserError(
              'A user\'s selector doesn\'t match any of the ' +
              'pre-aggregations defined in the data model.'
            );
          }
          break;
        case 'get':
          result = await this.preAggregationsJobsGET(
            context,
            <string[]>query.tokens,
            query.resType,
          );
          break;
        default:
          throw new Error(`The '${query.action}' action type doesn't supported.`);
      }
      this.event(`pre_aggregations_jobs_${query.action}`, {
        source: req.header('source') || 'unknown',
      });
      response(result, { status: 200 });
    } catch (e: any) {
      this.handleError({ e, context, query, res: response, requestStarted });
    }
  }

  /**
   * Post pre-aggregations build jobs entry point.
   */
  private async preAggregationsJobsPOST(
    context: RequestContext,
    selector: PreAggsSelector,
  ): Promise<string[]> {
    let jobs: string[] = [];

    // There might be a few contexts but dateRange if present is still the same
    // so let's normalize it only once.
    // It's expected that selector.dateRange is provided in local time (without timezone)
    // At the same time it is ok to get timestamps with `Z` (in UTC).
    if (selector.dateRange) {
      const start = parseUtcIntoLocalDate([{ val: selector.dateRange[0] }], 'UTC');
      const end = parseUtcIntoLocalDate([{ val: selector.dateRange[1] }], 'UTC');
      if (!start || !end) {
        throw new UserError(`Cannot parse selector date range ${selector.dateRange}`);
      }
      selector.dateRange = [start, end];
    }

    const promise = Promise.all(
      selector.contexts.map(async (config) => {
        const ctx = <RequestContext>{
          ...context,
          ...config,
        };
        const _jobs = await this.postPreAggregationsBuildJobs(
          ctx,
          selector,
        );
        return _jobs;
      })
    );
    const resolve = await promise;
    resolve.forEach((_jobs) => {
      jobs = jobs.concat(_jobs);
    });

    return jobs;
  }

  /**
   * Add pre-aggregations build job. Returns added jobs ids.
   */
  private async postPreAggregationsBuildJobs(
    context: RequestContext,
    selector: PreAggsSelector
  ): Promise<string[]> {
    const compiler = await this.getCompilerApi(context);
    const { timezones, dateRange } = selector;
    const preaggs = await compiler.preAggregations({
      dataSources: selector.dataSources,
      cubes: selector.cubes,
      preAggregationIds: selector.preAggregations,
    });
    if (preaggs.length === 0) {
      return [];
    } else {
      const jobs: string[] = await this
        .refreshScheduler()
        .postBuildJobs(
          context,
          {
            metadata: undefined,
            timezones,
            dateRange,
            preAggregations: preaggs.map(p => ({
              id: p.id,
              cacheOnly: false,
              partitions: undefined, // string[]
            })),
            forceBuildPreAggregations: false,
            throwErrors: false,
          }
        );
      return jobs;
    }
  }

  /**
   * Get pre-aggregations build jobs entry point.
   */
  private async preAggregationsJobsGET(
    context: RequestContext,
    tokens: string[],
    resType = 'array',
  ): Promise<PreAggJobStatusResponse> {
    const jobs: { job: PreAggJob | null, token: string }[] = await this
      .refreshScheduler()
      .getCachedBuildJobs(context, tokens);

    const metaCache: Map<string, any> = new Map();

    const response: PreAggJobStatusItem[] = await Promise.all(
      jobs.map(async ({ job, token }) => {
        if (!job) {
          return {
            token,
            status: 'not_found',
          };
        }

        const ctx = { ...context, ...job.context };
        const orchestrator = await this.getAdapterApi(ctx);
        const compiler = await this.getCompilerApi(ctx);
        const selector: PreAggsSelector = {
          cubes: [job.preagg.split('.')[0]],
          preAggregations: [job.preagg],
          contexts: [job.context],
          timezones: [job.timezone],
          dataSources: [job.dataSource],
        };
        if (
          job.status.indexOf('done') === 0 ||
          job.status.indexOf('failure') === 0
        ) {
          // returning from the cache
          return {
            token,
            table: job.target,
            status: job.status,
            selector,
          };
        } else {
          // checking the queue
          const status = await this.getPreAggJobQueueStatus(
            orchestrator,
            job,
          );
          if (status) {
            // returning queued status
            return {
              token,
              table: job.target,
              status,
              selector,
            };
          } else {
            const metaCacheKey = JSON.stringify(ctx);
            if (!metaCache.has(metaCacheKey)) {
              metaCache.set(metaCacheKey, await compiler.metaConfigExtended(context, ctx));
            }

            // checking and fetching result status
            const s = await this.getPreAggJobResultStatus(
              ctx.requestId,
              orchestrator,
              compiler,
              metaCache.get(metaCacheKey),
              job,
              token,
            );

            return {
              token,
              table: job.target,
              status: s,
              selector,
            };
          }
        }
      })
    );

    if (resType === 'object') {
      return response.reduce(
        (prev, current) => ({
          [current.token]: { ...current, token: undefined },
          ...prev
        }),
        {}
      );
    }

    return response;
  }

  /**
   * Returns PreAggJob status if it still in queue, false otherwise.
   */
  private async getPreAggJobQueueStatus(
    orchestrator: any,
    job: PreAggJob,
  ): Promise<false | string> {
    let inQueue = false;
    let status: string = 'n/a';
    const queuedList = await orchestrator.getPreAggregationQueueStates();
    queuedList.forEach((item) => {
      if (
        item.queryHandler &&
        item.queryHandler === 'query' &&
        item.query &&
        item.query.requestId === job.request &&
        item.query.newVersionEntry.table_name === job.table &&
        item.query.newVersionEntry.structure_version === job.structure &&
        item.query.newVersionEntry.content_version === job.content &&
        item.query.newVersionEntry.last_updated_at === job.updated
      ) {
        inQueue = true;
        switch (<string>item.status[0]) {
          case 'toProcess':
            status = 'scheduled';
            break;
          case 'active':
            status = 'processing';
            break;
          default:
            status = <string>item.status[0];
            break;
        }
      }
    });
    return inQueue ? status : false;
  }

  /**
   * Returns PreAggJob execution status.
   */
  private async getPreAggJobResultStatus(
    requestId: string,
    orchestrator: any,
    compiler: any,
    metadata: any,
    job: PreAggJob,
    token: string,
  ): Promise<string> {
    const preaggs = await compiler.preAggregations();
    const preagg = preaggs.find(pa => pa.id === job.preagg);
    if (preagg) {
      const cube = metadata.cubeDefinitions[preagg.cube];
      const [, status]: [boolean, string] =
        await orchestrator.isPartitionExist(
          requestId,
          preagg.preAggregation.external,
          cube.dataSource,
          compiler.preAggregationsSchema,
          job.target,
          job.key,
          token,
        );

      return status;
    }

    return 'pre_agg_not_found';
  }

  public async getPreAggregationsInQueue(
    { context, res }: { context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      const orchestratorApi = await this.getAdapterApi(context);
      res({
        result: await orchestratorApi.getPreAggregationQueueStates()
      });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  public async cancelPreAggregationsFromQueue(
    { query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      const { queryKeys, dataSource } = normalizeQueryCancelPreAggregations(this.parseQueryParam(query));
      const orchestratorApi = await this.getAdapterApi(context);
      res({
        result: await orchestratorApi.cancelPreAggregationQueriesFromQueue(queryKeys, dataSource)
      });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  /**
   * Convert incoming query parameter (JSON fetched from the HTTP) to
   * an array of query type and array of normalized queries.
   */
  protected async getNormalizedQueries(
    inputQuery: Record<string, any> | Record<string, any>[],
    context: RequestContext,
    persistent = false,
    memberExpressions: boolean = false,
  ): Promise<[QueryType, NormalizedQuery[], NormalizedQuery[]]> {
    let query = this.parseQueryParam(inputQuery);

    let queryType: QueryType = QueryTypeEnum.REGULAR_QUERY;
    if (!Array.isArray(query)) {
      query = this.compareDateRangeTransformer(query);
      if (Array.isArray(query)) {
        queryType = QueryTypeEnum.COMPARE_DATE_RANGE_QUERY;
      }
    } else {
      queryType = QueryTypeEnum.BLENDING_QUERY;
    }

    const queries: Query[] = Array.isArray(query) ? query : [query];

    this.log({
      type: 'Query Rewrite',
      query
    }, context);

    const startTime = new Date().getTime();
    const compilerApi = await this.getCompilerApi(context);

    const queryNormalizationResult: Array<{
      normalizedQuery: NormalizedQuery,
      hasExpressionsInQuery: boolean
    }> = queries.map((currentQuery) => {
      const hasExpressionsInQuery = this.hasExpressionsInQuery(currentQuery);

      if (hasExpressionsInQuery) {
        if (!memberExpressions) {
          throw new Error('Expressions are not allowed in this context');
        }

        currentQuery = this.parseMemberExpressionsInQuery(currentQuery);
      }

      return {
        normalizedQuery: (normalizeQuery(currentQuery, persistent)),
        hasExpressionsInQuery
      };
    });

    let normalizedQueries: NormalizedQuery[] = await Promise.all(
      queryNormalizationResult.map(
        async ({ normalizedQuery, hasExpressionsInQuery }) => {
          let evaluatedQuery: Query | NormalizedQuery = normalizedQuery;

          if (hasExpressionsInQuery) {
            // We need to parse/eval all member expressions early as applyRowLevelSecurity
            // needs to access the full SQL query in order to evaluate rules
            evaluatedQuery = this.evalMemberExpressionsInQuery(normalizedQuery);
          }

          // First apply cube/view level security policies
          const { query: queryWithRlsFilters, denied } = await compilerApi.applyRowLevelSecurity(
            normalizedQuery,
            evaluatedQuery,
            context
          );
          // Then apply user-supplied queryRewrite
          let rewrittenQuery = !denied ? await this.queryRewrite(
            queryWithRlsFilters,
            context
          ) : queryWithRlsFilters;

          // applyRowLevelSecurity may add new filters which may contain raw member expressions
          // if that's the case, we should run an extra pass of parsing here to make sure
          // nothing breaks down the road
          if (hasExpressionsInQuery || this.hasExpressionsInQuery(rewrittenQuery)) {
            rewrittenQuery = this.parseMemberExpressionsInQuery(rewrittenQuery);
            rewrittenQuery = this.evalMemberExpressionsInQuery(rewrittenQuery);
          }

          return normalizeQuery(rewrittenQuery, persistent);
        }
      )
    );

    this.log({
      type: 'Query Rewrite completed',
      normalizedQueries,
      duration: new Date().getTime() - startTime,
      query
    }, context);

    normalizedQueries = normalizedQueries.map(q => remapToQueryAdapterFormat(q));

    if (normalizedQueries.find((currentQuery) => !currentQuery)) {
      throw new Error('queryTransformer returned null query. Please check your queryTransformer implementation');
    }

    if (queryType === QueryTypeEnum.BLENDING_QUERY) {
      const queryGranularity = getQueryGranularity(normalizedQueries);

      if (queryGranularity.length > 1) {
        throw new UserError('Data blending query granularities must match');
      }
      if (queryGranularity.length === 0) {
        throw new UserError('Data blending query without granularity is not supported');
      }
    }

    return [queryType, normalizedQueries, queryNormalizationResult.map((it) => remapToQueryAdapterFormat(it.normalizedQuery))];
  }

  protected async sql4sql({
    query,
    disablePostProcessing,
    context,
    res,
  }: {query: string, disablePostProcessing: boolean} & BaseRequest) {
    try {
      await this.assertApiScope('sql', context.securityContext);

      const result = await this.sqlServer.sql4sql(query, disablePostProcessing, context.securityContext);
      res({ sql: result });
    } catch (e: any) {
      this.handleError({
        e,
        context,
        query,
        res,
      });
    }
  }

  public async sql({
    query,
    context,
    res,
    memberToAlias,
    exportAnnotatedSql,
    memberExpressions,
    expressionParams,
    disableExternalPreAggregations,
    disableLimitEnforcing,
  }: QueryRequest) {
    const requestStarted = new Date();

    try {
      await this.assertApiScope('sql', context.securityContext);

      const [queryType, normalizedQueries] =
        await this.getNormalizedQueries(query, context, disableLimitEnforcing, memberExpressions);

      const sqlQueries = await Promise.all<any>(
        normalizedQueries.map(async (normalizedQuery) => (await this.getCompilerApi(context)).getSql(
          this.coerceForSqlQuery({ ...normalizedQuery, memberToAlias, expressionParams, disableExternalPreAggregations }, context),
          {
            includeDebugInfo: getEnv('devMode') || context.signedWithPlaygroundAuthSecret,
            exportAnnotatedSql,
          }
        ))
      );

      const toQuery = (sqlQuery) => ({
        ...sqlQuery,
        order: R.fromPairs(sqlQuery.order.map(({ id: key, desc }) => [key, desc ? 'desc' : 'asc']))
      });

      res(queryType === QueryTypeEnum.REGULAR_QUERY ?
        { sql: toQuery(sqlQueries[0]) } :
        sqlQueries.map((sqlQuery) => ({ sql: toQuery(sqlQuery) })));
    } catch (e: any) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  private hasExpressionsInQuery(query: Query): boolean {
    const arraysToCheck = [
      query.measures,
      query.dimensions,
      query.segments,
      (query.subqueryJoins ?? []).map(join => join.on),
    ];

    return arraysToCheck.some(array => array?.some(item => typeof item === 'string' && item.startsWith('{')));
  }

  private parseMemberExpressionsInQuery(query: Query): Query {
    return {
      ...query,
      measures: (query.measures || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
      dimensions: (query.dimensions || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
      segments: (query.segments || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
      subqueryJoins: (query.subqueryJoins ?? []).map(join => (typeof join.on === 'string' ? {
        ...join,
        on: this.parseMemberExpression(join.on),
      } : join)),
    };
  }

  private parseMemberExpression(memberExpression: string): string | ParsedMemberExpression {
    if (memberExpression.startsWith('{')) {
      const obj = parseInputMemberExpression(JSON.parse(memberExpression));
      let expression: ParsedMemberExpression['expression'];
      switch (obj.expr.type) {
        case 'SqlFunction':
          expression = [
            ...obj.expr.cubeParams,
            `return \`${obj.expr.sql}\``,
          ];
          break;
        case 'PatchMeasure':
          expression = {
            type: 'PatchMeasure',
            sourceMeasure: obj.expr.sourceMeasure,
            replaceAggregationType: obj.expr.replaceAggregationType,
            addFilters: obj.expr.addFilters.map(filter => [
              ...filter.cubeParams,
              `return \`${filter.sql}\``,
            ]),
          };
          break;
        default:
          assertNever(obj.expr);
      }

      const groupingSet = obj.groupingSet ? {
        groupType: obj.groupingSet.groupType,
        id: obj.groupingSet.id,
        subId: obj.groupingSet.subId ? obj.groupingSet.subId : undefined
      } : undefined;

      return {
        cubeName: obj.cubeName,
        name: obj.alias,
        expressionName: obj.alias,
        expression,
        definition: memberExpression,
        groupingSet,
      };
    } else {
      return memberExpression;
    }
  }

  private evalMemberExpressionsInQuery(query: Query): Query {
    return {
      ...query,
      measures: (query.measures || []).map(m => (typeof m !== 'string' ? this.evalMemberExpression(m as ParsedMemberExpression) : m)),
      dimensions: (query.dimensions || []).map(m => (typeof m !== 'string' ? this.evalMemberExpression(m as ParsedMemberExpression) : m)),
      segments: (query.segments || []).map(m => (typeof m !== 'string' ? this.evalMemberExpression(m as ParsedMemberExpression) : m)),
      subqueryJoins: (query.subqueryJoins ?? []).map(join => (typeof join.on !== 'string' ? {
        ...join,
        on: this.evalMemberExpression(join.on as ParsedMemberExpression)
      } : join)),
    };
  }

  private evalMemberExpression(memberExpression: MemberExpression | ParsedMemberExpression): MemberExpression | ParsedMemberExpression {
    if (typeof memberExpression.expression === 'function') {
      return memberExpression;
    }

    if (Array.isArray(memberExpression.expression)) {
      return {
        ...memberExpression,
        expression: Function.constructor.apply(null, memberExpression.expression),
      };
    }

    if (memberExpression.expression.type === 'PatchMeasure') {
      return {
        ...memberExpression,
        expression: {
          ...memberExpression.expression,
          addFilters: memberExpression.expression.addFilters.map(filter => ({
            sql: Function.constructor.apply(null, filter),
          })),
        }
      };
    }

    throw new Error(`Unexpected member expression to evaluate: ${memberExpression}`);
  }

  public async sqlGenerators({ context, res }: { context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    try {
      const compilerApi = await this.getCompilerApi(context);
      const query = {
        requestId: context.requestId,
      };
      const memberToDataSource: Record<string, string> = await compilerApi.memberToDataSource(query);

      const dataSources = new Set(Object.values(memberToDataSource));
      const dataSourceToSqlGenerator = (await Promise.all(
        [...dataSources].map(async dataSource => ({ [dataSource]: (await compilerApi.getSqlGenerator(query, dataSource)).sqlGenerator }))
      )).reduce((a, b) => ({ ...a, ...b }), {});

      res({ memberToDataSource, dataSourceToSqlGenerator });
    } catch (e: any) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  protected createSecurityContextExtractor(options?: JWTOptions): SecurityContextExtractorFn {
    if (options?.claimsNamespace) {
      return (ctx: Readonly<RequestContext>) => {
        if (typeof ctx.securityContext === 'object' && ctx.securityContext !== null) {
          if (<string>options.claimsNamespace in ctx.securityContext) {
            return ctx.securityContext[<string>options.claimsNamespace];
          }
        }

        return {};
      };
    }

    let checkAuthDeprecationShown: boolean = false;

    return (ctx: Readonly<RequestContext>) => {
      let securityContext: any = {};

      if (typeof ctx.securityContext === 'object' && ctx.securityContext !== null) {
        if (ctx.securityContext.u) {
          if (!checkAuthDeprecationShown) {
            this.logger('JWT U Property Deprecation', {
              warning: (
                'Storing security context in the u property within the payload is now deprecated, please migrate: ' +
                'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#authinfo'
              )
            });

            checkAuthDeprecationShown = true;
          }

          securityContext = {
            ...ctx.securityContext,
            ...ctx.securityContext.u,
          };

          delete securityContext.u;
        } else {
          securityContext = ctx.securityContext;
        }
      }

      return securityContext;
    };
  }

  protected coerceForSqlQuery(query, context: Readonly<RequestContext>) {
    return {
      ...query,
      timeDimensions: query.timeDimensions || [],
      contextSymbols: {
        securityContext: this.securityContextExtractor(context),
      },
      requestId: context.requestId
    };
  }

  protected async dryRun({ query, context, res }: QueryRequest) {
    const requestStarted = new Date();

    try {
      await this.assertApiScope('data', context.securityContext);

      const [queryType, _, normalizedQueries] = await this.getNormalizedQueries(query, context, undefined, undefined);

      const sqlQueries = await Promise.all<any>(
        normalizedQueries.map(async (normalizedQuery) => (await this.getCompilerApi(context)).getSql(
          this.coerceForSqlQuery(normalizedQuery, context),
          {
            includeDebugInfo: getEnv('devMode') || context.signedWithPlaygroundAuthSecret
          }
        ))
      );

      res({
        queryType,
        normalizedQueries,
        queryOrder: sqlQueries.map((sqlQuery) => R.fromPairs(
          sqlQuery.order.map(({ id: member, desc }) => [member, desc ? 'desc' : 'asc'])
        )),
        transformedQueries: sqlQueries.map((sqlQuery) => sqlQuery.canUseTransformedQuery),
        pivotQuery: getPivotQuery(queryType, normalizedQueries)
      });
    } catch (e: any) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  /**
   * Returns an array of sqlQuery objects for specified normalized
   * queries.
   * @internal
   */
  private async getSqlQueriesInternal(
    context: RequestContext,
    normalizedQueries: (NormalizedQuery)[],
  ): Promise<Array<any>> {
    const sqlQueries = await Promise.all(
      normalizedQueries.map(
        async (normalizedQuery, index) => {
          const loadRequestSQLStarted = new Date();
          const sqlQuery = await (await this.getCompilerApi(context))
            .getSql(
              this.coerceForSqlQuery(normalizedQuery, context)
            );

          this.log({
            type: 'Load Request SQL',
            duration: this.duration(loadRequestSQLStarted),
            query: normalizedQueries[index],
            sqlQuery
          }, context);

          return sqlQuery;
        }
      )
    );
    return sqlQueries;
  }

  /**
   * Execute query and return adapter's result.
   * @internal
   */
  private async getSqlResponseInternal(
    context: RequestContext,
    normalizedQuery: NormalizedQuery,
    sqlQuery: any,
  ): Promise<ResultWrapper> {
    const queries = [{
      ...sqlQuery,
      query: sqlQuery.sql[0],
      values: sqlQuery.sql[1],
      continueWait: true,
      renewQuery: normalizedQuery.renewQuery,
      requestId: context.requestId,
      context,
      persistent: false,
    }];
    if (normalizedQuery.total) {
      const normalizedTotal = structuredClone(normalizedQuery);
      normalizedTotal.totalQuery = true;

      delete normalizedTotal.order;

      normalizedTotal.limit = null;
      normalizedTotal.rowLimit = null;
      normalizedTotal.offset = null;

      const [totalQuery] = await this.getSqlQueriesInternal(
        context,
        [normalizedTotal],
      );
      queries.push({
        ...totalQuery,
        query: totalQuery.sql[0],
        values: totalQuery.sql[1],
        continueWait: true,
        renewQuery: normalizedTotal.renewQuery,
        requestId: context.requestId,
        context
      });
    }
    const [response, total] = await Promise.all(
      queries.map(async (query) => {
        const res = await (await this.getAdapterApi(context))
          .executeQuery(query);
        return res;
      })
    );
    response.total = normalizedQuery.total
      ? Number(total.data[0][QueryAlias.TOTAL_COUNT])
      : undefined;

    return this.wrapAdapterQueryResultIfNeeded(response);
  }

  /**
   * Wraps the adapter's response in unified ResultWrapper if it comes from
   * a common driver (not a Cubestore's one, cause Cubestore Driver internally creates ResultWrapper)
   * @param res Adapter's response
   * @private
   */
  private wrapAdapterQueryResultIfNeeded(res: any): ResultWrapper {
    res.data = new ResultWrapper(res.data);

    return res;
  }

  /**
   * Prepare adapter's result and other transform parameters for a final
   * result object.
   * @internal
   */
  private prepareResultTransformData(
    context: RequestContext,
    queryType: QueryType,
    normalizedQuery: NormalizedQuery,
    sqlQuery: any,
    annotation: {
      measures: {
        [index: string]: unknown;
      };
      dimensions: {
        [index: string]: unknown;
      };
      segments: {
        [index: string]: unknown;
      };
      timeDimensions: {
        [index: string]: unknown;
      };
    },
    response: any,
    responseType?: ResultType,
  ): ResultWrapper {
    const resultWrapper = response.data;

    const transformDataParams = {
      aliasToMemberNameMap: sqlQuery.aliasNameToMember,
      annotation: {
        ...annotation.measures,
        ...annotation.dimensions,
        ...annotation.timeDimensions
      } as { [member: string]: ConfigItem },
      query: normalizedQuery,
      queryType,
      resType: responseType,
    };

    const resObj = {
      query: normalizedQuery,
      lastRefreshTime: response.lastRefreshTime?.toISOString(),
      ...(
        getEnv('devMode') ||
          context.signedWithPlaygroundAuthSecret
          ? {
            refreshKeyValues: response.refreshKeyValues,
            usedPreAggregations: response.usedPreAggregations,
            transformedQuery: sqlQuery.canUseTransformedQuery,
            requestId: context.requestId,
          }
          : null
      ),
      annotation,
      dataSource: response.dataSource,
      dbType: response.dbType,
      extDbType: response.extDbType,
      external: response.external,
      slowQuery: Boolean(response.slowQuery),
      total: normalizedQuery.total ? response.total : null,
    };

    resultWrapper.setTransformData(transformDataParams);
    resultWrapper.setRootResultObject(resObj);

    return resultWrapper;
  }

  /**
   * Returns stream object which will be used to stream results from
   * the data source if applicable, returns `null` otherwise.
   */
  public async stream(context: RequestContext, query: Query): Promise<null | {
    originalQuery: Query;
    normalizedQuery: NormalizedQuery;
    streamingQuery: unknown;
    stream: stream.Writable;
  }> {
    const requestStarted = new Date();
    try {
      this.log({ type: 'Load Request', query, streaming: true }, context);
      const [, normalizedQueries] = await this.getNormalizedQueries(query, context, true);
      const sqlQuery = (await this.getSqlQueriesInternal(context, normalizedQueries))[0];
      const q = {
        ...sqlQuery,
        query: sqlQuery.sql[0],
        values: sqlQuery.sql[1],
        continueWait: true,
        renewQuery: false,
        requestId: context.requestId,
        context,
        persistent: true,
        forceNoCache: true,
      };
      const _stream = {
        originalQuery: query,
        normalizedQuery: normalizedQueries[0],
        streamingQuery: q,
        stream: await (await this.getAdapterApi(context)).streamQuery(q),
      };
      return _stream;
    } catch (err: any) {
      const e = err.message === 'Continue wait' ? { error: 'Continue wait' } : err;
      this.handleError({
        e,
        context,
        query,
        res: (errorObj) => {
          throw errorObj;
        },
        requestStarted
      });
      return null;
    }
  }

  /**
   * Data queries APIs (`/load`, `/subscribe`) entry point. Used by
   * `CubejsApi#load` and `CubejsApi#subscribe` methods to fetch the
   * data.
   */
  public async load(request: QueryRequest) {
    let query: Query | Query[] | undefined;
    const {
      context,
      res,
      apiType = 'rest',
      ...props
    } = request;
    const requestStarted = new Date();

    try {
      await this.assertApiScope('data', context.securityContext);

      query = this.parseQueryParam(request.query);
      let resType: ResultType = ResultType.DEFAULT;

      if (!Array.isArray(query) && query.responseFormat) {
        resType = query.responseFormat;
      }

      this.log({
        type: 'Load Request',
        apiType,
        query
      }, context);

      const [queryType, normalizedQueries] =
        await this.getNormalizedQueries(query, context);

      if (
        queryType !== QueryTypeEnum.REGULAR_QUERY &&
        props.queryType == null
      ) {
        throw new UserError(
          `'${queryType
          }' query type is not supported by the client.` +
          'Please update the client.'
        );
      }

      let metaConfigResult = await (await this
        .getCompilerApi(context)).metaConfig(request.context, {
        requestId: context.requestId
      });

      metaConfigResult = this.filterVisibleItemsInMeta(context, metaConfigResult);

      const sqlQueries = await this.getSqlQueriesInternal(context, normalizedQueries);

      let slowQuery = false;

      const results = await Promise.all(
        normalizedQueries.map(async (normalizedQuery, index) => {
          slowQuery = slowQuery ||
            Boolean(sqlQueries[index].slowQuery);

          const response = await this.getSqlResponseInternal(
            context,
            normalizedQuery,
            sqlQueries[index],
          );

          const annotation = prepareAnnotation(
            metaConfigResult, normalizedQuery
          );

          return this.prepareResultTransformData(
            context,
            queryType,
            normalizedQuery,
            sqlQueries[index],
            annotation,
            response,
            resType,
          );
        })
      );

      this.log(
        {
          type: 'Load Request Success',
          query,
          duration: this.duration(requestStarted),
          apiType,
          isPlayground: Boolean(
            context.signedWithPlaygroundAuthSecret
          ),
          queries: results.length,
          queriesWithPreAggregations:
            results.filter(
              (r: any) => Object.keys(r.getRootResultObject()[0].usedPreAggregations || {}).length
            ).length,
          // Have to omit because data could be processed natively
          // so it is not known at this point
          // queriesWithData:
          //   results.filter((r: any) => r.data?.length).length,
          dbType: results.map(r => r.getRootResultObject()[0].dbType),
        },
        context,
      );

      if (props.queryType === 'multi') {
        // We prepare the final json result on native side
        const resultMulti = new ResultMultiWrapper(results, { queryType, slowQuery });
        await res(resultMulti);
      } else {
        // We prepare the full final json result on native side
        await res(results[0]);
      }
    } catch (e: any) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  public async sqlApiLoad(request: SqlApiRequest) {
    let query: Query | Query[] | null = null;
    const {
      context,
      res,
    } = request;
    const requestStarted = new Date();

    try {
      await this.assertApiScope('data', context.securityContext);

      query = this.parseQueryParam(request.query);
      let resType: ResultType = ResultType.DEFAULT;

      if (!Array.isArray(query) && query.responseFormat) {
        resType = query.responseFormat;
      }

      const [queryType, normalizedQueries] =
        await this.getNormalizedQueries(query, context, request.streaming, request.memberExpressions);

      const compilerApi = await this.getCompilerApi(context);
      let metaConfigResult = await compilerApi.metaConfig(request.context, {
        requestId: context.requestId
      });

      metaConfigResult = this.filterVisibleItemsInMeta(context, metaConfigResult);

      const sqlQueries = await this
        .getSqlQueriesInternal(
          context,
          normalizedQueries.map(q => ({ ...q, disableExternalPreAggregations: request.sqlQuery }))
        );

      let results;

      let slowQuery = false;

      const streamResponse = async (sqlQuery) => {
        const q = {
          ...sqlQuery,
          query: sqlQuery.query || sqlQuery.sql[0],
          values: sqlQuery.values || sqlQuery.sql[1],
          continueWait: true,
          renewQuery: false,
          requestId: context.requestId,
          context,
          persistent: true,
          forceNoCache: true,
        };

        const adapterApi = await this.getAdapterApi(context);

        return {
          stream: await adapterApi.streamQuery(q),
        };
      };

      if (request.sqlQuery) {
        const finalQuery = {
          query: request.sqlQuery[0],
          values: request.sqlQuery[1],
          continueWait: true,
          renewQuery: normalizedQueries[0].renewQuery,
          requestId: context.requestId,
          context,
          ...sqlQueries[0],
          // TODO Can we just pass through data? Ensure hidden members can't be queried
          aliasNameToMember: null,
        };

        if (request.streaming) {
          results = [await streamResponse(finalQuery)];
        } else {
          const adapterApi = await this.getAdapterApi(context);
          const response = await adapterApi.executeQuery(finalQuery);

          const annotation = prepareAnnotation(
            metaConfigResult, normalizedQueries[0]
          );

          // TODO Can we just pass through data? Ensure hidden members can't be queried
          results = [{
            data: response.data,
            annotation
          }];
        }

        await res(request.streaming ? results[0] : { results });
      } else {
        results = await Promise.all(
          normalizedQueries.map(async (normalizedQuery, index) => {
            slowQuery = slowQuery ||
              Boolean(sqlQueries[index].slowQuery);

            const annotation = prepareAnnotation(
              metaConfigResult, normalizedQuery
            );

            if (request.streaming) {
              return streamResponse(sqlQueries[index]);
            }

            const response = await this.getSqlResponseInternal(
              context,
              normalizedQuery,
              sqlQueries[index],
            );

            return this.prepareResultTransformData(
              context,
              queryType,
              normalizedQuery,
              sqlQueries[index],
              annotation,
              response,
              resType,
            );
          })
        );

        if (request.streaming) {
          await res(results[0]);
        } else {
          // We prepare the final json result on native side
          const resultArray = new ResultArrayWrapper(results);
          await res(resultArray);
        }
      }
    } catch (e: any) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  public async subscribe({
    query, context, res, subscribe, subscriptionState, queryType, apiType
  }) {
    const requestStarted = new Date();
    try {
      this.log({
        type: 'Subscribe',
        query
      }, context);

      let result: any = null;
      let error: any = null;

      if (!subscribe) {
        await this.load({ query, context, res, queryType, apiType });
        return;
      }

      // TODO subscribe to refreshKeys instead of constantly firing load
      await this.load({
        query,
        context,
        res: (message, opts) => {
          if (!Array.isArray(message) && 'error' in message && message.error) {
            error = { message, opts };
          } else {
            result = { message, opts };
          }
        },
        queryType,
        apiType,
      });
      const state = await subscriptionState();
      if (result && (!state || JSON.stringify(state.result) !== JSON.stringify(result))) {
        res(result.message, result.opts);
      } else if (error) {
        res(error.message, error.opts);
      }
      await subscribe({ error, result });
    } catch (e: any) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  protected resToResultFn(res: ExpressResponse) {
    return async (message, { status }: { status?: number } = {}) => {
      if (status) {
        res.status(status);
      }

      if (message.isWrapper) {
        res.set('Content-Type', 'application/json');
        res.send(Buffer.from(await message.getFinalResult()));
      } else {
        res.json(message);
      }
    };
  }

  protected parseQueryParam(query): Query | Query[] {
    if (!query || query === 'undefined') {
      throw new UserError('Query param is required');
    }

    if (typeof query === 'string') {
      try {
        return JSON.parse(query) as Query | Query[];
      } catch (e: any) {
        throw new UserError(`Unable to decode query param as JSON, error: ${e.message}`);
      }
    }

    return query as Query | Query[];
  }

  protected async getCompilerApi(context: RequestContext) {
    return this.compilerApi(context);
  }

  protected async getAdapterApi(context: RequestContext) {
    return this.adapterApi(context);
  }

  public async contextByReq(req: Request, securityContext, requestId: string): Promise<ExtendedRequestContext> {
    const extensions = typeof this.extendContext === 'function' ? await this.extendContext(req) : {};

    return {
      securityContext,
      // Deprecated, but let's allow it for now.
      authInfo: securityContext,
      signedWithPlaygroundAuthSecret: Boolean(req.signedWithPlaygroundAuthSecret),
      requestId,
      ...extensions,
    };
  }

  protected handleErrorMiddleware: ErrorRequestHandler = async (e, req: Request, res, next) => {
    this.handleError({
      e,
      context: (<any>req).context,
      res: this.resToResultFn(res),
      requestStarted: req.requestStarted || new Date(),
    });

    next(e);
  };

  public handleError({
    e, context, query, res, requestStarted
  }: HandleErrorOptions) {
    const requestId = getEnv('devMode') || context?.signedWithPlaygroundAuthSecret ? context?.requestId : undefined;
    const stack = getEnv('devMode') ? e.stack : undefined;

    const plainError = e.plainMessages;

    if (e instanceof CubejsHandlerError) {
      this.log({
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      }, context);
      res({ error: e.message, stack, requestId, plainError }, { status: e.status });
    } else if (e.error === 'Continue wait') {
      this.log({
        type: 'Continue wait',
        query,
        error: e.message,
        duration: this.duration(requestStarted),
      }, context);
      res({ error: e.message || e.error.message || e.error.toString(), requestId }, { status: 200 });
    } else if (e.error) {
      this.log({
        type: 'Orchestrator error',
        query,
        error: e.error,
        duration: this.duration(requestStarted),
      }, context);
      res({ error: e.message || e.error.message || e.error.toString(), requestId }, { status: 400 });
    } else if (e.type === 'UserError') {
      this.log({
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      }, context);
      res(
        {
          type: e.type,
          error: e.message,
          plainError,
          stack,
          requestId
        },
        { status: 400 }
      );
    } else {
      this.log({
        type: 'Internal Server Error',
        query,
        error: stack || e.toString(),
        duration: this.duration(requestStarted)
      }, context);
      res({ error: e.toString(), stack, requestId, plainError, }, { status: 500 });
    }
  }

  protected wrapCheckAuth(fn: CheckAuthFn): PreparedCheckAuthFn {
    // We dont need to span all logs with deprecation message
    let warningShowed = false;
    // securityContext should be object
    let showWarningAboutNotObject = false;

    return async (req, auth) => {
      const result = await fn(req, auth);

      // checkAuth from config can return new security context, e.g from Python config
      if (result?.security_context) {
        req.securityContext = result?.security_context;
      }

      // We renamed authInfo to securityContext, but users can continue to use both ways
      if (req.securityContext && !req.authInfo) {
        req.authInfo = req.securityContext;
      } else if (req.authInfo) {
        if (!warningShowed) {
          this.logger('AuthInfo Deprecation', {
            warning: (
              'authInfo was renamed to securityContext, please migrate: ' +
              'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware'
            )
          });

          warningShowed = true;
        }

        req.securityContext = req.authInfo;
      }

      if ((typeof req.securityContext !== 'object' || req.securityContext === null) && !showWarningAboutNotObject) {
        this.logger('Security Context Should Be Object', {
          warning: (
            `Value of securityContext (previously authInfo) expected to be object, actual: ${getRealType(req.securityContext)}`
          )
        });

        showWarningAboutNotObject = true;
      }

      return {
        securityContext: req.securityContext
      };
    };
  }

  protected createDefaultCheckAuth(options?: JWTOptions, internalOptions?: CheckAuthInternalOptions): PreparedCheckAuthFn {
    type VerifyTokenFn = (auth: string, secret: string) => Promise<object | string> | object | string;

    const verifyToken = (auth, secret) => jwt.verify(auth, secret, {
      algorithms: <JWTAlgorithm[] | undefined>options?.algorithms,
      issuer: options?.issuer,
      audience: options?.audience,
      subject: options?.subject,
    });

    let checkAuthFn: VerifyTokenFn = verifyToken;

    if (options?.jwkUrl) {
      const jwks = createJWKsFetcher(options, {
        onBackgroundException: (e) => {
          this.logger('JWKs Background Fetching Error', {
            error: e.message,
          });
        },
      });

      this.releaseListeners.push(jwks.release);

      // Precache JWKs response to speedup first auth
      if (options.jwkUrl && typeof options.jwkUrl === 'string') {
        jwks.fetchOnly(options.jwkUrl).catch((e) => this.logger('JWKs Prefetching Error', {
          error: e.message,
        }));
      }

      checkAuthFn = async (auth) => {
        const decoded = <Record<string, any> | null>jwt.decode(auth, { complete: true });
        if (!decoded) {
          throw new CubejsHandlerError(
            403,
            'Forbidden',
            'Unable to decode JWT key'
          );
        }

        if (!decoded.header || !decoded.header.kid) {
          throw new CubejsHandlerError(
            403,
            'Forbidden',
            'JWT without kid inside headers'
          );
        }

        const jwk = await jwks.getJWKbyKid(
          typeof options.jwkUrl === 'function' ? await options.jwkUrl(decoded) : <string>options.jwkUrl,
          decoded.header.kid
        );
        if (!jwk) {
          throw new CubejsHandlerError(
            403,
            'Forbidden',
            `Unable to verify, JWK with kid: "${decoded.header.kid}" not found`
          );
        }

        return verifyToken(auth, jwk);
      };
    }

    const secret = options?.key || this.apiSecret;

    return async (req, auth) => {
      if (auth) {
        try {
          req.securityContext = await checkAuthFn(auth, secret);
          req.signedWithPlaygroundAuthSecret = Boolean(internalOptions?.isPlaygroundCheckAuth);
        } catch (e: any) {
          if (this.enforceSecurityChecks) {
            throw new CubejsHandlerError(403, 'Forbidden', 'Invalid token', e);
          }
        }
      } else if (this.enforceSecurityChecks) {
        // @todo Move it to 401 or 400
        throw new CubejsHandlerError(403, 'Forbidden', 'Authorization header isn\'t set');
      }

      return {
        securityContext: req.securityContext
      };
    };
  }

  protected createCheckAuthFn(options: ApiGatewayOptions): PreparedCheckAuthFn {
    const mainCheckAuthFn = options.checkAuth
      ? this.wrapCheckAuth(options.checkAuth)
      : this.createDefaultCheckAuth(options.jwt);

    if (this.playgroundAuthSecret) {
      const systemCheckAuthFn = this.createCheckAuthSystemFn();

      return async (ctx, authorization) => {
        // TODO: separate two auth workflows
        try {
          await mainCheckAuthFn(ctx, authorization);
        } catch (mainAuthError) {
          try {
            await systemCheckAuthFn(ctx, authorization);
          } catch (playgroundAuthError) {
            throw mainAuthError;
          }
        }

        return {
          securityContext: ctx.securityContext,
        };
      };
    }

    return (ctx, authorization) => mainCheckAuthFn(ctx, authorization);
  }

  protected createCheckAuthSystemFn(): PreparedCheckAuthFn {
    const systemCheckAuthFn = this.createDefaultCheckAuth(
      {
        key: this.playgroundAuthSecret,
        algorithms: ['HS256']
      },
      { isPlaygroundCheckAuth: true }
    );

    return async (ctx, authorization) => {
      await systemCheckAuthFn(ctx, authorization);

      return {
        securityContext: ctx.securityContext
      };
    };
  }

  protected createContextToApiScopesFn(
    options: ApiGatewayOptions,
  ): ContextToApiScopesFn {
    return options.contextToApiScopes
      ? async (securityContext?: any, defaultApiScopes?: ApiScopes[]) => {
        const scopes = options.contextToApiScopes &&
            await options.contextToApiScopes(
              securityContext,
              defaultApiScopes,
            );
        if (!scopes || !Array.isArray(scopes)) {
          throw new Error(
            'A user-defined contextToApiScopes function returns an inconsistent type.'
          );
        } else {
          scopes.forEach((p) => {
            if (['graphql', 'meta', 'data', 'sql', 'jobs'].indexOf(p) === -1) {
              throw new Error(
                `A user-defined contextToApiScopes function returns a wrong scope: ${p}`
              );
            }
          });
        }
        return scopes;
      }
      : async () => {
        const defaultApiScope = getEnv('defaultApiScope');
        if (defaultApiScope) {
          return defaultApiScope;
        } else {
          return this.contextToApiScopesDefFn();
        }
      };
  }

  protected async assertApiScope(
    scope: ApiScopes,
    securityContext?: any,
  ): Promise<void> {
    const scopes =
      await this.contextToApiScopesFn(
        securityContext || {},
        getEnv('defaultApiScope') || await this.contextToApiScopesDefFn(),
      );
    const permited = scopes.indexOf(scope) >= 0;
    if (!permited) {
      throw new CubejsHandlerError(
        403,
        'Forbidden',
        `API scope is missing: ${scope}`
      );
    }
  }

  protected extractAuthorizationHeaderWithSchema(req: Request) {
    const authHeader = req.headers?.['x-cube-authorization'] || req.headers?.authorization;

    if (typeof authHeader === 'string') {
      const parts = authHeader.split(' ', 2);
      if (parts.length === 1) {
        return parts[0];
      }

      return parts[1];
    }

    return undefined;
  }

  protected async checkAuthWrapper(checkAuthFn: PreparedCheckAuthFn, req: Request, res: ExpressResponse, next) {
    const token = this.extractAuthorizationHeaderWithSchema(req);

    try {
      await checkAuthFn(req, token);
      if (next) {
        next();
      }
    } catch (e: unknown) {
      if (e instanceof CubejsHandlerError) {
        const error = e.originalError || e;
        const stack = getEnv('devMode') ? error.stack : undefined;
        this.log({
          type: error.message,
          url: req.url,
          token,
          error: stack || error.toString()
        }, <any>req);

        res.status(e.status).json({ error: e.message });
      } else if (e instanceof Error) {
        const stack = getEnv('devMode') ? e.stack : undefined;
        this.log({
          type: 'Auth Error',
          token,
          error: stack || e.toString()
        }, <any>req);

        res.status(500).json({
          error: e.toString(),
          stack,
        });
      }
    }
  }

  protected checkAuth: RequestHandler = async (req, res, next) => {
    await this.checkAuthWrapper(this.checkAuthFn, req, res, next);
  };

  protected checkAuthSystemMiddleware: RequestHandler = async (req, res, next) => {
    await this.checkAuthWrapper(this.checkAuthSystemFn, req, res, next);
  };

  protected requestContextMiddleware: RequestHandler = async (req: Request, res: ExpressResponse, next: NextFunction) => {
    try {
      req.context = await this.contextByReq(req, req.securityContext, getRequestIdFromRequest(req));
      req.requestStarted = new Date();
      if (next) {
        next();
      }
    } catch (e: any) {
      if (next) {
        next(e);
      } else {
        throw e;
      }
    }
  };

  protected requestLogger: RequestHandler = async (req: Request, res: ExpressResponse, next: NextFunction) => {
    const details = requestParser(req, res);

    this.log({ type: 'REST API Request', ...details }, req.context);

    if (next) {
      next();
    }
  };

  protected logNetworkUsage: RequestHandler = async (req: Request, res: ExpressResponse, next: NextFunction) => {
    this.log({
      type: 'Incoming network usage',
      service: 'api-http',
      bytes: Buffer.byteLength(req.url + req.rawHeaders.join('\n')) + (Number(req.get('content-length')) || 0),
      path: req.path,
    }, req.context);
    res.on('finish', () => {
      this.log({
        type: 'Outgoing network usage',
        service: 'api-http',
        bytes: Number(res.get('content-length')) || 0,
        path: req.path,
      }, req.context);
    });
    if (next) {
      next();
    }
  };

  protected compareDateRangeTransformer(query) {
    let queryCompareDateRange;
    let compareDateRangeTDIndex;

    (query.timeDimensions || []).forEach((td, index) => {
      if (td.compareDateRange != null) {
        if (queryCompareDateRange != null) {
          throw new UserError('compareDateRange can only exist for one timeDimension');
        }

        queryCompareDateRange = td.compareDateRange;
        compareDateRangeTDIndex = index;
      }
    });

    if (queryCompareDateRange == null) {
      return query;
    }

    return queryCompareDateRange.map((dateRange) => ({
      ...R.clone(query),
      timeDimensions: query.timeDimensions.map((td, index) => {
        if (compareDateRangeTDIndex === index) {
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          const { compareDateRange, ...timeDimension } = td;
          return {
            ...timeDimension,
            dateRange
          };
        }

        return td;
      })
    }));
  }

  public log(event: { type: string, [key: string]: any }, context?: Partial<RequestContext>) {
    const { type, ...restParams } = event;

    this.logger(type, {
      ...restParams,
      ...(!context ? undefined : {
        securityContext: context.securityContext,
        requestId: context.requestId,
        ...(!context.appName ? undefined : { appName: context.appName }),
        ...(!context.protocol ? undefined : { protocol: context.protocol }),
        ...(!context.apiType ? undefined : { apiType: context.apiType }),
      })
    });
  }

  protected healthResponse(res: ExpressResponse, health: 'HEALTH' | 'DOWN') {
    res.status(health === 'HEALTH' ? 200 : 500).json({
      health,
    });
  }

  protected createSystemContextHandler = (basePath: string): RequestHandler => {
    const body: Readonly<Record<string, any>> = {
      basePath,
      dockerVersion: getEnv('dockerImageVersion') || null,
      serverCoreVersion: this.options.serverCoreVersion || null
    };

    return (req, res) => {
      res.status(200).json(body);
    };
  };

  private logProbeError(e: any, type: string): void {
    const stack = getEnv('devMode') ? (e as Error).stack : undefined;
    this.log({
      type,
      driverType: e.driverType,
      error: stack || (e as Error).toString(),
    });
  }

  protected readiness: RequestHandler = async (req, res) => {
    let health: 'HEALTH' | 'DOWN' = 'HEALTH';

    if (this.standalone) {
      try {
        const orchestratorApi = await this.adapterApi({} as any);

        // todo: test other data sources
        orchestratorApi.addDataSeenSource('default');
        await orchestratorApi.testConnection();
        await orchestratorApi.testOrchestratorConnections();
      } catch (e: any) {
        this.logProbeError(e, 'Internal Server Error on readiness probe');
        health = 'DOWN';
      }
    }

    return this.healthResponse(res, health);
  };

  protected liveness: RequestHandler = async (req, res) => {
    let health: 'HEALTH' | 'DOWN' = 'HEALTH';

    try {
      await this.dataSourceStorage.testConnections();
      // @todo Optimize this moment?
      await this.dataSourceStorage.testOrchestratorConnections();
    } catch (e: any) {
      this.logProbeError(e, 'Internal Server Error on liveness probe');
      health = 'DOWN';
    }

    return this.healthResponse(res, health);
  };

  public release() {
    for (const releaseListener of this.releaseListeners) {
      releaseListener();
    }
  }
}
export {
  UserBackgroundContext,
  ApiGatewayOptions,
  ApiGateway,
};
