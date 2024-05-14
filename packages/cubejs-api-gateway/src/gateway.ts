/* eslint-disable no-restricted-syntax */
import * as stream from 'stream';
import jwt, { Algorithm as JWTAlgorithm } from 'jsonwebtoken';
import R from 'ramda';
import bodyParser from 'body-parser';
import { graphqlHTTP } from 'express-graphql';
import structuredClone from '@ungap/structured-clone';
import {
  getEnv,
  getRealType,
  QueryAlias,
} from '@cubejs-backend/shared';
import type {
  Application as ExpressApplication,
  ErrorRequestHandler,
  NextFunction,
  RequestHandler,
  Request as ExpressRequest,
  Response as ExpressResponse,
} from 'express';
import {
  QueryType,
  ApiScopes,
} from './types/strings';
import {
  QueryType as QueryTypeEnum, ResultType
} from './types/enums';
import {
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
  NormalizedQuery, MemberExpression,
} from './types/query';
import {
  UserBackgroundContext,
  ApiGatewayOptions,
} from './types/gateway';
import {
  CheckAuthMiddlewareFn,
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
  normalizeQueryPreAggregations, remapToQueryAdapterFormat,
} from './query';
import { cachedHandler } from './cached-handler';
import { createJWKsFetcher } from './jwk';
import { SQLServer } from './sql-server';
import { getJsonQueryFromGraphQLQuery, makeSchema } from './graphql';
import { ConfigItem, prepareAnnotation } from './helpers/prepareAnnotation';
import transformData from './helpers/transformData';
import {
  transformCube,
  transformMeasure,
  transformDimension,
  transformSegment,
  transformJoins,
  transformPreAggregations,
} from './helpers/transformMetaExtended';

const memberExpressionRegex = /^([a-zA-Z0-9_]+).([a-zA-Z0-9_]+):\(([a-zA-Z0-9_,]+)\):(.*)$/;

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

  public readonly checkAuthFn: CheckAuthFn;

  public readonly checkAuthSystemFn: CheckAuthFn;

  protected readonly contextToApiScopesFn: ContextToApiScopesFn;

  protected readonly contextToApiScopesDefFn: ContextToApiScopesFn =
    async () => ['graphql', 'meta', 'data'];

  protected readonly checkAuthMiddleware: CheckAuthMiddlewareFn;

  protected readonly requestLoggerMiddleware: RequestLoggerMiddlewareFn;

  protected readonly securityContextExtractor: SecurityContextExtractorFn;
  
  protected readonly contextRejectionMiddleware: ContextRejectionMiddlewareFn;

  protected readonly wsContextAcceptor: ContextAcceptorFn;

  protected readonly releaseListeners: (() => any)[] = [];

  protected readonly playgroundAuthSecret?: string;

  protected readonly event: (name: string, props?: object) => void;

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
    this.checkAuthMiddleware = options.checkAuthMiddleware
      ? this.wrapCheckAuthMiddleware(options.checkAuthMiddleware)
      : this.checkAuth;
    this.securityContextExtractor = this.createSecurityContextExtractor(options.jwt);
    this.requestLoggerMiddleware = options.requestLoggerMiddleware || this.requestLogger;
    this.contextRejectionMiddleware = options.contextRejectionMiddleware || (async (req, res, next) => next());
    this.wsContextAcceptor = options.wsContextAcceptor || (() => ({ accepted: true }));
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    this.event = options.event || function () {};
  }

  public initApp(app: ExpressApplication) {
    const userMiddlewares: RequestHandler[] = [
      this.checkAuthMiddleware,
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

      const metaConfig = await compilerApi.metaConfig({
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
        this.logger('GraphQL to JSON error', {
          error: (e.stack || e).toString(),
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
          let metaConfig = await compilerApi.metaConfig({
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
      await this.sql({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.post(`${this.basePath}/v1/sql`, jsonParser, userMiddlewares, userAsyncHandler(async (req, res) => {
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

    app.get(
      `${this.basePath}/v1/run-scheduled-refresh`,
      userMiddlewares,
      userAsyncHandler(async (req, res) => {
        await this.runScheduledRefresh({
          queryingOptions: req.query.queryingOptions,
          context: req.context,
          res: this.resToResultFn(res)
        });
      })
    );

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
          timezones: this.scheduledRefreshTimeZones || []
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

    app.use(this.handleErrorMiddleware);
  }

  public initSQLServer() {
    return new SQLServer(this);
  }

  public initSubscriptionServer(sendMessage: WebSocketSendMessageFn) {
    return new SubscriptionServer(this, sendMessage, this.subscriptionStore, this.wsContextAcceptor);
  }

  protected duration(requestStarted) {
    return requestStarted && (new Date().getTime() - requestStarted.getTime());
  }

  public async runScheduledRefresh({ context, res, queryingOptions }: {
    context: RequestContext,
    res: ResponseResultFn,
    queryingOptions: any
  }) {
    const requestStarted = new Date();
    try {
      await this.assertApiScope('jobs', context.securityContext);
      const refreshScheduler = this.refreshScheduler();
      res(await refreshScheduler.runScheduledRefresh(context, {
        ...this.parseQueryParam(queryingOptions || {}),
        throwErrors: true
      }));
    } catch (e) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
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
      const metaConfig = await compilerApi.metaConfig({
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
    } catch (e) {
      this.handleError({
        e,
        context,
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
      const metaConfigExtended = await compilerApi.metaConfigExtended({
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
    } catch (e) {
      this.handleError({
        e,
        context,
        res,
        requestStarted,
      });
    }
  }

  public async getPreAggregations({ cacheOnly, context, res }: { cacheOnly?: boolean, context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();
    try {
      const compilerApi = await this.getCompilerApi(context);
      const preAggregations = await compilerApi.preAggregations();

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          normalizeQueryPreAggregations(
            {
              timezones: this.scheduledRefreshTimeZones,
              preAggregations: preAggregations.map(p => ({
                id: p.id,
                cacheOnly,
              }))
            },
          )
        );

      res({ preAggregations: preAggregationPartitions.map(({ preAggregation }) => preAggregation) });
    } catch (e) {
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
      query = normalizeQueryPreAggregations(
        this.parseQueryParam(query),
        { timezones: this.scheduledRefreshTimeZones }
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
    } catch (e) {
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
      const { partitions } = (preAggregationPartitions && preAggregationPartitions[0] || {});
      const preAggregationPartition = partitions && partitions.find(p => p?.tableName === versionEntry.table_name);

      res({
        preview: preAggregationPartition && await orchestratorApi.getPreAggregationPreview(
          context,
          preAggregationPartition
        )
      });
    } catch (e) {
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
    } catch (e) {
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
   * TODO (buntarb): selector object validator.
   */
  private async preAggregationsJobs(req: Request, res: ExpressResponse) {
    const response = this.resToResultFn(res);
    const started = new Date();
    const context = <RequestContext>req.context;
    const query = <PreAggsJobsRequest>req.body;
    let result;
    try {
      await this.assertApiScope('jobs', req?.context?.securityContext);
      switch (query.action) {
        case 'post':
          if (
            !(<PreAggsSelector>query.selector).timezones ||
            (<PreAggsSelector>query.selector).timezones.length === 0
          ) {
            throw new UserError(
              'A user\'s selector must contain at least one time zone.'
            );
          }
          if (
            !(<PreAggsSelector>query.selector).contexts ||
            (
              <{securityContext: any}[]>(
                <PreAggsSelector>query.selector
              ).contexts
            ).length === 0
          ) {
            throw new UserError(
              'A user\'s selector must contain at least one context element.'
            );
          } else {
            let e = false;
            (<{securityContext: any}[]>(
              <PreAggsSelector>query.selector
            ).contexts).forEach((c) => {
              if (!c.securityContext) e = true;
            });
            if (e) {
              throw new UserError(
                'Every context element must contain the ' +
                '\'securityContext\' property.'
              );
            }
          }
          result = await this.preAggregationsJobsPOST(
            context,
            <PreAggsSelector>query.selector
          );
          if (result.length === 0) {
            throw new UserError(
              'A user\'s selector doesn\'t match any of the ' +
              'pre-aggregations described by the Cube schemas.'
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
    } catch (e) {
      this.handleError({ e, context, query, res: response, started });
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
    if (!selector.contexts?.length) {
      jobs = await this.postPreAggregationsBuildJobs(
        context,
        selector,
      );
    } else {
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
    }
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
    const { timezones } = selector;
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
            preAggregations: preaggs.map(p => ({
              id: p.id,
              cacheOnly: undefined, // boolean
              partitions: undefined, // string[]
            })),
            forceBuildPreAggregations: undefined,
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
              metaCache.set(metaCacheKey, await compiler.metaConfigExtended(ctx));
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
    } catch (e) {
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
    } catch (e) {
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
    query: Record<string, any> | Record<string, any>[],
    context: RequestContext,
    persistent = false,
  ): Promise<[QueryType, NormalizedQuery[]]> {
    query = this.parseQueryParam(query);

    let queryType: QueryType = QueryTypeEnum.REGULAR_QUERY;
    if (!Array.isArray(query)) {
      query = this.compareDateRangeTransformer(query);
      if (Array.isArray(query)) {
        queryType = QueryTypeEnum.COMPARE_DATE_RANGE_QUERY;
      }
    } else {
      queryType = QueryTypeEnum.BLENDING_QUERY;
    }

    const queries = Array.isArray(query) ? query : [query];

    this.log({
      type: 'Query Rewrite',
      query
    }, context);

    const startTime = new Date().getTime();

    let normalizedQueries: NormalizedQuery[] = await Promise.all(
      queries.map(
        async (currentQuery) => {
          const normalizedQuery = normalizeQuery(currentQuery, persistent);
          const rewrite = await this.queryRewrite(
            normalizedQuery,
            context,
          );
          return normalizeQuery(
            rewrite,
            persistent,
          );
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
      if (queryGranularity.filter(Boolean).length === 0) {
        throw new UserError('Data blending query without granularity is not supported');
      }
    }

    return [queryType, normalizedQueries];
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
      await this.assertApiScope('data', context.securityContext);

      query = this.parseQueryParam(query);

      if (memberExpressions) {
        query = this.parseMemberExpressionsInQueries(query);
      }

      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context, disableLimitEnforcing);

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
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  private parseMemberExpressionsInQueries(query: Record<string, any> | Record<string, any>[]): Query | Query[] {
    if (Array.isArray(query)) {
      return query.map(q => this.parseMemberExpressionsInQuery(<Query>q));
    } else {
      return this.parseMemberExpressionsInQuery(<Query>query);
    }
  }

  private parseMemberExpressionsInQuery(query: Query): Query {
    return {
      ...query,
      measures: (query.measures || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
      dimensions: (query.dimensions || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
      segments: (query.segments || []).map(m => (typeof m === 'string' ? this.parseMemberExpression(m) : m)),
    };
  }

  private parseMemberExpression(memberExpression: string): string | MemberExpression {
    const match = memberExpression.match(memberExpressionRegex);
    if (match) {
      const args = match[3].split(',');
      args.push(`return \`${match[4]}\``);
      return {
        cubeName: match[1],
        name: match[2],
        expressionName: match[2],
        expression: Function.constructor.apply(null, args),
        definition: memberExpression,
      };
    } else {
      return memberExpression;
    }
  }

  public async sqlGenerators({ context, res }: { context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    try {
      const compilerApi = await this.getCompilerApi(context);
      const query = {
        requestId: context.requestId,
      };
      const cubeNameToDataSource = await compilerApi.cubeNameToDataSource(query);

      let dataSources = Object.keys(cubeNameToDataSource).map(c => cubeNameToDataSource[c]);
      dataSources = [...new Set(dataSources)];
      const dataSourceToSqlGenerator = (await Promise.all(
        dataSources.map(async dataSource => ({ [dataSource]: (await compilerApi.getSqlGenerator(query, dataSource)).sqlGenerator }))
      )).reduce((a, b) => ({ ...a, ...b }), {});

      res({ cubeNameToDataSource, dataSourceToSqlGenerator });
    } catch (e) {
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

      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

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
    } catch (e) {
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
  ) {
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
    return response;
  }

  /**
   * Convert adapter's result and other request paramters to a final
   * result object.
   * @internal
   */
  private getResultInternal(
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
  ) {
    return {
      query: normalizedQuery,
      data: transformData(
        sqlQuery.aliasNameToMember,
        {
          ...annotation.measures,
          ...annotation.dimensions,
          ...annotation.timeDimensions
        } as { [member: string]: ConfigItem },
        response.data,
        normalizedQuery,
        queryType,
        responseType,
      ),
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
        query
      }, context);

      const [queryType, normalizedQueries] =
        await this.getNormalizedQueries(query, context);

      let metaConfigResult = await (await this
        .getCompilerApi(context)).metaConfig({
        requestId: context.requestId
      });

      metaConfigResult = this.filterVisibleItemsInMeta(context, metaConfigResult);

      const sqlQueries = await this
        .getSqlQueriesInternal(context, normalizedQueries);

      let slowQuery = false;

      const results = await Promise.all(
        normalizedQueries.map(async (normalizedQuery, index) => {
          slowQuery = slowQuery ||
            Boolean(sqlQueries[index].slowQuery);

          const annotation = prepareAnnotation(
            metaConfigResult, normalizedQuery
          );

          const response = await this.getSqlResponseInternal(
            context,
            normalizedQuery,
            sqlQueries[index],
          );

          return this.getResultInternal(
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
              (r: any) => Object.keys(
                r.usedPreAggregations || {}
              ).length
            ).length,
          queriesWithData:
            results.filter((r: any) => r.data?.length).length,
          dbType: results.map(r => r.dbType),
          rowsCount: results.reduce((sum: number, r: any) => sum + r.data?.length || 0, 0),
        },
        context,
      );

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

      if (props.queryType === 'multi') {
        res({
          queryType,
          results,
          pivotQuery: getPivotQuery(queryType, normalizedQueries),
          slowQuery
        });
      } else {
        res(results[0]);
      }
    } catch (e) {
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
      apiType = 'sql',
    } = request;
    const requestStarted = new Date();

    try {
      await this.assertApiScope('data', context.securityContext);

      query = this.parseQueryParam(request.query);
      let resType: ResultType = ResultType.DEFAULT;

      query = this.parseMemberExpressionsInQueries(query);

      if (!Array.isArray(query) && query.responseFormat) {
        resType = query.responseFormat;
      }

      const [queryType, normalizedQueries] =
        await this.getNormalizedQueries(query, context, request.streaming);

      const compilerApi = await this.getCompilerApi(context);
      let metaConfigResult = await compilerApi.metaConfig({
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

            return this.getResultInternal(
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
      }

      res(request.streaming ? results[0] : {
        results,
      });
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  public async subscribeQueueEvents({ context, signedWithPlaygroundAuthSecret, connectionId, res }) {
    if (this.enforceSecurityChecks && !signedWithPlaygroundAuthSecret) {
      throw new CubejsHandlerError(
        403,
        'Forbidden',
        'Only for signed with playground auth secret'
      );
    }
    return (await this.getAdapterApi(context)).subscribeQueueEvents(connectionId, res);
  }

  public async unSubscribeQueueEvents({ context, connectionId }) {
    return (await this.getAdapterApi(context)).unSubscribeQueueEvents(connectionId);
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
          if (!Array.isArray(message) && message.error) {
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
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  protected resToResultFn(res: ExpressResponse) {
    return (message, { status }: { status?: number } = {}) => (status ? res.status(status).json(message) : res.json(message));
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

  protected handleErrorMiddleware: ErrorRequestHandler = async (e, req, res, next) => {
    this.handleError({
      e,
      context: (<any>req).context,
      res: this.resToResultFn(res),
      requestStarted: new Date(),
    });

    next(e);
  };

  public handleError({
    e, context, query, res, requestStarted
  }: any) {
    const requestId = getEnv('devMode') || context?.signedWithPlaygroundAuthSecret ? context?.requestId : undefined;
    
    const plainError = e.plainMessages;
    
    if (e instanceof CubejsHandlerError) {
      this.log({
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      }, context);
      res({ error: e.message, stack: e.stack, requestId, plainError }, { status: e.status });
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
          stack: e.stack,
          requestId
        },
        { status: 400 }
      );
    } else {
      this.log({
        type: 'Internal Server Error',
        query,
        error: e.stack || e.toString(),
        duration: this.duration(requestStarted)
      }, context);
      res({ error: e.toString(), stack: e.stack, requestId, plainError, }, { status: 500 });
    }
  }

  protected wrapCheckAuthMiddleware(fn: CheckAuthMiddlewareFn): CheckAuthMiddlewareFn {
    this.logger('CheckAuthMiddleware Middleware Deprecation', {
      warning: (
        'Option checkAuthMiddleware is now deprecated in favor of checkAuth, please migrate: ' +
        'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#checkauthmiddleware'
      )
    });

    let showWarningAboutNotObject = false;

    return (req, res, next) => {
      fn(req, res, (e) => {
        // We renamed authInfo to securityContext, but users can continue to use both ways
        if (req.securityContext && !req.authInfo) {
          req.authInfo = req.securityContext;
        } else if (req.authInfo) {
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

        next(e);
      });
    };
  }

  protected wrapCheckAuth(fn: CheckAuthFn): CheckAuthFn {
    // We dont need to span all logs with deprecation message
    let warningShowed = false;
    // securityContext should be object
    let showWarningAboutNotObject = false;

    return async (req, auth) => {
      await fn(req, auth);

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
    };
  }

  protected createDefaultCheckAuth(options?: JWTOptions, internalOptions?: CheckAuthInternalOptions): CheckAuthFn {
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
    };
  }

  protected createCheckAuthFn(options: ApiGatewayOptions): CheckAuthFn {
    const mainCheckAuthFn = options.checkAuth
      ? this.wrapCheckAuth(options.checkAuth)
      : this.createDefaultCheckAuth(options.jwt);

    if (this.playgroundAuthSecret) {
      const systemCheckAuthFn = this.createCheckAuthSystemFn();
      return async (ctx, authorization) => {
        try {
          await mainCheckAuthFn(ctx, authorization);
        } catch (error) {
          await systemCheckAuthFn(ctx, authorization);
        }
      };
    }

    return (ctx, authorization) => mainCheckAuthFn(ctx, authorization);
  }

  protected createCheckAuthSystemFn(): CheckAuthFn {
    const systemCheckAuthFn = this.createDefaultCheckAuth(
      {
        key: this.playgroundAuthSecret,
        algorithms: ['HS256']
      },
      { isPlaygroundCheckAuth: true }
    );

    return async (ctx, authorization) => {
      await systemCheckAuthFn(ctx, authorization);
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
            if (['graphql', 'meta', 'data', 'jobs'].indexOf(p) === -1) {
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
    if (typeof req.headers.authorization === 'string') {
      const parts = req.headers.authorization.split(' ', 2);
      if (parts.length === 1) {
        return parts[0];
      }

      return parts[1];
    }

    return undefined;
  }

  protected async checkAuthWrapper(checkAuthFn: CheckAuthFn, req: Request, res: ExpressResponse, next) {
    const token = this.extractAuthorizationHeaderWithSchema(req);

    try {
      await checkAuthFn(req, token);
      if (next) {
        next();
      }
    } catch (e: unknown) {
      if (e instanceof CubejsHandlerError) {
        const error = e.originalError || e;
        this.log({
          type: error.message,
          url: req.url,
          token,
          error: error.stack || error.toString()
        }, <any>req);
        
        res.status(e.status).json({ error: e.message });
      } else if (e instanceof Error) {
        this.log({
          type: 'Auth Error',
          token,
          error: e.stack || e.toString()
        }, <any>req);

        res.status(500).json({
          error: e.toString(),
          stack: e.stack
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
      if (next) {
        next();
      }
    } catch (e) {
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
    this.log({
      type,
      driverType: e.driverType,
      error: (e as Error).stack || (e as Error).toString(),
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
