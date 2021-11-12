/* eslint-disable no-restricted-syntax */
import jwt, { Algorithm as JWTAlgorithm } from 'jsonwebtoken';
import R from 'ramda';
import moment from 'moment';
import bodyParser from 'body-parser';
import { getEnv, getRealType } from '@cubejs-backend/shared';

import type {
  Application as ExpressApplication,
  ErrorRequestHandler,
  NextFunction,
  RequestHandler,
  Response,
} from 'express';

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
  QUERY_TYPE, validatePostRewrite,
} from './query';
import {
  CheckAuthFn,
  CheckAuthMiddlewareFn,
  ExtendContextFn,
  ExtendedRequestContext,
  JWTOptions,
  QueryRewriteFn,
  Request,
  RequestContext,
  RequestLoggerMiddlewareFn,
  SecurityContextExtractorFn,
} from './interfaces';
import { cachedHandler } from './cached-handler';
import { createJWKsFetcher } from './jwk';
import { SQLServer } from './sql-server';

type ResponseResultFn = (message: Record<string, any> | Record<string, any>[], extra?: { status: number }) => void;

type MetaConfig = {
  config: {
    name: string,
    title: string
  }
};

type CheckAuthInternalOptions = {
  isPlaygroundCheckAuth: boolean;
};

const toConfigMap = (metaConfig: MetaConfig[]) => R.fromPairs(
  R.map((c) => [c.config.name, c.config], metaConfig)
);

const prepareAnnotation = (metaConfig: MetaConfig[], query: any) => {
  const configMap = toConfigMap(metaConfig);

  const annotation = (memberType) => (member) => {
    const [cubeName, fieldName] = member.split('.');
    const memberWithoutGranularity = [cubeName, fieldName].join('.');
    const config = configMap[cubeName][memberType].find(m => m.name === memberWithoutGranularity);

    if (!config) {
      return undefined;
    }

    return [member, {
      title: config.title,
      shortTitle: config.shortTitle,
      description: config.description,
      type: config.type,
      format: config.format,
      meta: config.meta,
      ...(memberType === 'measures' ? {
        drillMembers: config.drillMembers,
        drillMembersGrouped: config.drillMembersGrouped
      } : {})
    }];
  };

  const dimensions = (query.dimensions || []);
  return {
    measures: R.fromPairs((query.measures || []).map(annotation('measures')).filter(a => !!a)),
    dimensions: R.fromPairs(dimensions.map(annotation('dimensions')).filter(a => !!a)),
    segments: R.fromPairs((query.segments || []).map(annotation('segments')).filter(a => !!a)),
    timeDimensions: R.fromPairs(
      R.unnest(
        (query.timeDimensions || [])
          .filter(td => !!td.granularity)
          .map(
            td => [annotation('dimensions')(`${td.dimension}.${td.granularity}`)].concat(
              // TODO: deprecated: backward compatibility for referencing time dimensions without granularity
              dimensions.indexOf(td.dimension) === -1 ? [annotation('dimensions')(td.dimension)] : []
            ).filter(a => !!a)
          )
      )
    ),
  };
};

const transformValue = (value, type) => {
  if (value && (type === 'time' || value instanceof Date)) { // TODO support for max time
    return (value instanceof Date ? moment(value) : moment.utc(value)).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  return value && value.value ? value.value : value; // TODO move to sql adapter
};

const transformData = (aliasToMemberNameMap, annotation, data, query, queryType) => (data.map(r => {
  const row = R.pipe(
    // @ts-ignore
    R.toPairs,
    R.map(p => {
      const memberName = aliasToMemberNameMap[p[0]];
      const annotationForMember = annotation[memberName];

      if (!annotationForMember) {
        throw new UserError(`You requested hidden member: '${p[0]}'. Please make it visible using \`shown: true\`. Please note primaryKey fields are \`shown: false\` by default: https://cube.dev/docs/schema/reference/joins#setting-a-primary-key.`);
      }

      const transformResult = [
        memberName,
        transformValue(p[1], annotationForMember.type)
      ];

      const path = memberName.split('.');

      // TODO: deprecated: backward compatibility for referencing time dimensions without granularity
      const memberNameWithoutGranularity = [path[0], path[1]].join('.');
      if (path.length === 3 && (query.dimensions || []).indexOf(memberNameWithoutGranularity) === -1) {
        return [
          transformResult,
          [
            memberNameWithoutGranularity,
            transformResult[1]
          ]
        ];
      }

      return [transformResult];
    }),
    // @ts-ignore
    R.unnest,
    R.fromPairs
  // @ts-ignore
  )(r);

  // @ts-ignore
  const [{ dimension, granularity, dateRange } = {}] = query.timeDimensions;

  if (queryType === QUERY_TYPE.COMPARE_DATE_RANGE_QUERY) {
    return {
      ...row,
      compareDateRange: dateRange.join(' - ')
    };
  } else if (queryType === QUERY_TYPE.BLENDING_QUERY) {
    return {
      ...row,
      [['time', granularity].join('.')]: row[[dimension, granularity].join('.')]
    };
  }

  return row;
}));

export type UserBackgroundContext = {
  // @deprecated Renamed to securityContext, please use securityContext.
  authInfo?: any;
  securityContext: any;
};

type BaseRequest = {
 context: RequestContext;
 res: ResponseResultFn
};

type QueryRequest = BaseRequest & {
  query: Record<string, any> | Record<string, any>[];
  queryType?: 'multi'
};

export interface ApiGatewayOptions {
  standalone: boolean;
  dataSourceStorage: any;
  refreshScheduler: any;
  scheduledRefreshContexts?: () => Promise<UserBackgroundContext[]>;
  scheduledRefreshTimeZones?: String[];
  basePath: string;
  extendContext?: ExtendContextFn;
  checkAuth?: CheckAuthFn;
  // @deprecated Please use checkAuth
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  jwt?: JWTOptions;
  requestLoggerMiddleware?: RequestLoggerMiddlewareFn;
  queryRewrite?: QueryRewriteFn;
  subscriptionStore?: any;
  enforceSecurityChecks?: boolean;
  playgroundAuthSecret?: string;
  serverCoreVersion?: string;
}

export class ApiGateway {
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

  protected readonly checkAuthMiddleware: CheckAuthMiddlewareFn;

  protected readonly requestLoggerMiddleware: RequestLoggerMiddlewareFn;

  protected readonly securityContextExtractor: SecurityContextExtractorFn;

  protected readonly releaseListeners: (() => any)[] = [];

  protected readonly playgroundAuthSecret?: string;

  public constructor(
    protected readonly apiSecret: string,
    protected readonly compilerApi: any,
    protected readonly adapterApi: any,
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
    this.checkAuthMiddleware = options.checkAuthMiddleware
      ? this.wrapCheckAuthMiddleware(options.checkAuthMiddleware)
      : this.checkAuth;
    this.securityContextExtractor = this.createSecurityContextExtractor(options.jwt);
    this.requestLoggerMiddleware = options.requestLoggerMiddleware || this.requestLogger;
  }

  public initApp(app: ExpressApplication) {
    const userMiddlewares: RequestHandler[] = [
      this.checkAuthMiddleware,
      this.requestContextMiddleware,
      this.requestLoggerMiddleware
    ];

    // @todo Should we pass requestLoggerMiddleware?
    const guestMiddlewares = [];

    app.get(`${this.basePath}/v1/load`, userMiddlewares, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType
      });
    }));

    const jsonParser = bodyParser.json({ limit: '1mb' });
    app.post(`${this.basePath}/v1/load`, jsonParser, userMiddlewares, (async (req, res) => {
      await this.load({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.body.queryType
      });
    }));

    app.get(`${this.basePath}/v1/subscribe`, userMiddlewares, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType
      });
    }));

    app.get(`${this.basePath}/v1/sql`, userMiddlewares, (async (req, res) => {
      await this.sql({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/meta`, userMiddlewares, (async (req, res) => {
      await this.meta({
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/run-scheduled-refresh`, userMiddlewares, (async (req, res) => {
      await this.runScheduledRefresh({
        queryingOptions: req.query.queryingOptions,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/dry-run`, userMiddlewares, (async (req, res) => {
      await this.dryRun({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.post(`${this.basePath}/v1/dry-run`, jsonParser, userMiddlewares, (async (req, res) => {
      await this.dryRun({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    if (this.playgroundAuthSecret) {
      const systemMiddlewares: RequestHandler[] = [
        this.checkAuthSystemMiddleware,
        this.requestContextMiddleware,
        this.requestLoggerMiddleware
      ];

      app.get('/cubejs-system/v1/context', systemMiddlewares, this.createSystemContextHandler(this.basePath));

      app.get('/cubejs-system/v1/pre-aggregations', systemMiddlewares, (async (req, res) => {
        await this.getPreAggregations({
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.get('/cubejs-system/v1/pre-aggregations/security-contexts', systemMiddlewares, (async (req, res) => {
        const contexts = this.scheduledRefreshContexts ? await this.scheduledRefreshContexts() : [];
        this.resToResultFn(res)({
          securityContexts: contexts
            .map(ctx => ctx && (ctx.securityContext || ctx.authInfo))
            .filter(ctx => ctx)
        });
      }));

      app.get('/cubejs-system/v1/pre-aggregations/timezones', systemMiddlewares, (async (req, res) => {
        this.resToResultFn(res)({
          timezones: this.scheduledRefreshTimeZones || []
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/partitions', jsonParser, systemMiddlewares, (async (req, res) => {
        await this.getPreAggregationPartitions({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/preview', jsonParser, systemMiddlewares, (async (req, res) => {
        await this.getPreAggregationPreview({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/build', jsonParser, systemMiddlewares, (async (req, res) => {
        await this.buildPreAggregations({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/queue', jsonParser, systemMiddlewares, (async (req, res) => {
        await this.getPreAggregationsInQueue({
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));

      app.post('/cubejs-system/v1/pre-aggregations/cancel', jsonParser, systemMiddlewares, (async (req, res) => {
        await this.cancelPreAggregationsFromQueue({
          query: req.body.query,
          context: req.context,
          res: this.resToResultFn(res)
        });
      }));
    }

    app.get('/readyz', guestMiddlewares, cachedHandler(this.readiness));
    app.get('/livez', guestMiddlewares, cachedHandler(this.liveness));

    app.post(`${this.basePath}/v1/pre-aggregations/can-use`, userMiddlewares, (req: Request, res: Response) => {
      const { transformedQuery, references } = req.body;

      const canUsePreAggregationForTransformedQuery = this.compilerApi(req.context)
        .canUsePreAggregationForTransformedQuery(transformedQuery, references);

      res.json({ canUsePreAggregationForTransformedQuery });
    });

    app.use(this.handleErrorMiddleware);
  }

  public initSQLServer() {
    return new SQLServer(this);
  }

  public initSubscriptionServer(sendMessage: WebSocketSendMessageFn) {
    return new SubscriptionServer(this, sendMessage, this.subscriptionStore);
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

  public async meta({ context, res }: { context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    function visibilityFilter(item) {
      return getEnv('devMode') || context.signedWithPlaygroundAuthSecret || item.isVisible;
    }

    try {
      const metaConfig = await this.getCompilerApi(context).metaConfig({
        requestId: context.requestId,
      });
      const cubes = metaConfig
        .map((meta) => meta.config)
        .map((cube) => ({
          ...cube,
          measures: cube.measures.filter(visibilityFilter),
          dimensions: cube.dimensions.filter(visibilityFilter),
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

  public async getPreAggregations({ context, res }: { context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();
    try {
      const compilerApi = this.getCompilerApi(context);
      const preAggregations = await compilerApi.preAggregations();

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          compilerApi,
          normalizeQueryPreAggregations(
            {
              timezones: this.scheduledRefreshTimeZones,
              preAggregations: preAggregations.map(p => ({
                id: p.id
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
      const orchestratorApi = this.getAdapterApi(context);
      const compilerApi = this.getCompilerApi(context);

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          compilerApi,
          query
        );

      const versionEntriesResult = preAggregationPartitions &&
        await orchestratorApi.getPreAggregationVersionEntries(
          context,
          preAggregationPartitions,
          compilerApi.preAggregationsSchema
        );

      const mergePartitionsAndVersionEntries = () => ({ preAggregation, partitions, ...props }) => ({
        ...props,
        preAggregation,
        partitions: partitions.map(partition => {
          partition.versionEntries = versionEntriesResult?.versionEntriesByTableName[partition.sql?.tableName] || [];
          partition.structureVersion = versionEntriesResult?.structureVersionsByTableName[partition.sql?.tableName];
          return partition;
        }),
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

      const orchestratorApi = this.getAdapterApi(context);
      const compilerApi = this.getCompilerApi(context);

      const preAggregationPartitions = await this.refreshScheduler()
        .preAggregationPartitions(
          context,
          compilerApi,
          {
            timezones: [timezone],
            preAggregations: [{ id: preAggregationId }]
          }
        );
      const { partitions } = (preAggregationPartitions && preAggregationPartitions[0] || {});
      const preAggregationPartition = partitions && partitions.find(p => p.sql?.tableName === versionEntry.table_name);

      res({
        preview: preAggregationPartition && await orchestratorApi.getPreAggregationPreview(
          context,
          preAggregationPartition.sql
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
          this.getCompilerApi(context),
          query
        );

      res({ result });
    } catch (e) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  public async getPreAggregationsInQueue(
    { context, res }: { context: RequestContext, res: ResponseResultFn }
  ) {
    const requestStarted = new Date();
    try {
      const orchestratorApi = this.getAdapterApi(context);
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
      const orchestratorApi = this.getAdapterApi(context);
      res({
        result: await orchestratorApi.cancelPreAggregationQueriesFromQueue(queryKeys, dataSource)
      });
    } catch (e) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  protected async getNormalizedQueries(query, context: RequestContext): Promise<any> {
    query = this.parseQueryParam(query);
    let queryType = QUERY_TYPE.REGULAR_QUERY;

    if (!Array.isArray(query)) {
      query = this.compareDateRangeTransformer(query);
      if (Array.isArray(query)) {
        queryType = QUERY_TYPE.COMPARE_DATE_RANGE_QUERY;
      }
    } else {
      queryType = QUERY_TYPE.BLENDING_QUERY;
    }

    const queries = Array.isArray(query) ? query : [query];
    const normalizedQueries = await Promise.all(
      queries.map(async (currentQuery) => validatePostRewrite(await this.queryRewrite(normalizeQuery(currentQuery), context)))
    );

    if (normalizedQueries.find((currentQuery) => !currentQuery)) {
      throw new Error('queryTransformer returned null query. Please check your queryTransformer implementation');
    }

    if (queryType === QUERY_TYPE.BLENDING_QUERY) {
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

  public async sql({ query, context, res }: QueryRequest) {
    const requestStarted = new Date();

    try {
      query = this.parseQueryParam(query);
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all<any>(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
          this.coerceForSqlQuery(normalizedQuery, context),
          { includeDebugInfo: getEnv('devMode') || context.signedWithPlaygroundAuthSecret }
        ))
      );

      const toQuery = (sqlQuery) => ({
        ...sqlQuery,
        order: R.fromPairs(sqlQuery.order.map(({ id: key, desc }) => [key, desc ? 'desc' : 'asc']))
      });

      res(queryType === QUERY_TYPE.REGULAR_QUERY ?
        { sql: toQuery(sqlQueries[0]) } :
        sqlQueries.map((sqlQuery) => ({ sql: toQuery(sqlQuery) })));
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
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
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all<any>(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
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

  public async load({ query, context, res, ...props }: QueryRequest) {
    const requestStarted = new Date();

    try {
      query = this.parseQueryParam(query);
      this.log({
        type: 'Load Request',
        query
      }, context);

      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const [metaConfigResult, ...sqlQueries] = await Promise.all(
        [
          this.getCompilerApi(context).metaConfig({ requestId: context.requestId })
        ].concat(normalizedQueries.map(
          async (normalizedQuery, index) => {
            const loadRequestSQLStarted = new Date();
            const sqlQuery = await this.getCompilerApi(context).getSql(
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
        ))
      );

      let slowQuery = false;
      const results = await Promise.all<any[]>(normalizedQueries.map(async (normalizedQuery, index) => {
        const sqlQuery = sqlQueries[index];
        const annotation = prepareAnnotation(metaConfigResult, normalizedQuery);
        const aliasToMemberNameMap = sqlQuery.aliasNameToMember;

        const toExecute = {
          ...sqlQuery,
          query: sqlQuery.sql[0],
          values: sqlQuery.sql[1],
          continueWait: true,
          renewQuery: normalizedQuery.renewQuery,
          requestId: context.requestId,
          context
        };

        const response = await this.getAdapterApi(context).executeQuery(toExecute);

        const flattenAnnotation = {
          ...annotation.measures,
          ...annotation.dimensions,
          ...annotation.timeDimensions
        };

        slowQuery = slowQuery || Boolean(response.slowQuery);

        return {
          query: normalizedQuery,
          data: transformData(
            aliasToMemberNameMap,
            flattenAnnotation,
            response.data,
            normalizedQuery,
            queryType
          ),
          lastRefreshTime: response.lastRefreshTime?.toISOString(),
          ...(getEnv('devMode') || context.signedWithPlaygroundAuthSecret ? {
            refreshKeyValues: response.refreshKeyValues,
            usedPreAggregations: response.usedPreAggregations,
            transformedQuery: sqlQuery.canUseTransformedQuery,
            requestId: context.requestId,
          } : null),
          annotation,
          dataSource: response.dataSource,
          dbType: response.dbType,
          extDbType: response.extDbType,
          external: response.external,
          slowQuery: Boolean(response.slowQuery)
        };
      }));

      this.log(
        {
          type: 'Load Request Success',
          query,
          duration: this.duration(requestStarted),
          isPlayground: Boolean(context.signedWithPlaygroundAuthSecret),
          queriesWithPreAggregations: results.filter((r: any) => Object.keys(r.usedPreAggregations || {}).length)
            .length,
          queriesWithData: results.filter((r: any) => r.data?.length).length,
        },
        context
      );

      if (queryType !== QUERY_TYPE.REGULAR_QUERY && props.queryType == null) {
        throw new UserError(`'${queryType}' query type is not supported by the client. Please update the client.`);
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

  public subscribeQueueEvents({ context, signedWithPlaygroundAuthSecret, connectionId, res }) {
    if (this.enforceSecurityChecks && !signedWithPlaygroundAuthSecret) {
      throw new CubejsHandlerError(
        403,
        'Forbidden',
        'Only for signed with playground auth secret'
      );
    }
    return this.getAdapterApi(context).subscribeQueueEvents(connectionId, res);
  }

  public unSubscribeQueueEvents({ context, connectionId }) {
    return this.getAdapterApi(context).unSubscribeQueueEvents(connectionId);
  }

  public async subscribe({
    query, context, res, subscribe, subscriptionState, queryType
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
        await this.load({ query, context, res, queryType });
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
        queryType
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

  protected resToResultFn(res: Response) {
    return (message, { status }: { status?: number } = {}) => (status ? res.status(status).json(message) : res.json(message));
  }

  protected parseQueryParam(query) {
    if (!query || query === 'undefined') {
      throw new UserError('query param is required');
    }
    if (typeof query === 'string') {
      query = JSON.parse(query);
    }
    return query;
  }

  protected getCompilerApi(context) {
    if (typeof this.compilerApi === 'function') {
      return this.compilerApi(context);
    }

    return this.compilerApi;
  }

  protected getAdapterApi(context) {
    if (typeof this.adapterApi === 'function') {
      return this.adapterApi(context);
    }

    return this.adapterApi;
  }

  public async contextByReq(req: Request, securityContext, requestId: string): Promise<ExtendedRequestContext> {
    const extensions = typeof this.extendContext === 'function' ? await this.extendContext(req) : {};

    return {
      securityContext,
      // Deprecated, but let's allow it for now.
      authInfo: securityContext,
      signedWithPlaygroundAuthSecret: Boolean(req.signedWithPlaygroundAuthSecret),
      requestId,
      ...extensions
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
    if (e instanceof CubejsHandlerError) {
      this.log({
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      }, context);
      res({ error: e.message }, { status: e.status });
    } else if (e.error === 'Continue wait') {
      this.log({
        type: 'Continue wait',
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      }, context);
      res(e, { status: 200 });
    } else if (e.error) {
      this.log({
        type: 'Orchestrator error',
        query,
        error: e.error,
        duration: this.duration(requestStarted)
      }, context);
      res(e, { status: 400 });
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
          error: e.message
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
      res({ error: e.toString() }, { status: 500 });
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
          typeof options.jwkUrl === 'function' ? options.jwkUrl(decoded) : <string>options.jwkUrl,
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
        } catch (e) {
          if (this.enforceSecurityChecks) {
            throw new CubejsHandlerError(403, 'Forbidden', 'Invalid token');
          } else {
            this.log({
              type: e.message,
              token: auth,
              error: e.stack || e.toString()
            }, <any>req);
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

  protected async checkAuthWrapper(checkAuthFn: CheckAuthFn, req: Request, res: Response, next) {
    const token = this.extractAuthorizationHeaderWithSchema(req);

    try {
      await checkAuthFn(req, token);
      if (next) {
        next();
      }
    } catch (e) {
      if (e instanceof CubejsHandlerError) {
        res.status(e.status).json({ error: e.message });
      } else {
        this.log({
          type: 'Auth Error',
          token,
          error: e.stack || e.toString()
        }, <any>req);
        res.status(500).json({ error: e.toString() });
      }
    }
  }

  protected checkAuth: RequestHandler = async (req, res, next) => {
    await this.checkAuthWrapper(this.checkAuthFn, req, res, next);
  };

  protected checkAuthSystemMiddleware: RequestHandler = async (req, res, next) => {
    await this.checkAuthWrapper(this.checkAuthSystemFn, req, res, next);
  };

  protected requestContextMiddleware: RequestHandler = async (req: Request, res: Response, next: NextFunction) => {
    req.context = await this.contextByReq(req, req.securityContext, getRequestIdFromRequest(req));
    if (next) {
      next();
    }
  };

  protected requestLogger: RequestHandler = async (req: Request, res: Response, next: NextFunction) => {
    const details = requestParser(req, res);

    this.log({ type: 'REST API Request', ...details }, req.context);

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

  protected log(event: { type: string, [key: string]: any }, context?: RequestContext) {
    const { type, ...restParams } = event;

    this.logger(type, {
      ...restParams,
      ...(!context ? undefined : {
        securityContext: context.securityContext,
        requestId: context.requestId
      })
    });
  }

  protected healthResponse(res: Response, health: 'HEALTH' | 'DOWN') {
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

  protected readiness: RequestHandler = async (req, res) => {
    let health: 'HEALTH' | 'DOWN' = 'HEALTH';

    if (this.standalone) {
      const orchestratorApi = await this.adapterApi({});

      try {
        // todo: test other data sources
        orchestratorApi.addDataSeenSource('default');
        await orchestratorApi.testConnection();
      } catch (e) {
        this.log({
          type: 'Internal Server Error on readiness probe',
          error: e.stack || e.toString(),
        });

        return this.healthResponse(res, 'DOWN');
      }

      try {
        await orchestratorApi.testOrchestratorConnections();
      } catch (e) {
        this.log({
          type: 'Internal Server Error on readiness probe',
          error: e.stack || e.toString(),
        });

        health = 'DOWN';
      }
    }

    return this.healthResponse(res, health);
  };

  protected liveness: RequestHandler = async (req, res) => {
    let health: 'HEALTH' | 'DOWN' = 'HEALTH';

    try {
      await this.dataSourceStorage.testConnections();
    } catch (e) {
      this.log({
        type: 'Internal Server Error on liveness probe',
        error: e.stack || e.toString(),
      });

      return this.healthResponse(res, 'DOWN');
    }

    try {
      // @todo Optimize this moment?
      await this.dataSourceStorage.testOrchestratorConnections();
    } catch (e) {
      this.log({
        type: 'Internal Server Error on liveness probe',
        error: e.stack || e.toString(),
      });

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
