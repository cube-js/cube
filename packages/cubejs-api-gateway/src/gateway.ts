import jwt from 'jsonwebtoken';
import R from 'ramda';
import moment from 'moment';
import bodyParser from 'body-parser';
import { getRealType } from '@cubejs-backend/shared';

import type {
  Response, NextFunction,
  Application as ExpressApplication,
  RequestHandler,
  ErrorRequestHandler
} from 'express';

import { getRequestIdFromRequest, requestParser } from './requestParser';
import { UserError } from './UserError';
import { CubejsHandlerError } from './CubejsHandlerError';
import { SubscriptionServer, WebSocketSendMessageFn } from './SubscriptionServer';
import { LocalSubscriptionStore } from './LocalSubscriptionStore';
import { getPivotQuery, getQueryGranularity, normalizeQuery, QUERY_TYPE } from './query';
import {
  CheckAuthFn,
  CheckAuthMiddlewareFn,
  ExtendContextFn,
  QueryTransformerFn,
  RequestContext,
  RequestLoggerMiddlewareFn,
  Request,
} from './interfaces';
import { cachedHandler } from './cached-handler';

type ResponseResultFn = (message: object, extra?: { status: number }) => void;

type MetaConfig = {
  config: {
    name: string,
    title: string
  }
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
        throw new UserError(`You requested hidden member: '${p[0]}'. Please make it visible using \`shown: true\`. Please note primaryKey fields are \`shown: false\` by default: https://cube.dev/docs/joins#setting-a-primary-key.`);
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

export interface ApiGatewayOptions {
  standalone: boolean;
  dataSourceStorage: any;
  refreshScheduler: any;
  basePath: string;
  extendContext?: ExtendContextFn;
  checkAuth?: CheckAuthFn;
  // @deprecated Please use checkAuth
  checkAuthMiddleware?: CheckAuthMiddlewareFn;
  requestLoggerMiddleware?: RequestLoggerMiddlewareFn;
  queryTransformer?: QueryTransformerFn;
  subscriptionStore?: any;
  enforceSecurityChecks?: boolean;
}

export class ApiGateway {
  protected readonly refreshScheduler: any;

  protected readonly basePath: string;

  protected readonly queryTransformer: QueryTransformerFn;

  protected readonly subscriptionStore: any;

  protected readonly enforceSecurityChecks: boolean;

  protected readonly standalone: boolean;

  protected readonly extendContext?: ExtendContextFn;

  protected readonly dataSourceStorage: any;

  public readonly checkAuthFn: CheckAuthFn;

  protected readonly checkAuthMiddleware: CheckAuthMiddlewareFn;

  protected readonly requestLoggerMiddleware: RequestLoggerMiddlewareFn;

  // Flag to show deprecation for u, only once
  protected checkAuthDeprecationShown: boolean = false;

  public constructor(
    protected readonly apiSecret: string,
    protected readonly compilerApi: any,
    protected readonly adapterApi: any,
    protected readonly logger: any,
    options: ApiGatewayOptions,
  ) {
    this.dataSourceStorage = options.dataSourceStorage;
    this.refreshScheduler = options.refreshScheduler;
    this.standalone = options.standalone;
    this.basePath = options.basePath;

    this.queryTransformer = options.queryTransformer || (async (query) => query);
    this.subscriptionStore = options.subscriptionStore || new LocalSubscriptionStore();
    this.enforceSecurityChecks = options.enforceSecurityChecks || (process.env.NODE_ENV === 'production');
    this.extendContext = options.extendContext;
    this.checkAuthFn = options.checkAuth ? this.wrapCheckAuth(options.checkAuth) : this.defaultCheckAuth.bind(this);
    this.checkAuthMiddleware = options.checkAuthMiddleware
      ? this.wrapCheckAuthMiddleware(options.checkAuthMiddleware)
      : this.checkAuth.bind(this);
    this.requestLoggerMiddleware = options.requestLoggerMiddleware || this.requestLogger.bind(this);
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

    app.get('/readyz', guestMiddlewares, cachedHandler(this.readiness));
    app.get('/livez', guestMiddlewares, cachedHandler(this.liveness));

    app.use(this.handleErrorMiddleware);
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
    try {
      const metaConfig = await this.getCompilerApi(context).metaConfig({ requestId: context.requestId });
      const cubes = metaConfig.map(c => c.config);
      res({ cubes });
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
      queries.map((currentQuery) => this.queryTransformer(normalizeQuery(currentQuery), context))
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

  public async sql({ query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    try {
      query = this.parseQueryParam(query);
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
          this.coerceForSqlQuery(normalizedQuery, context),
          { includeDebugInfo: process.env.NODE_ENV !== 'production' }
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

  protected coerceForSqlQuery(query, context: Readonly<RequestContext>) {
    let securityContext: any = {};

    if (typeof context.securityContext === 'object' && context.securityContext !== null) {
      if (context.securityContext.u) {
        if (!this.checkAuthDeprecationShown) {
          this.logger('JWT U Property Deprecation', {
            warning: (
              'Storing security context in the u property within the payload is now deprecated, please migrate: ' +
              'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#authinfo'
            )
          });

          this.checkAuthDeprecationShown = true;
        }

        securityContext = {
          ...context.securityContext,
          ...context.securityContext.u,
        };

        delete securityContext.u;
      } else {
        securityContext = context.securityContext;
      }
    }

    return {
      ...query,
      timeDimensions: query.timeDimensions || [],
      contextSymbols: {
        securityContext,
      },
      requestId: context.requestId
    };
  }

  protected async dryRun({ query, context, res }: { query: any, context: RequestContext, res: ResponseResultFn }) {
    const requestStarted = new Date();

    try {
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all<any>(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
          this.coerceForSqlQuery(normalizedQuery, context),
          { includeDebugInfo: process.env.NODE_ENV !== 'production' }
        ))
      );

      res({
        queryType,
        normalizedQueries,
        queryOrder: sqlQueries.map((sqlQuery) => R.fromPairs(
          sqlQuery.order.map(({ id: member, desc }) => [member, desc ? 'desc' : 'asc'])
        )),
        pivotQuery: getPivotQuery(queryType, normalizedQueries)
      });
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  public async load({ query, context, res, ...props }: any) {
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
      const results = await Promise.all(normalizedQueries.map(async (normalizedQuery, index) => {
        const sqlQuery = sqlQueries[index];
        const annotation = prepareAnnotation(metaConfigResult, normalizedQuery);
        const aliasToMemberNameMap = sqlQuery.aliasNameToMember;

        const toExecute = {
          ...sqlQuery,
          query: sqlQuery.sql[0],
          values: sqlQuery.sql[1],
          continueWait: true,
          renewQuery: normalizedQuery.renewQuery,
          requestId: context.requestId
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
          lastRefreshTime: response.lastRefreshTime && response.lastRefreshTime.toISOString(),
          ...(process.env.NODE_ENV === 'production' ? undefined : {
            refreshKeyValues: response.refreshKeyValues,
            usedPreAggregations: response.usedPreAggregations
          }),
          annotation,
          slowQuery: Boolean(response.slowQuery)
        };
      }));

      this.log({
        type: 'Load Request Success',
        query,
        duration: this.duration(requestStarted)
      }, context);

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
          if (message.error) {
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
    // @ts-ignore
    return (message, { status } = {}) => (status ? res.status(status).json(message) : res.json(message));
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

  public async contextByReq(req: Request, securityContext, requestId: string) {
    const extensions = await Promise.resolve(typeof this.extendContext === 'function' ? this.extendContext(req) : {});

    return {
      securityContext,
      // Deprecated, but let's allow it for now.
      authInfo: securityContext,
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
  }

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

    // securityContext should be object
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

  protected async defaultCheckAuth(req: Request, auth?: string) {
    if (auth) {
      const secret = this.apiSecret;
      try {
        req.securityContext = jwt.verify(auth, secret);
      } catch (e) {
        if (this.enforceSecurityChecks) {
          throw new UserError('Invalid token');
        } else {
          this.log({
            type: 'Invalid Token',
            token: auth,
            error: e.stack || e.toString()
          }, <any>req);
        }
      }
    } else if (this.enforceSecurityChecks) {
      throw new UserError('Authorization header isn\'t set');
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

  protected checkAuth: RequestHandler = async (req, res, next) => {
    const token = this.extractAuthorizationHeaderWithSchema(req);

    try {
      await this.checkAuthFn(req, token);
      if (next) {
        next();
      }
    } catch (e) {
      if (e instanceof UserError) {
        res.status(403).json({ error: e.message });
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

  protected requestContextMiddleware: RequestHandler = async (req: Request, res: Response, next: NextFunction) => {
    req.context = await this.contextByReq(req, req.securityContext, getRequestIdFromRequest(req));
    if (next) {
      next();
    }
  }

  protected requestLogger: RequestHandler = async (req: Request, res: Response, next: NextFunction) => {
    const details = requestParser(req, res);

    this.log({ type: 'REST API Request', ...details }, req.context);

    if (next) {
      next();
    }
  }

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
  }

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
  }
}
