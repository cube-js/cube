import jwt from 'jsonwebtoken';
import R from 'ramda';
import moment from 'moment';
import uuid from 'uuid/v4';
import bodyParser from 'body-parser';

import { requestParser } from './requestParser';
import { UserError } from './UserError';
import { CubejsHandlerError } from './CubejsHandlerError';
import { SubscriptionServer } from './SubscriptionServer';
import { LocalSubscriptionStore } from './LocalSubscriptionStore';
import { getPivotQuery, getQueryGranularity, normalizeQuery, QUERY_TYPE } from './query';

const toConfigMap = (metaConfig) => (
  R.pipe(
    R.map(c => [c.config.name, c.config]),
    R.fromPairs
  )(metaConfig)
);

const prepareAnnotation = (metaConfig, query) => {
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
  )(r);

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

const coerceForSqlQuery = (query, context) => ({
  ...query,
  timeDimensions: query.timeDimensions || [],
  contextSymbols: {
    userContext: context.authInfo && context.authInfo.u || {}
  },
  requestId: context.requestId
});

export class ApiGateway {
  constructor(apiSecret, compilerApi, adapterApi, logger, options) {
    options = options || {};
    this.apiSecret = apiSecret;
    this.compilerApi = compilerApi;
    this.adapterApi = adapterApi;
    this.refreshScheduler = options.refreshScheduler;
    this.logger = logger;
    this.basePath = options.basePath || '/cubejs-api';
    // eslint-disable-next-line no-unused-vars,@typescript-eslint/no-unused-vars
    this.queryTransformer = options.queryTransformer || (async (query, context) => query);
    this.subscriptionStore = options.subscriptionStore || new LocalSubscriptionStore();
    this.enforceSecurityChecks = options.enforceSecurityChecks || (process.env.NODE_ENV === 'production');
    this.extendContext = options.extendContext;

    this.initializeMiddleware(options);
  }

  initializeMiddleware(options) {
    const checkAuthMiddleware = options.checkAuthMiddleware || this.checkAuth.bind(this);
    this.checkAuthFn = options.checkAuth || this.defaultCheckAuth.bind(this);
    const requestContextMiddleware = this.requestContextMiddleware.bind(this);
    const requestLoggerMiddleware = options.requestLoggerMiddleware || this.requestLogger.bind(this);
    this.requestMiddleware = [checkAuthMiddleware, requestContextMiddleware, requestLoggerMiddleware];
  }

  initApp(app) {
    app.get(`${this.basePath}/v1/load`, this.requestMiddleware, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType
      });
    }));

    const jsonParser = bodyParser.json({ limit: '1mb' });
    app.post(`${this.basePath}/v1/load`, jsonParser, this.requestMiddleware, (async (req, res) => {
      await this.load({
        query: req.body.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.body.queryType
      });
    }));

    app.get(`${this.basePath}/v1/subscribe`, this.requestMiddleware, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res),
        queryType: req.query.queryType
      });
    }));

    app.get(`${this.basePath}/v1/sql`, this.requestMiddleware, (async (req, res) => {
      await this.sql({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/meta`, this.requestMiddleware, (async (req, res) => {
      await this.meta({
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/run-scheduled-refresh`, this.requestMiddleware, (async (req, res) => {
      await this.runScheduledRefresh({
        queryingOptions: req.query.queryingOptions,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/dry-run`, this.requestMiddleware, (async (req, res) => {
      await this.dryRun({
        query: req.query.query,
        context: req.context,
        res: this.resToResultFn(res)
      });
    }));
  }

  initSubscriptionServer(sendMessage) {
    return new SubscriptionServer(this, sendMessage, this.subscriptionStore);
  }

  duration(requestStarted) {
    return requestStarted && (new Date().getTime() - requestStarted.getTime());
  }

  async runScheduledRefresh({ context, res, queryingOptions }) {
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

  async meta({ context, res }) {
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

  async getNormalizedQueries(query, context) {
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

  async sql({ query, context, res }) {
    const requestStarted = new Date();

    try {
      query = this.parseQueryParam(query);
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
          coerceForSqlQuery(normalizedQuery, context),
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

  async dryRun({ query, context, res }) {
    const requestStarted = new Date();

    try {
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const sqlQueries = await Promise.all(
        normalizedQueries.map((normalizedQuery) => this.getCompilerApi(context).getSql(
          coerceForSqlQuery(normalizedQuery, context),
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

  async load({ query, context, res, ...props }) {
    const requestStarted = new Date();

    try {
      query = this.parseQueryParam(query);
      this.log(context, {
        type: 'Load Request',
        query
      });
      const [queryType, normalizedQueries] = await this.getNormalizedQueries(query, context);

      const [metaConfigResult, ...sqlQueries] = await Promise.all(
        [
          this.getCompilerApi(context).metaConfig({ requestId: context.requestId })
        ].concat(normalizedQueries.map(
          async (normalizedQuery, index) => {
            const loadRequestSQLStarted = new Date();
            const sqlQuery = await this.getCompilerApi(context).getSql(coerceForSqlQuery(normalizedQuery, context));

            this.log(context, {
              type: 'Load Request SQL',
              duration: this.duration(loadRequestSQLStarted),
              query: normalizedQueries[index],
              sqlQuery
            });

            return sqlQuery;
          }
        ))
      );

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

        const response = await this.getAdapterApi({
          ...context,
          dataSource: sqlQuery.dataSource
        }).executeQuery(toExecute);

        const flattenAnnotation = {
          ...annotation.measures,
          ...annotation.dimensions,
          ...annotation.timeDimensions
        };

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
          annotation
        };
      }));

      this.log(context, {
        type: 'Load Request Success',
        query,
        duration: this.duration(requestStarted)
      });

      if (queryType !== QUERY_TYPE.REGULAR_QUERY && props.queryType == null) {
        throw new UserError(`'${queryType}' query type is not supported by the client. Please update the client.`);
      }

      if (props.queryType === 'multi') {
        res({
          queryType,
          results,
          pivotQuery: getPivotQuery(queryType, normalizedQueries)
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

  async subscribe({
    query, context, res, subscribe, subscriptionState, queryType
  }) {
    const requestStarted = new Date();
    try {
      this.log(context, {
        type: 'Subscribe',
        query
      });
      let result = null;
      let error = null;

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

  resToResultFn(res) {
    return (message, { status } = {}) => (status ? res.status(status).json(message) : res.json(message));
  }

  parseQueryParam(query) {
    if (!query || query === 'undefined') {
      throw new UserError('query param is required');
    }
    if (typeof query === 'string') {
      query = JSON.parse(query);
    }
    return query;
  }

  getCompilerApi(context) {
    if (typeof this.compilerApi === 'function') {
      return this.compilerApi(context);
    }
    return this.compilerApi;
  }

  getAdapterApi(context) {
    if (typeof this.adapterApi === 'function') {
      return this.adapterApi(context);
    }
    return this.adapterApi;
  }

  async contextByReq(req, authInfo, requestId) {
    const extensions = await Promise.resolve(typeof this.extendContext === 'function' ? this.extendContext(req) : {});

    return {
      authInfo,
      requestId,
      ...extensions
    };
  }

  requestIdByReq(req) {
    return req.headers['x-request-id'] || req.headers.traceparent || uuid();
  }

  handleError({
    e, context, query, res, requestStarted
  }) {
    if (e instanceof CubejsHandlerError) {
      this.log(context, {
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      });
      res({ error: e.message }, { status: e.status });
    } else if (e.error === 'Continue wait') {
      this.log(context, {
        type: 'Continue wait',
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      });
      res(e, { status: 200 });
    } else if (e.error) {
      this.log(context, {
        type: 'Orchestrator error',
        query,
        error: e.error,
        duration: this.duration(requestStarted)
      });
      res(e, { status: 400 });
    } else if (e.type === 'UserError') {
      this.log(context, {
        type: e.type,
        query,
        error: e.message,
        duration: this.duration(requestStarted)
      });
      res(
        {
          type: e.type,
          error: e.message
        },
        { status: 400 }
      );
    } else {
      this.log(context, {
        type: 'Internal Server Error',
        query,
        error: e.stack || e.toString(),
        duration: this.duration(requestStarted)
      });
      res({ error: e.toString() }, { status: 500 });
    }
  }

  async defaultCheckAuth(req, auth) {
    if (auth) {
      const secret = this.apiSecret;
      try {
        req.authInfo = jwt.verify(auth, secret);
      } catch (e) {
        if (this.enforceSecurityChecks) {
          throw new UserError('Invalid token');
        } else {
          this.log(req, {
            type: 'Invalid Token',
            token: auth,
            error: e.stack || e.toString()
          });
        }
      }
    } else if (this.enforceSecurityChecks) {
      throw new UserError('Authorization header isn\'t set');
    }
  }

  async checkAuth(req, res, next) {
    const auth = req.headers.authorization;

    try {
      await this.checkAuthFn(req, auth);
      if (next) {
        next();
      }
    } catch (e) {
      if (e instanceof UserError) {
        res.status(403).json({ error: e.message });
      } else {
        this.log(req, {
          type: 'Auth Error',
          token: auth,
          error: e.stack || e.toString()
        });
        res.status(500).json({ error: e.toString() });
      }
    }
  }

  async requestContextMiddleware(req, res, next) {
    req.context = await this.contextByReq(req, req.authInfo, this.requestIdByReq(req));
    if (next) {
      next();
    }
  }

  async requestLogger(req, res, next) {
    const details = requestParser(req, res);
    this.log(req.context, { type: 'REST API Request', ...details });
    if (next) {
      next();
    }
  }

  compareDateRangeTransformer(query) {
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

  log(context, event) {
    const { type, ...restParams } = event;
    this.logger(type, {
      ...restParams,
      authInfo: context.authInfo,
      requestId: context.requestId
    });
  }
}
