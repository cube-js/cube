const jwt = require('jsonwebtoken');
const R = require('ramda');
const Joi = require('@hapi/joi');
const moment = require('moment');
const uuid = require('uuid/v4');

const dateParser = require('./dateParser');
const UserError = require('./UserError');
const CubejsHandlerError = require('./CubejsHandlerError');
const SubscriptionServer = require('./SubscriptionServer');
const LocalSubscriptionStore = require('./LocalSubscriptionStore');

const toConfigMap = (metaConfig) => (
  R.pipe(
    R.map(c => [c.config.name, c.config]),
    R.fromPairs
  )(metaConfig)
);

const prepareAnnotation = (metaConfig, query) => {
  const configMap = toConfigMap(metaConfig);

  const annotation = (memberType) => (member) => {
    const path = member.split('.');
    const config = configMap[path[0]][memberType].find(m => m.name === member);
    if (!config) {
      return undefined;
    }
    return [member, {
      title: config.title,
      shortTitle: config.shortTitle,
      description: config.description,
      type: config.type,
      format: config.format
    }];
  };

  return {
    measures: R.fromPairs((query.measures || []).map(annotation('measures')).filter(a => !!a)),
    dimensions: R.fromPairs((query.dimensions || []).map(annotation('dimensions')).filter(a => !!a)),
    segments: R.fromPairs((query.segments || []).map(annotation('segments')).filter(a => !!a)),
    timeDimensions: R.fromPairs((query.timeDimensions || []).map(td => annotation('dimensions')(td.dimension)).filter(a => !!a)), // TODO
  };
};

const transformValue = (value, type) => {
  if (value && type === 'time') {
    return (value instanceof Date ? moment(value) : moment.utc(value)).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  return value && value.value ? value.value : value; // TODO move to sql adapter
};


const transformData = (aliasToMemberNameMap, annotation, data) => (data.map(r => R.pipe(
  R.toPairs,
  R.map(p => {
    const memberName = aliasToMemberNameMap[p[0]];
    const annotationForMember = annotation[memberName];
    if (!annotationForMember) {
      throw new UserError(`You requested hidden member: '${p[0]}'. Please make it visible using \`shown: true\``);
    }
    return [
      memberName,
      transformValue(p[1], annotationForMember.type)
    ];
  }),
  R.fromPairs
)(r)));

const id = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);
const dimensionWithTime = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(hour|day|week|month|year))?$/);

const operators = [
  'equals',
  'notEquals',
  'contains',
  'notContains',
  'in',
  'notIn',
  'gt',
  'gte',
  'lt',
  'lte',
  'set',
  'notSet',
  'inDateRange',
  'notInDateRange',
  'onTheDate',
  'beforeDate',
  'afterDate'
];

const querySchema = Joi.object().keys({
  measures: Joi.array().items(id),
  dimensions: Joi.array().items(dimensionWithTime),
  filters: Joi.array().items(Joi.object().keys({
    dimension: id,
    member: id,
    operator: Joi.valid(operators).required(),
    values: Joi.array().items(Joi.string().allow(''))
  }).xor('dimension', 'member')),
  timeDimensions: Joi.array().items(Joi.object().keys({
    dimension: id.required(),
    granularity: Joi.valid('day', 'month', 'year', 'week', 'hour', 'minute', 'second', null),
    dateRange: [
      Joi.array().items(Joi.string()).min(1).max(2),
      Joi.string()
    ]
  })),
  order: Joi.object().pattern(id, Joi.valid('asc', 'desc')),
  segments: Joi.array().items(id),
  timezone: Joi.string(),
  limit: Joi.number().integer().min(1).max(50000),
  offset: Joi.number().integer().min(0),
  renewQuery: Joi.boolean(),
  ungrouped: Joi.boolean()
});

const normalizeQuery = (query) => {
  // eslint-disable-next-line no-unused-vars
  const { error, value } = Joi.validate(query, querySchema);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }
  const validQuery = query.measures && query.measures.length ||
    query.dimensions && query.dimensions.length ||
    query.timeDimensions && query.timeDimensions.filter(td => !!td.granularity).length;
  if (!validQuery) {
    throw new UserError(
      'Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid'
    );
  }
  const filterWithoutOperator = (query.filters || []).find(f => !f.operator);
  if (filterWithoutOperator) {
    throw new UserError(`Operator required for filter: ${JSON.stringify(filterWithoutOperator)}`);
  }
  const filterWithIncorrectOperator = (query.filters || [])
    .find(f => [
      'equals',
      'notEquals',
      'contains',
      'notContains',
      'in',
      'notIn',
      'gt',
      'gte',
      'lt',
      'lte',
      'set',
      'notSet',
      'inDateRange',
      'notInDateRange',
      'onTheDate',
      'beforeDate',
      'afterDate'
    ].indexOf(f.operator) === -1);
  if (filterWithIncorrectOperator) {
    throw new UserError(`Operator ${filterWithIncorrectOperator.operator} not supported for filter: ${JSON.stringify(filterWithIncorrectOperator)}`);
  }
  const filterWithoutValues = (query.filters || [])
    .find(f => !f.values && ['set', 'notSet'].indexOf(f.operator) === -1);
  if (filterWithoutValues) {
    throw new UserError(`Values required for filter: ${JSON.stringify(filterWithoutValues)}`);
  }
  const regularToTimeDimension = (query.dimensions || []).filter(d => d.split('.').length === 3).map(d => ({
    dimension: d.split('.').slice(0, 2).join('.'),
    granularity: d.split('.')[2]
  }));
  const timezone = query.timezone || 'UTC';
  const order = query.order && Object.keys(query.order).map(k => ({
    id: k,
    desc: query.order[k] === 'desc'
  }));
  return {
    ...query,
    rowLimit: query.rowLimit || query.limit,
    timezone,
    order,
    filters: (query.filters || []).map(f => (
      {
        ...f,
        dimension: (f.dimension || f.member)
      }
    )),
    dimensions: (query.dimensions || []).filter(d => d.split('.').length !== 3),
    timeDimensions: (query.timeDimensions || []).map(td => {
      let dateRange;
      if (typeof td.dateRange === 'string') {
        dateRange = dateParser(td.dateRange, timezone);
      } else {
        dateRange = td.dateRange && td.dateRange.length === 1 ? [td.dateRange[0], td.dateRange[0]] : td.dateRange;
      }
      return {
        ...td,
        dateRange
      };
    }).concat(regularToTimeDimension)
  };
};

const coerceForSqlQuery = (query, context) => ({
  ...query,
  timeDimensions: query.timeDimensions || [],
  contextSymbols: {
    userContext: context.authInfo && context.authInfo.u || {}
  }
});

class ApiGateway {
  constructor(apiSecret, compilerApi, adapterApi, logger, options) {
    options = options || {};
    this.apiSecret = apiSecret;
    this.compilerApi = compilerApi;
    this.adapterApi = adapterApi;
    this.logger = logger;
    this.checkAuthMiddleware = options.checkAuthMiddleware || this.checkAuth.bind(this);
    this.checkAuthFn = options.checkAuth || this.defaultCheckAuth.bind(this);
    this.basePath = options.basePath || '/cubejs-api';
    // eslint-disable-next-line no-unused-vars
    this.queryTransformer = options.queryTransformer || (async (query, context) => query);
    this.subscriptionStore = options.subscriptionStore || new LocalSubscriptionStore();
    this.enforceSecurityChecks = options.enforceSecurityChecks || (process.env.NODE_ENV === 'production');
    this.extendContext = options.extendContext;
  }

  initApp(app) {
    app.get(`${this.basePath}/v1/load`, this.checkAuthMiddleware, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: await this.contextByReq(req, req.authInfo, this.requestIdByReq(req)),
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/subscribe`, this.checkAuthMiddleware, (async (req, res) => {
      await this.load({
        query: req.query.query,
        context: await this.contextByReq(req, req.authInfo, this.requestIdByReq(req)),
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/sql`, this.checkAuthMiddleware, (async (req, res) => {
      await this.sql({
        query: req.query.query,
        context: await this.contextByReq(req, req.authInfo, this.requestIdByReq(req)),
        res: this.resToResultFn(res)
      });
    }));

    app.get(`${this.basePath}/v1/meta`, this.checkAuthMiddleware, (async (req, res) => {
      await this.meta({
        context: await this.contextByReq(req, req.authInfo, this.requestIdByReq(req)),
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

  async meta({ context, res }) {
    const requestStarted = new Date();
    try {
      const metaConfig = await this.getCompilerApi(context).metaConfig();
      const cubes = metaConfig.map(c => c.config);
      res({ cubes });
    } catch (e) {
      this.handleError({
        e, context, res, requestStarted
      });
    }
  }

  async sql({
    query, context, res
  }) {
    const requestStarted = new Date();
    try {
      query = this.parseQueryParam(query);
      const normalizedQuery = await this.queryTransformer(normalizeQuery(query), context);
      const sqlQuery = await this.getCompilerApi(context).getSql(
        coerceForSqlQuery(normalizedQuery, context),
        { includeDebugInfo: process.env.NODE_ENV !== 'production' }
      );
      res({
        sql: sqlQuery
      });
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  async load({
    query, context, res
  }) {
    const requestStarted = new Date();
    try {
      query = this.parseQueryParam(query);
      this.log(context, {
        type: 'Load Request',
        query
      });
      const normalizedQuery = await this.queryTransformer(normalizeQuery(query), context);
      const [compilerSqlResult, metaConfigResult] = await Promise.all([
        this.getCompilerApi(context).getSql(coerceForSqlQuery(normalizedQuery, context)),
        this.getCompilerApi(context).metaConfig()
      ]);
      const sqlQuery = compilerSqlResult;
      this.log(context, {
        type: 'Load Request SQL',
        query,
        sqlQuery
      });
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
        ...context, dataSource: sqlQuery.dataSource
      }).executeQuery(toExecute);
      this.log(context, {
        type: 'Load Request Success',
        query,
        duration: this.duration(requestStarted)
      });
      const flattenAnnotation = {
        ...annotation.measures,
        ...annotation.dimensions,
        ...annotation.timeDimensions
      };
      res({
        query: normalizedQuery,
        data: transformData(aliasToMemberNameMap, flattenAnnotation, response.data),
        lastRefreshTime: response.lastRefreshTime && response.lastRefreshTime.toISOString(),
        ...(process.env.NODE_ENV === 'production' ? undefined : {
          refreshKeyValues: response.refreshKeyValues,
          usedPreAggregations: response.usedPreAggregations
        }),
        annotation
      });
    } catch (e) {
      this.handleError({
        e, context, query, res, requestStarted
      });
    }
  }

  async subscribe({
    query, context, res, subscribe, subscriptionState
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
        await this.load({ query, context, res });
        return;
      }

      // TODO subscribe to refreshKeys instead of constantly firing load
      await this.load({
        query,
        context: { ...context, requestId: `${context.requestId}-${uuid()}` },
        res: (message, opts) => {
          if (message.error) {
            error = { message, opts };
          } else {
            result = { message, opts };
          }
        }
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
      throw new UserError(`query param is required`);
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
      throw new UserError("Authorization header isn't set");
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

  log(context, event) {
    const { type, ...restParams } = event;
    this.logger(type, { ...restParams, ...context });
  }
}

module.exports = ApiGateway;
