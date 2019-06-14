const jwt = require('jsonwebtoken');
const R = require('ramda');
const Joi = require('joi');
const moment = require('moment');
const dateParser = require('./dateParser');

const UserError = require('./UserError');

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

const prepareAliasToMemberNameMap = (metaConfig, sqlQuery, query) => {
  const configMap = toConfigMap(metaConfig);

  const lookupAlias = (memberType) => (member) => {
    const path = member.split('.');
    const config = configMap[path[0]][memberType].find(m => m.name === member);
    if (!config) {
      return undefined;
    }
    return [config.aliasName, member];
  };

  return R.fromPairs(
    (query.measures || []).map(lookupAlias('measures'))
      .concat((query.dimensions || []).map(lookupAlias('dimensions')))
      .concat((query.segments || []).map(lookupAlias('segments')))
      .concat((query.timeDimensions || []).map(td => lookupAlias('dimensions')(td.dimension)))
      .concat(sqlQuery.timeDimensionAlias ? [[sqlQuery.timeDimensionAlias, sqlQuery.timeDimensionField]] : [])
      .filter(a => !!a)
  );
};

const transformValue = (value, type) => {
  if (value && type === 'time') {
    return moment(value).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
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
    granularity: Joi.valid('day', 'month', 'year', 'week', 'hour', null),
    dateRange: [
      Joi.array().items(Joi.string()).min(1).max(2),
      Joi.string()
    ]
  })),
  order: Joi.object().pattern(id, Joi.valid('asc', 'desc')),
  segments: Joi.array().items(id),
  timezone: Joi.string(),
  limit: Joi.number().integer().min(1).max(50000),
  renewQuery: Joi.boolean()
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
        dateRange = dateParser(td.dateRange);
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

const coerceForSqlQuery = (query, req) => ({
  ...query,
  timeDimensions: (query.timeDimensions || [])
    .map(td => (td.granularity === 'day' ? { ...td, granularity: 'date' } : td)),
  contextSymbols: {
    userContext: req.authInfo && req.authInfo.u || {}
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
    this.basePath = options.basePath || '/cubejs-api';
  }

  initApp(app) {
    app.get(`${this.basePath}/v1/load`, this.checkAuthMiddleware, (async (req, res) => {
      try {
        if (!req.query.query || req.query.query === 'undefined') {
          throw new UserError(`query param is required`);
        }
        const query = JSON.parse(req.query.query);
        this.log(req, {
          type: 'Load Request',
          query: req.query.query
        });
        const normalizedQuery = normalizeQuery(query);
        const [compilerSqlResult, metaConfigResult] = await Promise.all([
          this.getCompilerApi(req).getSql(coerceForSqlQuery(normalizedQuery, req)),
          this.getCompilerApi(req).metaConfig()
        ]);
        const sqlQuery = compilerSqlResult;
        const metaConfig = metaConfigResult;
        const annotation = prepareAnnotation(metaConfig, normalizedQuery);
        const aliasToMemberNameMap = prepareAliasToMemberNameMap(metaConfig, sqlQuery, normalizedQuery);
        const toExecute = {
          ...sqlQuery,
          query: sqlQuery.sql[0],
          values: sqlQuery.sql[1],
          continueWait: true,
          renewQuery: normalizedQuery.renewQuery
        };
        const response = await this.getAdapterApi(req).executeQuery(toExecute);
        this.log(req, {
          type: 'Load Request Success',
          query: req.query.query,
        });
        const flattenAnnotation = {
          ...annotation.measures,
          ...annotation.dimensions,
          ...annotation.timeDimensions
        };
        res.json({
          query: normalizedQuery,
          data: transformData(aliasToMemberNameMap, flattenAnnotation, response.data),
          annotation
        });
      } catch (e) {
        this.handleError(e, req, res);
      }
    }));

    app.get(`${this.basePath}/v1/sql`, this.checkAuthMiddleware, (async (req, res) => {
      try {
        if (!req.query.query || req.query.query === 'undefined') {
          throw new UserError(`query param is required`);
        }
        const query = JSON.parse(req.query.query);
        const normalizedQuery = normalizeQuery(query);
        const sqlQuery = await this.getCompilerApi(req).getSql(coerceForSqlQuery(normalizedQuery, req));
        res.json({
          sql: sqlQuery
        });
      } catch (e) {
        this.handleError(e, req, res);
      }
    }));

    app.get(`${this.basePath}/v1/meta`, this.checkAuthMiddleware, (async (req, res) => {
      try {
        const metaConfig = await this.getCompilerApi(req).metaConfig();
        const cubes = metaConfig.map(c => c.config);
        res.json({ cubes });
      } catch (e) {
        this.handleError(e, req, res);
      }
    }));
  }

  getCompilerApi(req) {
    if (typeof this.compilerApi === 'function') {
      return this.compilerApi(this.contextByReq(req));
    }
    return this.compilerApi;
  }

  getAdapterApi(req) {
    if (typeof this.adapterApi === 'function') {
      return this.adapterApi(this.contextByReq(req));
    }
    return this.adapterApi;
  }

  contextByReq(req) {
    return { authInfo: req.authInfo };
  }

  handleError(e, req, res) {
    if (e instanceof UserError) {
      this.log(req, {
        type: 'User Error',
        query: req.query && req.query.query,
        error: e.message
      });
      res.status(400).json({ error: e.message });
    } else if (e.error === 'Continue wait') {
      this.log(req, {
        type: 'Continue wait',
        query: req.query && req.query.query,
        error: e.message
      });
      res.status(200).json(e);
    } else if (e.error) {
      this.log(req, {
        type: 'Orchestrator error',
        query: req.query && req.query.query,
        error: e.error
      });
      res.status(400).json(e);
    } else {
      this.log(req, {
        type: 'Internal Server Error',
        query: req.query && req.query.query,
        error: e.stack || e.toString()
      });
      res.status(500).json({ error: e.toString() });
    }
  }

  async checkAuth(req, res, next) {
    const auth = req.headers.authorization;

    if (auth) {
      const secret = this.apiSecret;
      try {
        req.authInfo = jwt.verify(auth, secret);
        return next && next();
      } catch (e) {
        if (process.env.NODE_ENV === 'production') {
          res.status(403).json({ error: 'Invalid token' });
        } else {
          this.log(req, {
            type: 'Invalid Token',
            token: auth,
            error: e.stack || e.toString()
          });
          return next && next();
        }
      }
    } else if (process.env.NODE_ENV === 'production') {
      res.status(403).send({ error: "Authorization header isn't set" });
    } else {
      return next && next();
    }
    return null;
  }

  log(req, event) {
    const { type, ...restParams } = event;
    this.logger(type, { ...restParams, authInfo: req.authInfo });
  }
}

module.exports = ApiGateway;
