const jwt = require('jsonwebtoken');
const R = require('ramda');
const Joi = require('joi');
const moment = require('moment');

class UserError extends Error {}

const toConfigMap = (metaConfig) => {
  return R.pipe(
    R.map(c => [c.config.name, c.config]),
    R.fromPairs
  )(metaConfig);
};

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
    }]
  };

  return {
    measures: R.fromPairs((query.measures || []).map(annotation('measures')).filter(a => !!a)),
    dimensions: R.fromPairs((query.dimensions || []).map(annotation('dimensions')).filter(a => !!a)),
    segments: R.fromPairs((query.segments || []).map(annotation('segments')).filter(a => !!a)),
    timeDimensions: R.fromPairs((query.timeDimensions || []).map(td => annotation('dimensions')(td.dimension)).filter(a => !!a)), // TODO
  }
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
  )
};

const transformValue = (value, type) => {
  if (value && type === 'time') {
    return moment(value).format(moment.HTML5_FMT.DATETIME_LOCAL_MS);
  }
  return value && value.value ? value.value : value; // TODO move to sql adapter
};


const transformData = (aliasToMemberNameMap, annotation, data) => {
  return data.map(r => R.pipe(
    R.toPairs,
    R.map(p => [
      aliasToMemberNameMap[p[0]],
      transformValue(p[1], annotation[aliasToMemberNameMap[p[0]]].type)
    ]),
    R.fromPairs
  )(r));
};

const id = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/);
const dimensionWithTime = Joi.string().regex(/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+(\.(hour|day|week|month))?$/);

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
    dimension: id.required(),
    operator: Joi.valid(operators).required(),
    values: Joi.array().items(Joi.string())
  })),
  timeDimensions: Joi.array().items(Joi.object().keys({
    dimension: id.required(),
    granularity: Joi.valid('day', 'month', 'week', 'hour', null),
    dateRange: Joi.array().items(Joi.string()).min(1).max(2)
  })),
  segments: Joi.array().items(id),
  timezone: Joi.string(),
  limit: Joi.number().integer().min(1).max(50000)
});

const normalizeQuery = (query) => {
  const { error, value } = Joi.validate(query, querySchema);
  if (error) {
    throw new UserError(`Invalid query format: ${error.message || error.toString()}`);
  }
  const filterWithoutOperator = (query.filters || []).find(f => !f.operator);
  if (filterWithoutOperator) {
    throw new UserError(`Operator required for filter: ${JSON.stringify(filterWithoutOperator)}`);
  }
  const filterWithIncorrectOperator = (query.filters || [])
    .find(f =>
      [
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
      ].indexOf(f.operator) === -1
    );
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
  return {
    ...query,
    rowLimit: query.rowLimit || query.limit,
    timezone: query.timezone || 'UTC', // TODO get from bot
    dimensions: (query.dimensions || []).filter(d => d.split('.').length !== 3),
    timeDimensions: (query.timeDimensions || []).map(td => ({
      ...td,
      dateRange: td.dateRange && td.dateRange.length === 1 ? [td.dateRange[0], td.dateRange[0]] : td.dateRange
    })).concat(regularToTimeDimension)
  }
};

const coerceForSqlQuery = (query) => {
  return {
    ...query,
    timeDimensions: (query.timeDimensions || []).map(td => {
      return td.granularity === 'day' ? { ...td, granularity: 'date' } : td;
    })
  }
};

class ApiGateway {
  constructor(apiSecret, compilerApi, adapterApi, logger) {
    this.apiSecret = apiSecret;
    this.compilerApi = compilerApi;
    this.adapterApi = adapterApi;
    this.logger = logger;
  }

  initApp(app) {
    app.get('/cubejs-api/v1/load', this.checkAuth.bind(this), (async (req, res) => {
      try {
        let query = JSON.parse(req.query.query);
        this.log(req, {
          type: 'Load Request',
          query: req.query.query
        });
        const normalizedQuery = normalizeQuery(query);
        const [compilerSqlResult, metaConfigResult] = await Promise.all([
          this.compilerApi.getSql(coerceForSqlQuery(normalizedQuery)),
          this.compilerApi.metaConfig()
        ]);
        const sqlQuery = compilerSqlResult;
        let metaConfig = metaConfigResult;
        const annotation = prepareAnnotation(metaConfig, normalizedQuery);
        let aliasToMemberNameMap = prepareAliasToMemberNameMap(metaConfig, sqlQuery, normalizedQuery);
        const toExecute = { ...sqlQuery, query: sqlQuery.sql[0], values: sqlQuery.sql[1], continueWait: true };
        const response = await this.adapterApi.executeQuery(toExecute);
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

    app.get('/cubejs-api/v1/sql', this.checkAuth.bind(this), (async (req, res) => {
      try {
        let query = JSON.parse(req.query.query);
        const normalizedQuery = normalizeQuery(query);
        const sqlQuery = await this.compilerApi.getSql(coerceForSqlQuery(normalizedQuery));
        res.json({
          sql: sqlQuery
        });
      } catch (e) {
        this.handleError(e, req, res);
      }
    }));
  }

  handleError (e, req, res) {
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
    const auth = req.headers['authorization'];

    if (auth) {
      const secret = this.apiSecret;
      try {
        req.authInfo = jwt.verify(auth, secret);
        return next && next();
      } catch (e) {
        res.status(403).json({ error: 'Invalid token' });
      }
    } else {
      res.status(403).send({ error: "Authorization header isn't set" });
    }
  }

  log(req, event) {
    const { type, ...restParams } = event;
    this.logger(type, { ...restParams, authInfo: req.authInfo });
  }

}

module.exports = ApiGateway;