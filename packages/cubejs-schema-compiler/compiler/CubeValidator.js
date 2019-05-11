const Joi = require('joi');

const identifierRegex = /^[_a-zA-Z][_a-zA-Z0-9]*$/;
const identifier = Joi.string().regex(/^[_a-zA-Z][_a-zA-Z0-9]*$/, 'identifier');
const timeInterval =
  Joi.alternatives([
    Joi.string().regex(/^(-?\d+) (minute|hour|day|week|month|year)$/, 'time interval'),
    Joi.any().valid('unbounded')
  ]);

const BaseDimensionWithoutSubQuery = {
  aliases: Joi.array().items(Joi.string()),
  type: Joi.any().valid('string', 'number', 'boolean', 'time', 'geo').required(),
  fieldType: Joi.any().valid('string'),
  valuesAsSegments: Joi.boolean(),
  primaryKey: Joi.boolean(),
  shown: Joi.boolean(),
  title: Joi.string(),
  description: Joi.string(),
  suggestFilterValues: Joi.boolean(),
  enableSuggestions: Joi.boolean(),
  format: Joi.alternatives([
    Joi.string().valid('imageUrl', 'link', 'currency', 'percent', 'number', 'id'),
    Joi.object().keys({
      type: Joi.string().valid('link'),
      label: Joi.string().required()
    })
  ])
};

const BaseDimension = Object.assign({
  subQuery: Joi.boolean()
}, BaseDimensionWithoutSubQuery);

const BaseMeasure = {
  aliases: Joi.array().items(Joi.string()),
  format: Joi.any().valid('percent', 'currency', 'number'),
  shown: Joi.boolean(),
  visible: Joi.boolean(),
  cumulative: Joi.boolean(),
  filters: Joi.array().items(
    Joi.object().keys({
      sql: Joi.func().required()
    })
  ),
  title: Joi.string(),
  description: Joi.string(),
  rollingWindow: Joi.object().keys({
    trailing: timeInterval,
    leading: timeInterval,
    offset: Joi.any().valid('start', 'end')
  }),
  drillMemberReferences: Joi.func(),
  drillMembers: Joi.func(),
  drillFilters: Joi.array().items(
    Joi.object().keys({
      sql: Joi.func().required()
    })
  ),
};

const BasePreAggregation = {
  refreshKey: Joi.object().keys({
    sql: Joi.func().required()
  }),
  useOriginalSqlPreAggregations: Joi.boolean(),
  external: Joi.boolean(),
  partitionGranularity: Joi.any().valid('day', 'week', 'month', 'year')
};

const cubeSchema = Joi.object().keys({
  name: identifier,
  sql: Joi.func().required(),
  refreshKey: Joi.object().keys({
    sql: Joi.func().required()
  }),
  fileName: Joi.string().required(),
  extends: Joi.func(),
  allDefinitions: Joi.func(),
  title: Joi.string(),
  sqlAlias: Joi.string(),
  description: Joi.string(),
  joins: Joi.object().pattern(identifierRegex, Joi.object().keys({
    sql: Joi.func().required(),
    relationship: Joi.any().valid('hasMany', 'belongsTo', 'hasOne').required()
  })),
  measures: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys(
      Object.assign({}, BaseMeasure, {
        sql: Joi.func(),
        type: Joi.any().valid('count').required()
      })
    ),
    Joi.object().keys(
      Object.assign({}, BaseMeasure, {
        sql: Joi.func().required(),
        type: Joi.any().valid(
          'number', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
        ).required()
      })
    )
  )),
  dimensions: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys(
      Object.assign({}, BaseDimensionWithoutSubQuery, {
        case: Joi.object().keys({
          when: Joi.array().items(Joi.object().keys({
            sql: Joi.func().required(),
            label: Joi.string()
          })),
          else: Joi.object().keys({
            label: Joi.string()
          })
        }).required()
      })
    ),
    Joi.object().keys(
      Object.assign({}, BaseDimensionWithoutSubQuery, {
        latitude: Joi.object().keys({
          sql: Joi.func().required()
        }),
        longitude: Joi.object().keys({
          sql: Joi.func().required()
        })
      })
    ),
    Joi.object().keys(
      Object.assign({}, BaseDimension, {
        sql: Joi.func().required()
      })
    )
  )),
  segments: Joi.object().pattern(identifierRegex, Joi.object().keys({
    aliases: Joi.array().items(Joi.string()),
    sql: Joi.func().required(),
    title: Joi.string(),
    description: Joi.string()
  })),
  preAggregations: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys(Object.assign(BasePreAggregation, {
      type: Joi.any().valid('autoRollup').required(),
      maxPreAggregations: Joi.number()
    })),
    Joi.object().keys(Object.assign(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required()
    })),
    Joi.object().keys(Object.assign(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func()
    })),
    Joi.object().keys(Object.assign(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
      timeDimensionReference: Joi.func().required(),
      granularity: Joi.any().valid('hour', 'day', 'week', 'month', 'year').required()
    }))
  ))
});

class CubeValidator {
  constructor(cubeSymbols) {
    this.cubeSymbols = cubeSymbols;
    this.validCubes = {};
  }

  compile(cubes, errorReporter) {
    return this.cubeSymbols.cubeList.map((v) =>
        this.validate(this.cubeSymbols.getCubeDefinition(v.name), errorReporter.inContext(`${v.name} cube`))
    );
  }

  validate(cube, errorReporter) {
    Joi.validate(cube, cubeSchema, (err) => {
      if (err) {
        errorReporter.error(err.message);
      } else {
        this.validCubes[cube.name] = true;
      }
    });
  }

  isCubeValid(cube) {
    return this.validCubes[cube.name];
  }
}

module.exports = CubeValidator;
