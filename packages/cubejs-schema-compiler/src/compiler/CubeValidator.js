const Joi = require('@hapi/joi');

const identifierRegex = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

const identifier = Joi.string().regex(identifierRegex, 'identifier');
const timeInterval =
  Joi.alternatives([
    Joi.string().regex(/^(-?\d+) (minute|hour|day|week|month|year)$/, 'time interval'),
    Joi.any().valid('unbounded')
  ]);
const everyInterval = Joi.string().regex(/^(\d+) (second|minute|hour|day|week)s?$/, 'refresh time interval');
const everyCronInterval = Joi.string();
const everyCronTimeZone = Joi.string();

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
  ]),
  meta: Joi.any()
};

const BaseDimension = Object.assign({
  subQuery: Joi.boolean(),
  propagateFiltersToSubQuery: Joi.boolean()
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
  meta: Joi.any()
};

const BasePreAggregationWithoutPartitionGranularity = {
  refreshKey: Joi.alternatives().try(
    Joi.object().keys({
      sql: Joi.func().required(),
      // We dont support timezone for this, because it's useless
      // We cannot support cron interval
      every: Joi.alternatives().try(everyInterval),
    }),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval),
      timezone: everyCronTimeZone,
      incremental: Joi.boolean(),
      updateWindow: timeInterval
    })
  ),
  sqlAlias: Joi.string().optional(),
  useOriginalSqlPreAggregations: Joi.boolean(),
  external: Joi.boolean(),
  scheduledRefresh: Joi.boolean(),
  indexes: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys({
      sql: Joi.func().required()
    }),
    Joi.object().keys({
      columns: Joi.func().required()
    })
  )),
  // refreshRange was deprecated
  refreshRangeStart: {
    sql: Joi.func().required()
  },
  refreshRangeEnd: {
    sql: Joi.func().required()
  },
  // new api
  buildRangeStart: {
    sql: Joi.func().required()
  },
  buildRangeEnd: {
    sql: Joi.func().required()
  },
};

const BasePreAggregation = {
  ...BasePreAggregationWithoutPartitionGranularity,
  partitionGranularity: Joi.any().valid('hour', 'day', 'week', 'month', 'year'),
};

const cubeSchema = Joi.object().keys({
  name: identifier,
  sql: Joi.func().required(),
  refreshKey: Joi.alternatives().try(
    Joi.object().keys({
      sql: Joi.func().required(),
      // We dont support timezone for this, because it's useless
      // We cannot support cron interval
      every: Joi.alternatives().try(everyInterval),
    }),
    Joi.object().keys({
      immutable: Joi.boolean().required()
    }),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval),
      timezone: everyCronTimeZone,
    })
  ),
  fileName: Joi.string().required(),
  extends: Joi.func(),
  allDefinitions: Joi.func(),
  title: Joi.string(),
  sqlAlias: Joi.string(),
  dataSource: Joi.string(),
  description: Joi.string(),
  rewriteQueries: Joi.boolean(),
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
            label: Joi.alternatives([
              Joi.string(),
              Joi.object().keys({
                sql: Joi.func().required()
              })
            ])
          })),
          else: Joi.object().keys({
            label: Joi.alternatives([
              Joi.string(),
              Joi.object().keys({
                sql: Joi.func().required()
              })
            ])
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
    description: Joi.string(),
    meta: Joi.any()
  })),
  preAggregations: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('autoRollup').required(),
      maxPreAggregations: Joi.number(),
    })),
    // OriginalSQL partitioning with references
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('originalSql').required(),
      timeDimensionReference: Joi.func().required(),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
    })),
    // OriginalSQL partitioning without references
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('originalSql').required(),
      timeDimension: Joi.func().required(),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
    })),
    Joi.object().keys(Object.assign({}, BasePreAggregationWithoutPartitionGranularity, {
      type: Joi.any().valid('originalSql').required(),
    })),
    // RollupJoin with references
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
      rollupReferences: Joi.func().required(),
    })),
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
      timeDimensionReference: Joi.func().required(),
      granularity: Joi.any().valid(
        'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
      ).required(),
      rollupReferences: Joi.func().required(),
    })),
    // RollupJoin without references
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
      rollups: Joi.func().required(),
    })),
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
      timeDimension: Joi.func().required(),
      granularity: Joi.any().valid(
        'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
      ).required(),
      rollups: Joi.func().required(),
    })),
    // Rollup with references
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func()
    })),
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
      timeDimensionReference: Joi.func().required(),
      granularity: Joi.any().valid(
        'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
      ).required()
    })),
    // Rollup without References postfix
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func()
    })),
    Joi.object().keys(Object.assign({}, BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
      timeDimension: Joi.func().required(),
      granularity: Joi.any().valid(
        'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
      ).required()
    }))
  ))
});

export class CubeValidator {
  constructor(cubeSymbols) {
    this.cubeSymbols = cubeSymbols;
    this.validCubes = {};
  }

  compile(cubes, errorReporter) {
    return this.cubeSymbols.cubeList.map(
      (v) => this.validate(this.cubeSymbols.getCubeDefinition(v.name), errorReporter.inContext(`${v.name} cube`))
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
