const Joi = require('@hapi/joi');
const cronParser = require('cron-parser');

const identifierRegex = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

const identifier = Joi.string().regex(identifierRegex, 'identifier');

const regexTimeInterval = Joi.string().custom((value, helper) => {
  if (value.match(/^(-?\d+) (minute|hour|day|week|month|year)$/)) {
    return value;
  } else {
    return helper.message({ custom: `"${helper.state.path.join('.')}" does not match regexp: /^(-?\\d+) (minute|hour|day|week|month|year)$/` });
  }
});

const timeInterval =
  Joi.alternatives([
    regexTimeInterval,
    Joi.any().valid('unbounded')
  ]);

const everyInterval = Joi.string().custom((value, helper) => {
  if (value.match(/^(\d+) (second|minute|hour|day|week)s?$/)) {
    return value;
  } else {
    return helper.message({ custom: `"${helper.state.path.join('.')}" does not match regexp: /^(\\d+) (second|minute|hour|day|week)s?$/` });
  }
});

const everyCronInterval = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression(value);
    return value;
  } catch (e) {
    return helper.message({ custom: `"${helper.state.path.join('.')}" CronParser: ${e.toString()}` });
  }
});

const everyCronTimeZone = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression('0 * * * *', { currentDate: '2020-01-01 00:00:01', tz: value });
    return value;
  } catch (e) {
    return helper.message({ custom: `"${helper.state.path.join('.')}" unknown timezone. Take a look here https://www.npmjs.com/package/cron-parser to get available time zones` });
  }
});

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
      updateWindow: everyInterval
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
  partitionGranularity: Joi.any().valid('hour', 'day', 'week', 'month', 'quarter', 'year'),
};

const PreAggregationsAlternatives = Joi.object().pattern(identifierRegex, Joi.alternatives().try(
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
));

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
      every: Joi.alternatives().try(everyInterval, everyCronInterval).required(),
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
  preAggregations: PreAggregationsAlternatives
});

function formatErrorMessageFromDetails(explain, d) {
  if (d?.context?.details) {
    d?.context?.details?.forEach((d2) => formatErrorMessageFromDetails(explain, d2));
  } else if (d?.message) {
    let msg = d?.context?.message || d?.message;
    msg = msg?.replace(`"${d.context?.label}"`, `(${d.context?.label} = ${d.context?.value})`);
    explain.add(msg);
  }
}

const excludedErrorMessages = new Set();

function fillExcludedErrorMessages(d) {
  d?.context?.valids?.forEach((v) => {
    excludedErrorMessages.add(`${d?.context?.label} = ${v}`);
  });
  d?.context?.details?.forEach((dd) => fillExcludedErrorMessages(dd));
}

PreAggregationsAlternatives.validate({ eventsByType: { type: 'unknown' } })?.error?.details?.forEach(
  (d) => fillExcludedErrorMessages(d)
);

function formatErrorMessage(error) {
  const explain = new Set();

  error?.details?.forEach((d) => formatErrorMessageFromDetails(explain, d));

  excludedErrorMessages.forEach((e) => {
    explain.forEach((m) => {
      if (m.indexOf(e) >= 0) explain.delete(m);
    });
  });

  let { message } = error;

  if (explain.size > 0) {
    message += `\nPossible reasons:\n\t* ${Array.from(explain).join('\n\t* ')}`;
  }

  return message;
}

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
    const result = cubeSchema.validate(cube);

    if (result.error != null) {
      errorReporter.error(formatErrorMessage(result.error), result.error);
    } else {
      this.validCubes[cube.name] = true;
    }

    return result;
  }

  isCubeValid(cube) {
    return this.validCubes[cube.name];
  }
}
