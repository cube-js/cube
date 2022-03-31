const Joi = require('@hapi/joi');
const cronParser = require('cron-parser');

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

const identifierRegex = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

const identifier = Joi.string().regex(identifierRegex, 'identifier');

const regexTimeInterval = Joi.string().custom((value, helper) => {
  if (value.match(/^(-?\d+) (minute|hour|day|week|month|year)$/)) {
    return value;
  } else {
    return helper.message({ custom: `(${helper.state.path.join('.')} = ${value}) does not match regexp: /^(-?\\d+) (minute|hour|day|week|month|year)$/` });
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
    return helper.message({ custom: `(${helper.state.path.join('.')} = ${value}) does not match regexp: /^(\\d+) (second|minute|hour|day|week)s?$/` });
  }
});

const everyCronInterval = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression(value);
    return value;
  } catch (e) {
    return helper.message({ custom: `(${helper.state.path.join('.')} = ${value}) CronParser: ${e.toString()}` });
  }
});

const everyCronTimeZone = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression('0 * * * *', { currentDate: '2020-01-01 00:00:01', tz: value });
    return value;
  } catch (e) {
    return helper.message({ custom: `(${helper.state.path.join('.')} = ${value}) unknown timezone. Take a look here https://cube.dev/docs/schema/reference/cube#supported-timezones to get available time zones` });
  }
});

const BaseDimensionWithoutSubQuery = {
  aliases: Joi.array().items(Joi.string()),
  type: Joi.any().valid('string', 'number', 'boolean', 'time', 'geo').required(),
  fieldType: Joi.any().valid('string'),
  valuesAsSegments: Joi.boolean().strict(),
  primaryKey: Joi.boolean().strict(),
  shown: Joi.boolean().strict(),
  title: Joi.string(),
  description: Joi.string(),
  suggestFilterValues: Joi.boolean().strict(),
  enableSuggestions: Joi.boolean().strict(),
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
  subQuery: Joi.boolean().strict(),
  propagateFiltersToSubQuery: Joi.boolean().strict()
}, BaseDimensionWithoutSubQuery);

const BaseMeasure = {
  aliases: Joi.array().items(Joi.string()),
  format: Joi.any().valid('percent', 'currency', 'number'),
  shown: Joi.boolean().strict(),
  visible: Joi.boolean().strict(),
  cumulative: Joi.boolean().strict(),
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

function condition(fun, then, otherwise) {
  return Joi.alternatives().conditional(
    Joi.ref('.'), {
      is: Joi.custom((value, helper) => (fun(value) ? value : helper.message({}))),
      then,
      otherwise
    }
  );
}

function defined(a) {
  return typeof a !== 'undefined';
}

function inherit(a, b) {
  return Joi.object().keys(Object.assign({}, a, b));
}

function requireOneOf(...keys) {
  return Joi.alternatives().try(
    ...(keys.map((k) => Joi.object().keys({ [k]: Joi.exist().required() })))
  );
}

const PreAggregationRefreshKeySchema = condition(
  (s) => defined(s.sql),
  Joi.object().keys({
    sql: Joi.func().required(),
    // We dont support timezone for this, because it's useless
    // We cannot support cron interval
    every: everyInterval,
  }),
  condition(
    (s) => defined(s.every),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval),
      timezone: everyCronTimeZone,
      incremental: Joi.boolean().strict(),
      updateWindow: everyInterval
    }),
    requireOneOf('sql', 'every')
  )
);

const BasePreAggregationWithoutPartitionGranularity = {
  refreshKey: PreAggregationRefreshKeySchema,
  sqlAlias: Joi.string().optional(),
  useOriginalSqlPreAggregations: Joi.boolean().strict(),
  external: Joi.boolean().strict(),
  scheduledRefresh: Joi.boolean().strict(),
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

const AutoRollupSchema = inherit(BasePreAggregation, {
  type: Joi.any().valid('autoRollup').required(),
  maxPreAggregations: Joi.number(),
});

const OriginalSqlSchema = condition(
  (s) => defined(s.partitionGranularity) || defined(s.timeDimension) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required(),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
      timeDimensionReference: Joi.func().required(),
    }),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required(),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
      timeDimension: Joi.func().required(),
    })
  ),
  inherit(BasePreAggregationWithoutPartitionGranularity, {
    type: Joi.any().valid('originalSql').required(),
  })
);

const GranularitySchema = Joi.string().valid('second', 'minute', 'hour', 'day', 'week', 'month', 'year').required();

const ReferencesFields = ['timeDimensionReference', 'rollupReferences', 'measureReferences', 'dimensionReferences', 'segmentReferences'];
const NonReferencesFields = ['timeDimension', 'rollups', 'measures', 'dimensions', 'segments'];

function hasAnyField(fields, s) {
  return !fields.every((f) => !defined(s[f]));
}

function errorOnMixing(schema) {
  return condition(
    (s) => hasAnyField(ReferencesFields, s) && hasAnyField(NonReferencesFields, s),
    Joi.any().forbidden().error(
      new Error(`[${ReferencesFields.join(', ')}] are deprecated, please, use [${NonReferencesFields.join(', ')}] instead`)
    ),
    schema
  );
}

const RollUpJoinSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.rollupReferences) || defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      granularity: GranularitySchema,
      timeDimensionReference: Joi.func().required(),
      rollupReferences: Joi.func().required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
    }),
    // RollupJoin without references
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      granularity: GranularitySchema,
      timeDimension: Joi.func().required(),
      rollups: Joi.func().required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
    })
  ),
  condition(
    (s) => defined(s.rollupReferences),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      rollupReferences: Joi.func().required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
    }),
    // RollupJoin without references
    condition(
      (s) => defined(s.rollups),
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollupJoin').required(),
        rollups: Joi.func().required(),
        measures: Joi.func(),
        dimensions: Joi.func(),
        segments: Joi.func(),
      }),
      requireOneOf('granularity', 'rollups', 'timeDimension')
    )
  )
);

const RollUpSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      timeDimensionReference: Joi.func().required(),
      granularity: GranularitySchema,
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
    }),
    // Rollup without References postfix
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      timeDimension: Joi.func().required(),
      granularity: GranularitySchema,
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
    })
  ),
  Joi.alternatives().try(
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func()
    }),
    // Rollup without References postfix
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func()
    })
  )
);

const PreAggregationsAlternatives = Joi.object().pattern(
  identifierRegex,
  errorOnMixing(
    Joi.alternatives().conditional(
      Joi.ref('.type'), [
        { is: 'autoRollup', then: AutoRollupSchema },
        { is: 'originalSql', then: OriginalSqlSchema },
        { is: 'rollupJoin', then: RollUpJoinSchema },
        { is: 'rollup',
          then: RollUpSchema,
          otherwise: Joi.object().keys({
            type: Joi.string().valid('autoRollup', 'originalSql', 'rollupJoin', 'rollup').required()
          })
        }
      ]
    )
  )
);

const CubeRefreshKeySchema = condition(
  (s) => defined(s.every),
  condition(
    (s) => defined(s.sql),
    Joi.object().keys({
      sql: Joi.func().required(),
      // We dont support timezone for this, because it's useless
      // We cannot support cron interval
      every: everyInterval,
    }),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval).required(),
      timezone: everyCronTimeZone,
    })
  ),
  condition(
    (s) => defined(s.immutable),
    Joi.object().keys({
      immutable: Joi.boolean().strict().required()
    }),
    requireOneOf('every', 'sql', 'immutable')
  )
);

const MeasuresSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().conditional(
  Joi.ref('.type'), [
    {
      is: 'count',
      then: inherit(BaseMeasure, {
        type: Joi.any().valid('count').required(),
        sql: Joi.func(),
      })
    },
    {
      is: Joi.string().valid(
        'number', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
      ),
      then: inherit(BaseMeasure, {
        sql: Joi.func().required(),
        type: Joi.any().valid(
          'number', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
        ).required()
      }),
      otherwise: Joi.object().keys({
        type: Joi.string().valid(
          'count', 'number', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
        ).required()
      })
    }
  ]
));

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

const cubeSchema = Joi.object().keys({
  name: identifier,
  sql: Joi.func().required(),
  refreshKey: CubeRefreshKeySchema,
  fileName: Joi.string().required(),
  extends: Joi.func(),
  allDefinitions: Joi.func(),
  title: Joi.string(),
  sqlAlias: Joi.string(),
  dataSource: Joi.string(),
  description: Joi.string(),
  rewriteQueries: Joi.boolean().strict(),
  joins: Joi.object().pattern(identifierRegex, Joi.object().keys({
    sql: Joi.func().required(),
    relationship: Joi.any().valid('hasMany', 'belongsTo', 'hasOne').required()
  })),
  measures: MeasuresSchema,
  dimensions: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    inherit(BaseDimensionWithoutSubQuery, {
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
    }),
    inherit(BaseDimensionWithoutSubQuery, {
      latitude: Joi.object().keys({
        sql: Joi.func().required()
      }),
      longitude: Joi.object().keys({
        sql: Joi.func().required()
      })
    }),
    inherit(BaseDimension, {
      sql: Joi.func().required()
    })
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
    const key = d?.context?.message || d?.message;
    const val = key?.replace(`"${d.context?.label}"`, `(${d.context?.label} = ${d.context?.value})`);
    explain.set(key, val);
  }
}

function formatErrorMessage(error) {
  const explain = new Map();
  explain.set(error.message, error.message);

  error?.details?.forEach((d) => formatErrorMessageFromDetails(explain, d));

  const messages = Array.from(explain.values());

  let message = messages.shift();

  if (messages.length > 0) {
    message += `\nPossible reasons (one of):\n\t* ${messages.join('\n\t* ')}`;
  }

  return message.replace(/ = undefined\) is required/g, ') is required');
}

function collectFunctionFieldsPatterns(patterns, path, o) {
  let key = o?.id || o?.key || ((o?.patterns?.length || 0) > 0 ? '*' : undefined);
  if (o?.schema?.type === 'array' && key && typeof key === 'string') {
    key = `${key}.0`;
  }

  // eslint-disable-next-line no-nested-ternary
  const newPath = key && typeof key === 'string' ? (path.length > 0 ? `${path}.${key}` : key) : path;

  if (o?.schema?.type === 'function') {
    patterns.add(newPath);
    return;
  }

  if (Array.isArray(o)) {
    o.forEach((v) => collectFunctionFieldsPatterns(patterns, newPath, v));
  } else if (o instanceof Map) {
    o.forEach((v, k) => collectFunctionFieldsPatterns(patterns, newPath, v));
  } else if (o === Object(o)) {
    // eslint-disable-next-line no-restricted-syntax
    for (const k in o) {
      if (k !== '$_root' && o.hasOwnProperty(k)) collectFunctionFieldsPatterns(patterns, newPath, o[k]);
    }
  }
}

export function functionFieldsPatterns() {
  const functionPatterns = new Set();
  collectFunctionFieldsPatterns(functionPatterns, '', cubeSchema);
  return Array.from(functionPatterns);
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
