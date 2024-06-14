import Joi from 'joi';
import cronParser from 'cron-parser';

import type { CubeSymbols } from './CubeSymbols';
import type { ErrorReporter } from './ErrorReporter';

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

// Update this whenever schema is updated otherwise YAML will be broken
export const nonStringFields = new Set([
  'primaryKey',
  'scheduledRefresh',
  'unionWithSourceData',
  'rewriteQueries',
  'shown',
  'public',
  'subQuery',
  'propagateFiltersToSubQuery',
  'incremental',
  'external',
  'useOriginalSqlPreAggregations',
  'readOnly',
  'prefix'
]);

const identifierRegex = /^[_a-zA-Z][_a-zA-Z0-9]*$/;

const identifier = Joi.string().regex(identifierRegex, 'identifier');

function formatStatePath(state: Joi.State): string {
  if (state.path) {
    // TODO: Remove cast after upgrade of Joi. It show it as string, while it's array
    const path = state.path as any as string[];
    return path.join('.');
  }

  return '<unknown path>';
}

const regexTimeInterval = Joi.string().custom((value, helper) => {
  if (value.match(/^(-?\d+) (minute|hour|day|week|month|quarter|year)$/)) {
    return value;
  } else {
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) does not match regexp: /^(-?\\d+) (minute|hour|day|week|month|quarter|year)$/` });
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
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) does not match regexp: /^(\\d+) (second|minute|hour|day|week)s?$/` });
  }
});

const everyCronInterval = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression(value);
    return value;
  } catch (e: any) {
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) CronParser: ${e.toString()}` });
  }
});

const everyCronTimeZone = Joi.string().custom((value, helper) => {
  try {
    cronParser.parseExpression('0 * * * *', { currentDate: '2020-01-01 00:00:01', tz: value });
    return value;
  } catch (e) {
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) unknown timezone. Take a look here https://cube.dev/docs/schema/reference/cube#supported-timezones to get available time zones` });
  }
});

const BaseDimensionWithoutSubQuery = {
  aliases: Joi.array().items(Joi.string()),
  type: Joi.any().valid('string', 'number', 'boolean', 'time', 'geo').required(),
  fieldType: Joi.any().valid('string'),
  valuesAsSegments: Joi.boolean().strict(),
  primaryKey: Joi.boolean().strict(),
  shown: Joi.boolean().strict(),
  public: Joi.boolean().strict(),
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
  meta: Joi.any(),
};

const BaseDimension = Object.assign({
  subQuery: Joi.boolean().strict(),
  propagateFiltersToSubQuery: Joi.boolean().strict()
}, BaseDimensionWithoutSubQuery);

const FixedRollingWindow = {
  type: Joi.string().valid('fixed'),
  trailing: timeInterval,
  leading: timeInterval,
  offset: Joi.any().valid('start', 'end')
};

const YearToDate = {
  type: Joi.string().valid('year_to_date'),
};

const QuarterToDate = {
  type: Joi.string().valid('quarter_to_date'),
};

const MonthToDate = {
  type: Joi.string().valid('month_to_date'),
};

const BaseMeasure = {
  aliases: Joi.array().items(Joi.string()),
  format: Joi.any().valid('percent', 'currency', 'number'),
  public: Joi.boolean().strict(),
  // TODO: Deprecate and remove, please use public
  visible: Joi.boolean().strict(),
  // TODO: Deprecate and remove, please use public
  shown: Joi.boolean().strict(),
  cumulative: Joi.boolean().strict(),
  filters: Joi.array().items(
    Joi.object().keys({
      sql: Joi.func().required()
    })
  ),
  title: Joi.string(),
  description: Joi.string(),
  rollingWindow: Joi.alternatives().conditional(
    Joi.ref('.type'), [
      { is: 'year_to_date', then: YearToDate },
      { is: 'quarter_to_date', then: QuarterToDate },
      { is: 'month_to_date', then: MonthToDate },
      { is: 'fixed',
        then: FixedRollingWindow,
        otherwise: FixedRollingWindow
      }
    ]
  ),
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
      columns: Joi.func().required(),
      type: Joi.any().valid('regular', 'aggregate'),
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
  readOnly: Joi.boolean().strict(),
  streamOffset: Joi.any().valid('earliest', 'latest'),
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
      uniqueKeyColumns: Joi.array().items(Joi.string()),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
      timeDimensionReference: Joi.func().required(),
      allowNonStrictDateRangeMatch: Joi.bool(),
    }),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required(),
      uniqueKeyColumns: Joi.array().items(Joi.string()),
      partitionGranularity: BasePreAggregation.partitionGranularity.required(),
      timeDimension: Joi.func().required(),
      allowNonStrictDateRangeMatch: Joi.bool(),
    })
  ),
  inherit(BasePreAggregationWithoutPartitionGranularity, {
    type: Joi.any().valid('originalSql').required(),
    uniqueKeyColumns: Joi.array().items(Joi.string())
  }),
);

const GranularitySchema = Joi.string().valid('second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year').required();

const ReferencesFields = ['timeDimensionReference', 'rollupReferences', 'measureReferences', 'dimensionReferences', 'segmentReferences'];
const NonReferencesFields = ['timeDimension', 'timeDimensions', 'rollups', 'measures', 'dimensions', 'segments'];

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
      scheduledRefresh: Joi.boolean().valid(false),
      granularity: GranularitySchema,
      allowNonStrictDateRangeMatch: Joi.bool(),
      timeDimensionReference: Joi.func().required(),
      rollupReferences: Joi.func().required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
    }),
    // RollupJoin without references
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required(),
      scheduledRefresh: Joi.boolean().valid(false),
      granularity: GranularitySchema,
      timeDimension: Joi.func().required(),
      allowNonStrictDateRangeMatch: Joi.bool(),
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
      scheduledRefresh: Joi.boolean().valid(false),
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
        scheduledRefresh: Joi.boolean().valid(false),
        rollups: Joi.func().required(),
        measures: Joi.func(),
        dimensions: Joi.func(),
        segments: Joi.func(),
      }),
      requireOneOf('granularity', 'rollups', 'timeDimension')
    )
  )
);

const RollupLambdaSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension),
  {
    type: Joi.any().valid('rollupLambda').required(),
    granularity: GranularitySchema,
    timeDimension: Joi.func().required(),
    rollups: Joi.func().required(),
    measures: Joi.func(),
    dimensions: Joi.func(),
    segments: Joi.func(),
    unionWithSourceData: Joi.boolean().strict(),
  },
  {
    type: Joi.any().valid('rollupLambda').required(),
    rollups: Joi.func().required(),
    measures: Joi.func(),
    dimensions: Joi.func(),
    segments: Joi.func(),
    unionWithSourceData: Joi.boolean().strict(),
  },
);

const RollUpSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension) || defined(s.timeDimensions) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      timeDimensionReference: Joi.func().required(),
      granularity: GranularitySchema,
      allowNonStrictDateRangeMatch: Joi.bool(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
    }),
    condition(
      (s) => defined(s.timeDimension),
      // Rollup without References postfix
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollup').required(),
        timeDimension: Joi.func().required(),
        allowNonStrictDateRangeMatch: Joi.bool(),
        granularity: GranularitySchema,
        measures: Joi.func(),
        dimensions: Joi.func(),
        segments: Joi.func(),
      }),
      // Rollup with multiple time dimensions
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollup').required(),
        timeDimensions: Joi.array().items(Joi.object().keys({
          dimension: Joi.func(),
          granularity: GranularitySchema,
        })),
        allowNonStrictDateRangeMatch: Joi.bool(),
        measures: Joi.func(),
        dimensions: Joi.func(),
        segments: Joi.func(),
      })
    )
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
        { is: 'rollupLambda', then: RollupLambdaSchema },
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

const measureType = Joi.string().valid(
  'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
);

const measureTypeWithCount = Joi.string().valid(
  'count', 'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox'
);

const postAggregateMeasureType = Joi.string().valid(
  'count', 'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox',
  'rank'
);

const MeasuresSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().conditional(Joi.ref('.postAggregate'), [
  {
    is: true,
    then: inherit(BaseMeasure, {
      postAggregate: Joi.boolean().strict(),
      type: postAggregateMeasureType.required(),
      sql: Joi.func(), // TODO .required(),
      groupBy: Joi.func(),
      reduceBy: Joi.func(),
      addGroupBy: Joi.func(),
      timeShift: Joi.array().items(Joi.object().keys({
        timeDimension: Joi.func().required(),
        interval: regexTimeInterval.required(),
        type: Joi.string().valid('next', 'prior').required(),
      })),
      // TODO validate for order window functions
      orderBy: Joi.array().items(Joi.object().keys({
        sql: Joi.func().required(),
        dir: Joi.string().valid('asc', 'desc')
      })),
    })
  }
]).conditional(
  Joi.ref('.type'), [
    {
      is: 'count',
      then: inherit(BaseMeasure, {
        type: Joi.any().valid('count').required(),
        sql: Joi.func(),
      })
    },
    {
      is: measureType,
      then: inherit(BaseMeasure, {
        sql: Joi.func().required(),
        type: measureType.required()
      }),
      otherwise: Joi.object().keys({
        type: measureTypeWithCount.required()
      })
    }
  ]
));

const SegmentsSchema = Joi.object().pattern(identifierRegex, Joi.object().keys({
  aliases: Joi.array().items(Joi.string()),
  sql: Joi.func().required(),
  title: Joi.string(),
  description: Joi.string(),
  meta: Joi.any(),
  shown: Joi.boolean().strict(),
  public: Joi.boolean().strict(),
}));

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

const baseSchema = {
  name: identifier,
  refreshKey: CubeRefreshKeySchema,
  fileName: Joi.string().required(),
  extends: Joi.func(),
  allDefinitions: Joi.func(),
  title: Joi.string(),
  sqlAlias: Joi.string(),
  dataSource: Joi.string(),
  description: Joi.string(),
  rewriteQueries: Joi.boolean().strict(),
  shown: Joi.boolean().strict(),
  public: Joi.boolean().strict(),
  meta: Joi.any(),
  joins: Joi.object().pattern(identifierRegex, Joi.object().keys({
    sql: Joi.func().required(),
    relationship: Joi.any().valid(
      'belongsTo', 'belongs_to', 'many_to_one', 'manyToOne',
      'hasMany', 'has_many', 'one_to_many', 'oneToMany',
      'hasOne', 'has_one', 'one_to_one', 'oneToOne'
    ).required()
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
      }).required(),
      longitude: Joi.object().keys({
        sql: Joi.func().required()
      }).required()
    }),
    inherit(BaseDimension, {
      sql: Joi.func().required()
    }),
    inherit(BaseDimension, {
      postAggregate: Joi.boolean().valid(true),
      type: Joi.any().valid('number').required(),
      sql: Joi.func().required(),
      addGroupBy: Joi.func(),
    })
  )),
  segments: SegmentsSchema,
  preAggregations: PreAggregationsAlternatives,
  hierarchies: Joi.array().items(Joi.object().keys({
    name: Joi.string().required(),
    title: Joi.string(),
    levels: Joi.func()
  })),
};

const cubeSchema = inherit(baseSchema, {
  sql: Joi.func(),
  sqlTable: Joi.func(),
}).xor('sql', 'sqlTable').messages({
  'object.xor': 'You must use either sql or sqlTable within a model, but not both'
});

const viewSchema = inherit(baseSchema, {
  isView: Joi.boolean().strict(),
  includes: Joi.func(),
  excludes: Joi.func(),
  cubes: Joi.array().items(
    Joi.object().keys({
      joinPath: Joi.func().required(),
      prefix: Joi.boolean(),
      split: Joi.boolean(),
      alias: Joi.string(),
      includes: Joi.alternatives([
        Joi.string().valid('*'),
        Joi.array().items(Joi.alternatives([
          Joi.string().required(),
          Joi.object().keys({
            name: Joi.string().required(),
            alias: Joi.string()
          })
        ]))
      ]).required(),
      excludes: Joi.array().items(Joi.string().required()),
    }).oxor('split', 'prefix').messages({
      'object.oxor': 'Using split together with prefix is not supported'
    })
  ),
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

export function functionFieldsPatterns(): string[] {
  const functionPatterns = new Set<string>();
  collectFunctionFieldsPatterns(functionPatterns, '', { ...cubeSchema, ...viewSchema });
  return Array.from(functionPatterns);
}

export class CubeValidator {
  protected readonly validCubes: Map<string, boolean> = new Map();

  public constructor(
    protected readonly cubeSymbols: CubeSymbols
  ) {
  }

  public compile(cubes, errorReporter: ErrorReporter) {
    return this.cubeSymbols.cubeList.map(
      (v) => this.validate(this.cubeSymbols.getCubeDefinition(v.name), errorReporter.inContext(`${v.name} cube`))
    );
  }

  public validate(cube, errorReporter: ErrorReporter) {
    const result = cube.isView ? viewSchema.validate(cube) : cubeSchema.validate(cube);

    if (result.error != null) {
      errorReporter.error(formatErrorMessage(result.error), result.error);
    } else {
      this.validCubes[cube.name] = true;
    }

    return result;
  }

  public isCubeValid(cube) {
    return this.validCubes[cube.name] || cube.isSplitView;
  }
}
