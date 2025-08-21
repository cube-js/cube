import Joi from 'joi';
import cronParser from 'cron-parser';

import type { CubeSymbols, CubeDefinition } from './CubeSymbols';
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
  'prefix',
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
  return Joi.object().keys({ ...a, ...b });
}

function requireOneOf(...keys) {
  return Joi.alternatives().try(
    ...(keys.map((k) => Joi.object().keys({ [k]: Joi.exist().required() })))
  );
}

const regexTimeInterval = Joi.string().custom((value, helper) => {
  if (value.match(/^(-?\d+) (minute|hour|day|week|month|quarter|year)s?$/)) {
    return value;
  } else {
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) does not match regexp: /^(-?\\d+) (minute|hour|day|week|month|quarter|year)s?$/` });
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

const GranularityInterval = Joi.string().pattern(/^\d+\s+(second|minute|hour|day|week|month|quarter|year)s?(\s\d+\s+(second|minute|hour|day|week|month|quarter|year)s?){0,7}$/, 'granularity interval');
// Do not allow negative intervals for granularities, while offsets could be negative
const GranularityOffset = Joi.string().pattern(/^-?(\d+\s+)(second|minute|hour|day|week|month|quarter|year)s?(\s-?\d+\s+(second|minute|hour|day|week|month|quarter|year)s?){0,7}$/, 'granularity offset');

const formatSchema = Joi.alternatives([
  Joi.string().valid('imageUrl', 'link', 'currency', 'percent', 'number', 'id'),
  Joi.object().keys({
    type: Joi.string().valid('link'),
    label: Joi.string().required()
  })
]);

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
  format: formatSchema,
  meta: Joi.any(),
  granularities: Joi.when('type', {
    is: 'time',
    then: Joi.object().pattern(identifierRegex,
      Joi.alternatives([
        Joi.object().keys({
          title: Joi.string(),
          interval: GranularityInterval.required(),
          origin: Joi.string().required().custom((value, helpers) => {
            const date = new Date(value);

            if (Number.isNaN(date.getTime())) {
              return helpers.message({ custom: 'Origin should be valid date-only form: YYYY[-MM[-DD]] or date-time form: YYYY-MM-DD[T]HH:mm[:ss[.sss[Z]]]' });
            }
            return value;
          }),
        }),
        Joi.object().keys({
          title: Joi.string(),
          interval: GranularityInterval.required().custom((value, helper) => {
            const intParsed = value.split(' ');
            const msg = { custom: 'Arbitrary intervals cannot be used without origin point specified' };

            if (intParsed.length !== 2) {
              return helper.message(msg);
            }

            const v = parseInt(intParsed[0], 10);
            const unit = intParsed[1];

            const validIntervals = {
              // Any number of years is valid
              year: () => true,
              // Only months divisible by a year with no remainder are valid
              month: () => 12 % v === 0,
              // Only quarters divisible by a year with no remainder are valid
              quarter: () => 4 % v === 0,
              // Only 1 week is valid
              week: () => v === 1,
              // Only 1 day is valid
              day: () => v === 1,
              // Only hours divisible by a day with no remainder are valid
              hour: () => 24 % v === 0,
              // Only minutes divisible by an hour with no remainder are valid
              minute: () => 60 % v === 0,
              // Only seconds divisible by a minute with no remainder are valid
              second: () => 60 % v === 0,
            };

            const isValid = Object.keys(validIntervals).some(key => unit.includes(key) && validIntervals[key]());

            return isValid ? value : helper.message(msg);
          }),
          offset: GranularityOffset.optional(),
        }),
        Joi.object().keys({
          title: Joi.string(),
          sql: Joi.func().required()
        })
      ])).optional(),
    otherwise: Joi.forbidden()
  })
};

const BaseDimension = {
  subQuery: Joi.boolean().strict(),
  propagateFiltersToSubQuery: Joi.boolean().strict(),
  ...BaseDimensionWithoutSubQuery
};

const FixedRollingWindow = {
  type: Joi.string().valid('fixed'),
  trailing: timeInterval,
  leading: timeInterval,
  offset: Joi.any().valid('start', 'end')
};

const GranularitySchema = Joi.string().required(); // To support custom granularities

const YearToDate = {
  type: Joi.string().valid('year_to_date'),
};

const QuarterToDate = {
  type: Joi.string().valid('quarter_to_date'),
};

const MonthToDate = {
  type: Joi.string().valid('month_to_date'),
};

const ToDate = {
  type: Joi.string().valid('to_date'),
  granularity: GranularitySchema,
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
      { is: 'to_date', then: ToDate },
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

const PreAggregationRefreshKeySchema = condition(
  (s) => defined(s.sql),
  Joi.object().keys({
    sql: Joi.func().required(),
    // We don't support timezone for this, because it's useless
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
  outputColumnTypes: Joi.array().items(Joi.object().keys({
    member: Joi.func().required(),
    type: Joi.string().required()
  })),
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
      uniqueKeyColumns: Joi.array().items(Joi.string()),
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
        uniqueKeyColumns: Joi.array().items(Joi.string()),
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
        uniqueKeyColumns: Joi.array().items(Joi.string()),
      })
    )
  ),
  Joi.alternatives().try(
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measureReferences: Joi.func(),
      dimensionReferences: Joi.func(),
      segmentReferences: Joi.func(),
      uniqueKeyColumns: Joi.array().items(Joi.string()),
    }),
    // Rollup without References postfix
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required(),
      measures: Joi.func(),
      dimensions: Joi.func(),
      segments: Joi.func(),
      uniqueKeyColumns: Joi.array().items(Joi.string()),
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
      // We don't support timezone for this, because it's useless
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

const multiStageMeasureType = Joi.string().valid(
  'count', 'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'runningTotal', 'countDistinctApprox', 'numberAgg',
  'rank'
);

const timeShiftItemRequired = Joi.object({
  timeDimension: Joi.func().required(),
  interval: regexTimeInterval.required(),
  type: Joi.string().valid('next', 'prior').required(),
});

const timeShiftItemOptional = Joi.object({
  timeDimension: Joi.func(), // not required
  interval: regexTimeInterval,
  name: identifier,
  type: Joi.string().valid('next', 'prior'),
})
  .xor('name', 'interval')
  .and('interval', 'type');

const MeasuresSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().conditional(Joi.ref('.multiStage'), [
  {
    is: true,
    then: inherit(BaseMeasure, {
      multiStage: Joi.boolean().strict(),
      type: multiStageMeasureType.required(),
      sql: Joi.func(), // TODO .required(),
      groupBy: Joi.func(),
      reduceBy: Joi.func(),
      addGroupBy: Joi.func(),
      timeShift: Joi.alternatives().conditional(Joi.array().length(1), {
        then: Joi.array().items(timeShiftItemOptional),
        otherwise: Joi.array().items(timeShiftItemRequired)
      }),
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

const CalendarTimeShiftItem = Joi.alternatives().try(
  Joi.object({
    name: identifier.required(),
    interval: regexTimeInterval.required(),
    type: Joi.string().valid('next', 'prior').required(),
    sql: Joi.forbidden()
  }),
  Joi.object({
    name: identifier.required(),
    sql: Joi.func().required(),
    interval: Joi.forbidden(),
    type: Joi.forbidden()
  }),
  Joi.object({
    interval: regexTimeInterval.required(),
    type: Joi.string().valid('next', 'prior').required(),
    sql: Joi.func().required(),
    name: Joi.forbidden()
  })
);

const DimensionsSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().try(
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
    sql: Joi.func().required(),
  }),
  inherit(BaseDimension, {
    multiStage: Joi.boolean().valid(true),
    type: Joi.any().valid('number').required(),
    sql: Joi.func().required(),
    addGroupBy: Joi.func(),
  }),
  // TODO should be valid only for calendar cubes, but this requires significant refactoring
  // of all schemas. Left for the future when we'll switch to zod.
  inherit(BaseDimensionWithoutSubQuery, {
    type: Joi.any().valid('time').required(),
    sql: Joi.func().required(),
    timeShift: Joi.array().items(CalendarTimeShiftItem),
  })
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

const PolicyFilterSchema = Joi.object().keys({
  member: Joi.func().required(),
  memberReference: Joi.string(),
  operator: Joi.any().valid(
    'equals',
    'notEquals',
    'contains',
    'notContains',
    'startsWith',
    'notStartsWith',
    'endsWith',
    'notEndsWith',
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
    'beforeOrOnDate',
    'afterDate',
    'afterOrOnDate',
    'measureFilter',
  ).required(),
  values: Joi.when('operator', {
    is: Joi.valid('set', 'notSet'),
    then: Joi.func().optional(),
    otherwise: Joi.func().required()
  })
});

const PolicyFilterConditionSchema = Joi.object().keys({
  or: Joi.array().items(PolicyFilterSchema, Joi.link('...').description('Filter Condition schema')),
  and: Joi.array().items(PolicyFilterSchema, Joi.link('...').description('Filter Condition schema')),
}).xor('or', 'and');

const MemberLevelPolicySchema = Joi.object().keys({
  includes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string())
  ]),
  excludes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string().required())
  ]),
  includesMembers: Joi.array().items(Joi.string().required()),
  excludesMembers: Joi.array().items(Joi.string().required()),
});

const RowLevelPolicySchema = Joi.object().keys({
  filters: Joi.array().items(PolicyFilterSchema, PolicyFilterConditionSchema),
  allowAll: Joi.boolean().valid(true).strict(),
}).xor('filters', 'allowAll');

const RolePolicySchema = Joi.object().keys({
  role: Joi.string(),
  group: Joi.string(),
  groups: Joi.array().items(Joi.string()),
  memberLevel: MemberLevelPolicySchema,
  rowLevel: RowLevelPolicySchema,
  conditions: Joi.array().items(Joi.object().keys({
    if: Joi.func().required(),
  })),
})
  .nand('group', 'groups') // Cannot have both group and groups
  .nand('role', 'group') // Cannot have both role and group
  .nand('role', 'groups') // Cannot have both role and groups
  .or('role', 'group', 'groups'); // Must have at least one

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

const hierarchySchema = Joi.object().pattern(identifierRegex, Joi.object().keys({
  title: Joi.string(),
  public: Joi.boolean().strict(),
  levels: Joi.func()
}));

const baseSchema = {
  name: identifier,
  refreshKey: CubeRefreshKeySchema,
  fileName: Joi.string().required(),
  extends: Joi.func(),
  allDefinitions: Joi.func(), // Helpers function for extending
  rawFolders: Joi.func(), // Helpers function for extending
  rawCubes: Joi.func(), // Helpers function for extending
  title: Joi.string(),
  sqlAlias: Joi.string(),
  dataSource: Joi.string(),
  description: Joi.string(),
  rewriteQueries: Joi.boolean().strict(),
  shown: Joi.boolean().strict(),
  public: Joi.boolean().strict(),
  meta: Joi.any(),
  joins: Joi.alternatives([
    Joi.object().pattern(identifierRegex, Joi.object().keys({
      sql: Joi.func().required(),
      relationship: Joi.any().valid(
        'belongsTo', 'belongs_to', 'many_to_one', 'manyToOne',
        'hasMany', 'has_many', 'one_to_many', 'oneToMany',
        'hasOne', 'has_one', 'one_to_one', 'oneToOne'
      ).required()
    })),
    Joi.array().items(Joi.object().keys({
      name: identifier.required(),
      sql: Joi.func().required(),
      relationship: Joi.any().valid(
        'belongsTo', 'belongs_to', 'many_to_one', 'manyToOne',
        'hasMany', 'has_many', 'one_to_many', 'oneToMany',
        'hasOne', 'has_one', 'one_to_one', 'oneToOne'
      ).required()
    }))
  ]),
  measures: MeasuresSchema,
  dimensions: DimensionsSchema,
  segments: SegmentsSchema,
  preAggregations: PreAggregationsAlternatives,
  accessPolicy: Joi.array().items(RolePolicySchema.required()),
  hierarchies: hierarchySchema,
};

const cubeSchema = inherit(baseSchema, {
  sql: Joi.func(),
  sqlTable: Joi.func(),
  calendar: Joi.boolean().strict(),
}).xor('sql', 'sqlTable').messages({
  'object.xor': 'You must use either sql or sqlTable within a model, but not both'
});

const folderSchema = Joi.object().keys({
  name: Joi.string().required(),
  includes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(
      Joi.alternatives([
        Joi.string().required(),
        Joi.link('#folderSchema'), // Can contain nested folders
      ]),
    ),
  ]).required(),
}).id('folderSchema');

const viewSchema = inherit(baseSchema, {
  isView: Joi.boolean().strict(),
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
            name: identifier.required(),
            alias: identifier,
            title: Joi.string(),
            description: Joi.string(),
            format: formatSchema,
            meta: Joi.any(),
          })
        ]))
      ]).required(),
      excludes: Joi.array().items(Joi.string().required()),
    }).oxor('split', 'prefix').messages({
      'object.oxor': 'Using split together with prefix is not supported'
    })
  ),
  folders: Joi.array().items(folderSchema),
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
    const options = {
      nonEnumerables: true,
      abortEarly: false, // This will allow all errors to be reported, not just the first one
    };
    const result = cube.isView ? viewSchema.validate(cube, options) : cubeSchema.validate(cube, options);

    if (result.error != null) {
      errorReporter.error(formatErrorMessage(result.error), result.error);
    } else {
      this.validCubes.set(cube.name, true);
    }

    return result;
  }

  public isCubeValid(cube: CubeDefinition): boolean {
    return this.validCubes.get(cube.name) ?? cube.isSplitView ?? false;
  }
}
