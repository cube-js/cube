import crypto from 'crypto';
import Joi from 'joi';
import { LRUCache } from 'lru-cache';
import cronParser from 'cron-parser';
import { isPredefinedGranularity, TIME_SERIES } from '@cubejs-backend/shared';

import { CubeSymbols, CubeDefinition, ToString } from './CubeSymbols';
import type { ErrorReporter } from './ErrorReporter';
import { CompilerInterface } from './PrepareCompiler';
import { NAMED_NUMERIC_FORMATS } from './named-numeric-formats';
import type { ViewGroupInput } from './ViewGroupEvaluator';

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
  'mask',
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

function condition(fun: (value: Record<string, unknown>) => boolean, then: Joi.SchemaLike, otherwise: Joi.SchemaLike) {
  return Joi.alternatives().conditional(
    Joi.ref('.'), {
      is: Joi.custom((value, helper) => (fun(value) ? value : helper.message({}))),
      then,
      otherwise
    }
  );
}

function defined(a: unknown) {
  return typeof a !== 'undefined';
}

function inherit(a: Joi.SchemaMap, b: Joi.SchemaMap) {
  return Joi.object().keys({ ...a, ...b });
}

function requireOneOf(...keys: string[]) {
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
    return helper.message({ custom: `(${formatStatePath(helper.state)} = ${value}) unknown timezone. Take a look here https://docs.cube.dev/admin/time-zones#valid-time-zone-values to get available time zones` });
  }
});

const PREDEFINED_GRANULARITY_NAMES = Object.keys(TIME_SERIES).sort();

const GranularityInterval = Joi.string().pattern(/^\d+\s+(second|minute|hour|day|week|month|quarter|year)s?(\s\d+\s+(second|minute|hour|day|week|month|quarter|year)s?){0,7}$/, 'granularity interval');
// Do not allow negative intervals for granularities, while offsets could be negative
const GranularityOffset = Joi.string().pattern(/^-?(\d+\s+)(second|minute|hour|day|week|month|quarter|year)s?(\s-?\d+\s+(second|minute|hour|day|week|month|quarter|year)s?){0,7}$/, 'granularity offset');

const formatAlternatives = [
  Joi.string().valid('imageUrl', 'link', 'currency', 'percent', 'number', 'id'),
  Joi.object().keys({
    type: Joi.string().valid('link').description('Format kind: link renders the value as a link titled with label'),
    label: Joi.string().required().description('Display label for the link')
  })
];
const formatSchema = Joi.alternatives(formatAlternatives);

// POSIX strftime specification (IEEE Std 1003.1 / POSIX.1) with d3-time-format extensions
// See: https://pubs.opengroup.org/onlinepubs/009695399/functions/strptime.html
// See: https://d3js.org/d3-time-format
const TIME_SPECIFIERS = new Set([
  // POSIX standard specifiers
  'a', 'A', 'b', 'B', 'c', 'd', 'H', 'I', 'j', 'm',
  'M', 'n', 'p', 'S', 't', 'U', 'w', 'W', 'x', 'X',
  'y', 'Y', 'Z', '%',
  // d3-time-format extensions
  'e', // space-padded day of month
  'f', // microseconds
  'g', // ISO 8601 year without century
  'G', // ISO 8601 year with century
  'L', // milliseconds
  'q', // quarter
  'Q', // milliseconds since UNIX epoch
  's', // seconds since UNIX epoch
  'u', // Monday-based weekday [1,7]
  'V', // ISO 8601 week number
]);

const customTimeFormatSchema = Joi.string().custom((value, helper) => {
  let hasSpecifier = false;
  let i = 0;

  while (i < value.length) {
    if (value[i] === '%') {
      if (i + 1 >= value.length) {
        return helper.message({ custom: `Invalid time format "${value}". Incomplete specifier at end of string` });
      }

      const specifier = value[i + 1];

      if (!TIME_SPECIFIERS.has(specifier)) {
        return helper.message({ custom: `Invalid time format "${value}". Unknown specifier '%${specifier}'` });
      }

      // %% is an escape for literal %, not a date/time specifier
      if (specifier !== '%') {
        hasSpecifier = true;
      }

      i += 2;
    } else {
      // Any other character is treated as literal text
      i++;
    }
  }

  if (!hasSpecifier) {
    return helper.message({
      custom: `Invalid strptime format "${value}". Format must contain at least one strptime specifier (e.g., %Y, %m, %d)`
    });
  }

  return value;
});

const timeFormatSchema = Joi.alternatives([
  formatSchema,
  customTimeFormatSchema
]);

// d3-format specification (Python format spec mini-language)
// See: https://d3js.org/d3-format
// See: https://docs.python.org/3/library/string.html#format-specification-mini-language
// Format specifier: [[fill]align][sign][symbol][0][width][,][.precision][~][type]
const NUMERIC_FORMAT_TYPES = new Set([
  'e', // exponent notation
  'f', // fixed-point notation
  'g', // either decimal or exponent notation
  'r', // decimal notation, rounded to significant digits
  's', // decimal notation with an SI prefix
  '%', // multiply by 100 and format as percentage
  'p', // multiply by 100, round to significant digits, and format as percentage
  'b', // binary notation
  'o', // octal notation
  'd', // decimal notation (integer)
  'x', // lowercase hexadecimal notation
  'X', // uppercase hexadecimal notation
  'c', // character data
  'n', // like g, but with locale-specific thousand separator
]);

// d3-format specifier: [[fill]align][sign][symbol][0][width][,][.precision][~][type]
// Regex breakdown:
// (?:(.)?([<>=^]))?           - optional fill (any char) + align (<>=^)
// ([+\-( ])?                  - optional sign (+, -, (, or space)
// ([$#])?                     - optional symbol ($ or #)
// (0)?                        - optional zero flag
// (\d+)?                      - optional width (positive integer)
// (,)?                        - optional comma flag (grouping)
// (?:\.(\d+))?                - optional precision (.N where N is non-negative integer)
// (~)?                        - optional tilde (trim insignificant zeros)
// ([a-zA-Z%])?                - optional type character
const NUMERIC_FORMAT_REGEX = /^(?:(.)?([<>=^]))?([+\-( ])?([$#])?(0)?(\d+)?(,)?(?:\.(\d+))?(~)?([a-zA-Z%])?$/;

const customNumericFormatSchema = Joi.string().custom((value, helper) => {
  const match = value.match(NUMERIC_FORMAT_REGEX);
  if (!match) {
    return helper.message({
      custom: `Invalid numeric format "${value}". Must be a valid d3-format specifier (e.g., ".2f", ",.0f", "$,.2f", ".0%", ".2s")`
    });
  }

  const [, fill, align, sign, symbol, zero, width, comma, precision, tilde, type] = match;

  if (fill && !align) {
    return helper.message({
      custom: `Invalid numeric format "${value}". Fill character requires alignment specifier (<, >, =, or ^)`
    });
  }

  if (type && !NUMERIC_FORMAT_TYPES.has(type.toLowerCase())) {
    return helper.message({
      custom: `Invalid numeric format "${value}". Unknown type character '${type}'. Valid types: ${[...NUMERIC_FORMAT_TYPES].join(', ')}`
    });
  }

  // Validate that the format is not empty (must have at least something meaningful)
  if (!sign && !symbol && !zero && !width && !comma && precision === undefined && !tilde && !type) {
    return helper.message({
      custom: `Invalid numeric format "${value}". Format must contain at least one specifier (e.g., type, precision, comma, sign, symbol)`
    });
  }

  return value;
});

const namedNumericFormatSchema = Joi.string().custom((value, helper) => {
  if (value in NAMED_NUMERIC_FORMATS) {
    return value;
  }

  return helper.message({
    custom: `"${value}" is not a valid named numeric format. Valid named formats: number, percent, currency, id, abbr, accounting (with optional _N decimal suffix, e.g. percent_3)`
  });
});

const measureFormatSchema = Joi.alternatives([
  Joi.string().valid('percent', 'currency', 'number'),
  namedNumericFormatSchema,
  customNumericFormatSchema
]);

const dimensionNumericFormatSchema = Joi.alternatives([
  ...formatAlternatives,
  namedNumericFormatSchema,
  customNumericFormatSchema
]);

// ISO 4217 currency codes
const KNOWN_CURRENCY_CODES = new Set([
  'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN',
  'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL',
  'BSD', 'BTN', 'BWP', 'BYN', 'BZD', 'CAD', 'CDF', 'CHF', 'CLP', 'CNY',
  'COP', 'CRC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EGP',
  'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD',
  'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS',
  'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR',
  'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD',
  'LSL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRU',
  'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK',
  'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG',
  'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK',
  'SGD', 'SHP', 'SLE', 'SOS', 'SRD', 'SSP', 'STN', 'SYP', 'SZL', 'THB',
  'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX',
  'USD', 'UYU', 'UZS', 'VES', 'VND', 'VUV', 'WST', 'XAF', 'XCD', 'XOF',
  'XPF', 'YER', 'ZAR', 'ZMW', 'ZWL',
]);

const currencySchema = Joi.string().custom((value, helper) => {
  const upper = value.toUpperCase();
  if (KNOWN_CURRENCY_CODES.has(upper)) {
    return upper;
  }

  return helper.message({
    custom: `"${value}" is not a valid currency code. Expected a valid 3-letter ISO 4217 code (e.g. USD, EUR)`
  });
});

const MaskSchema = Joi.alternatives([
  Joi.object().keys({ sql: Joi.func().required().description('SQL expression for the masked value. For a measure, use an aggregate expression.') }),
  Joi.number(),
  Joi.boolean().strict(),
  Joi.string(),
]);

const LinkItemSchema = Joi.object().keys({
  name: identifier.required().description('Unique link identifier within the dimension'),
  label: Joi.string().required().description('Label shown to viewers'),
  url: Joi.func().description('SQL expression building the external URL (e.g. a member reference like {full_name})'),
  dashboard: Joi.string().description('Slug of the target dashboard for in-app drill-in'),
  icon: Joi.string().description('Tabler icon name shown for the link'),
  target: Joi.string().valid('blank', 'self').description('Where the link opens'),
  primary: Joi.boolean().strict().description('Render as the cell text itself (at most one per dimension)'),
  params: Joi.array().items(Joi.object().keys({
    key: Joi.string().required().description('Name of the query parameter'),
    value: Joi.func().required().description('SQL expression for the query parameter value'),
  })).description('Query parameters appended to the link URL; values are SQL expressions'),
}).oxor('url', 'dashboard');

const LinksSchema = Joi.array().items(LinkItemSchema).custom((value, helpers) => {
  const names = value.map((link: any) => (typeof link.name === 'function' ? link.name() : link.name));
  const seen = new Set<string>();

  for (const name of names) {
    if (seen.has(name)) {
      return helpers.error('any.custom', { message: `Duplicate link name '${name}'` });
    }
    seen.add(name);
  }
  const primaryCount = value.filter((link: any) => link.primary === true).length;
  if (primaryCount > 1) {
    return helpers.error('any.custom', { message: 'Only one link can be marked as primary' });
  }
  return value;
});

const BaseDimensionWithoutSubQuery = {
  aliases: Joi.array().items(Joi.string()).description('Other names for this dimension'),
  type: Joi.any().valid('string', 'number', 'boolean', 'time', 'geo').required().description('Type of the dimension'),
  fieldType: Joi.any().valid('string').meta({ hidden: true }),
  valuesAsSegments: Joi.boolean().strict().meta({ hidden: true }),
  primaryKey: Joi.boolean().strict().description('Whether this dimension is a primary key'),
  shown: Joi.boolean().strict().description('Whether this dimension is visible in the UI'),
  public: Joi.boolean().strict().description('Whether this dimension is publicly accessible'),
  title: Joi.string().description('Display title for the dimension'),
  description: Joi.string().description('Description of what this dimension represents'),
  suggestFilterValues: Joi.boolean().strict().description("Whether tools suggest this dimension's values when filtering by it"),
  enableSuggestions: Joi.boolean().strict().description('Accepted for compatibility; Core does not read it. Use suggest_filter_values'),
  links: LinksSchema.description("Links rendered on this dimension's cells in supporting tools (e.g. dashboard tables). Each link opens an external url (a SQL expression) or drills into a dashboard referenced by its slug."),
  mask: MaskSchema.description(`Value returned instead of the dimension's value when an access policy masks it: a static number, boolean or string, or an object with a SQL expression. In YAML a string mask is read as a Python expression, so quote a literal twice: "'***'"`),
  format: Joi.when('type', {
    switch: [
      { is: 'time', then: timeFormatSchema },
      { is: 'number', then: dimensionNumericFormatSchema },
    ],
    otherwise: formatSchema
  }).description('How the dimension is displayed. String dimensions take imageUrl, link, currency, percent, number, id or a link object (type: link, label); number and time dimensions also take format strings. See https://docs.cube.dev/reference/data-modeling/dimensions#format'),
  currency: Joi.when('type', {
    is: 'number',
    then: currencySchema,
    otherwise: Joi.any().custom((_value, helper) => helper.message({
      custom: '"currency" property can only be used with dimensions of type "number"'
    }))
  }).description('ISO 4217 currency code (e.g. USD, EUR, GBP) for number-type dimensions. Use with format: currency or currency_N. Case-insensitive. See https://docs.cube.dev/reference/data-modeling/dimensions#currency'),
  meta: Joi.any().description('Additional metadata for the dimension'),
  order: Joi.string().valid('asc', 'desc').description('Default sort order for this dimension. See https://docs.cube.dev/reference/data-modeling/dimensions#order'),
  key: Joi.func().description("Dimension that holds this dimension's key, such as an id for a name. Tools group and filter by the key"),
  keyReference: Joi.string().meta({ hidden: true }),
  values: Joi.when('type', {
    is: 'switch',
    then: Joi.array().items(Joi.string()),
    otherwise: Joi.forbidden()
  }).description("Predefined set of allowed values for switch dimensions. Required when type is 'switch'."),
  granularities: Joi.when('type', {
    is: 'time',
    then: Joi.object().pattern(identifierRegex,
      Joi.alternatives([
        Joi.object().keys({
          title: Joi.string().description('Human-friendly description of the granularity'),
          interval: GranularityInterval.required().description("Duration of the time interval (e.g., '1 week', '15 minutes', '1 year', '1 quarter')"),
          origin: Joi.string().required().custom((value, helpers) => {
            const date = new Date(value);

            if (Number.isNaN(date.getTime())) {
              return helpers.message({ custom: 'Origin should be valid date-only form: YYYY[-MM[-DD]] or date-time form: YYYY-MM-DD[T]HH:mm[:ss[.sss[Z]]]' });
            }
            return value;
          }).description("Align intervals with a specific date (e.g., '2025-04-01')"),
        }),
        Joi.object().keys({
          title: Joi.string().description('Human-friendly description of the granularity'),
          interval: GranularityInterval.required().custom((value, helper) => {
            const intParsed = value.split(' ');
            const msg = { custom: 'Arbitrary intervals cannot be used without origin point specified' };

            if (intParsed.length !== 2) {
              return helper.message(msg);
            }

            const v = parseInt(intParsed[0], 10);
            const unit = intParsed[1];

            const validIntervals: Record<string, () => boolean> = {
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
          }).description("Duration of the time interval (e.g., '1 week', '15 minutes', '1 year', '1 quarter')"),
          offset: GranularityOffset.optional().description("Shift the time interval forward or backward (e.g., '-1 day', '1 month')"),
        }),
        Joi.object().keys({
          title: Joi.string().description('Human-friendly description of the granularity'),
          sql: Joi.func().required().description('SQL expression that overrides a built-in granularity (e.g., month, quarter) in a calendar cube. Use interval instead to define a custom granularity.')
        })
      ])).optional(),
    otherwise: Joi.forbidden()
  }).description('Custom time granularities for time dimensions (e.g., fiscal year, custom week)')
};

const BaseDimension = {
  subQuery: Joi.boolean().strict().description('Whether this dimension is defined as a subquery'),
  propagateFiltersToSubQuery: Joi.boolean().strict().description('Whether to propagate filters to the subquery'),
  ...BaseDimensionWithoutSubQuery
};

const FixedRollingWindow = {
  type: Joi.string().valid('fixed').description("Type of rolling window calculation (e.g., 'to_date' for period-to-date calculations)"),
  trailing: timeInterval.description("Trailing time interval for the rolling window (e.g., '3 months')"),
  leading: timeInterval.description('Leading time interval for the rolling window'),
  offset: Joi.any().valid('start', 'end').description('Whether to align the window at the start or end of the time range')
};

const GranularitySchema = Joi.string().required(); // To support custom granularities

const YearToDate = {
  type: Joi.string().valid('year_to_date').description("Type of rolling window calculation (e.g., 'to_date' for period-to-date calculations)"),
};

const QuarterToDate = {
  type: Joi.string().valid('quarter_to_date').description("Type of rolling window calculation (e.g., 'to_date' for period-to-date calculations)"),
};

const MonthToDate = {
  type: Joi.string().valid('month_to_date').description("Type of rolling window calculation (e.g., 'to_date' for period-to-date calculations)"),
};

const ToDate = {
  type: Joi.string().valid('to_date').description("Type of rolling window calculation (e.g., 'to_date' for period-to-date calculations)"),
  granularity: GranularitySchema.description("Time granularity for period-to-date calculations (e.g., 'year' for YTD, 'quarter' for QTD)"),
};

const forbiddenCurrencyNonNumericMeasure = (value: string) => Joi.forbidden().messages({
  'any.unknown': `"currency" property can only be used with numeric measures, actual type: ${value}`,
});

const BaseMeasure = {
  aliases: Joi.array().items(Joi.string()).description('Other names for this measure'),
  format: measureFormatSchema.description('How the measure is displayed. Use a named format (number, id, percent, currency, abbr, accounting) with optional _N suffix (N=0–6) for decimal precision on supported formats, or any valid d3-format specifier (e.g. $,.2f, .0%). See https://docs.cube.dev/reference/data-modeling/measures#format'),
  currency: Joi.when('type', {
    switch: [
      { is: 'string', then: forbiddenCurrencyNonNumericMeasure('string') },
      { is: 'boolean', then: forbiddenCurrencyNonNumericMeasure('boolean') },
      { is: 'time', then: forbiddenCurrencyNonNumericMeasure('time') },
    ],
    otherwise: currencySchema,
  }).description('ISO 4217 currency code (e.g. USD, EUR, GBP) for this measure. Use with format: currency or currency_N. Case-insensitive. See https://docs.cube.dev/reference/data-modeling/measures#currency'),
  public: Joi.boolean().strict().description('Whether this measure is publicly accessible'),
  // TODO: Deprecate and remove, please use public
  visible: Joi.boolean().strict().description('Same as public; prefer public'),
  // TODO: Deprecate and remove, please use public
  shown: Joi.boolean().strict().description('Whether this measure is visible in the UI'),
  cumulative: Joi.boolean().strict().description('Marks the measure as cumulative in the metadata. Measures with rolling_window are cumulative already'),
  mask: MaskSchema.description(`Value returned instead of the measure's value when an access policy masks it: a static number, boolean or string, or an object with an aggregate SQL expression. Defaults to NULL. In YAML a string mask is read as a Python expression, so quote a literal twice: "'***'"`),
  filters: Joi.array().items(
    Joi.object().keys({
      sql: Joi.func().required().description('SQL condition rows must match to be aggregated')
    })
  ).description('Filters applied to the rows the measure aggregates. Each filter is a SQL condition'),
  title: Joi.string().description('Display title for the measure'),
  description: Joi.string().description('Description of what this measure represents'),
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
  ).description('Rolling window for a cumulative measure: the time range around each date that the measure aggregates'),
  drillMemberReferences: Joi.func().meta({ hidden: true }),
  drillMembers: Joi.func().description('Drill down members for this measure'),
  drillFilters: Joi.array().items(
    Joi.object().keys({
      sql: Joi.func().required().description('SQL condition for the drill-down rows')
    })
  ).description('Filters applied when drilling down into this measure'),
  meta: Joi.any().description('Additional metadata for the measure')
};

const PreAggregationRefreshKeySchema = condition(
  (s) => defined(s.sql),
  Joi.object().keys({
    sql: Joi.func().required().description('SQL query to determine if refresh is needed'),
    // We don't support timezone for this, because it's useless
    // We cannot support cron interval
    every: everyInterval.description("Refresh interval (e.g., '1 hour', '1 day')"),
  }),
  condition(
    (s) => defined(s.every),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval).description("Refresh interval (e.g., '1 hour', '1 day')"),
      timezone: everyCronTimeZone.description('Timezone for refresh scheduling'),
      incremental: Joi.boolean().strict().description('Whether to use incremental refresh'),
      updateWindow: everyInterval.description('Time window for incremental updates')
    }),
    requireOneOf('sql', 'every')
  )
);

const BasePreAggregationWithoutPartitionGranularity = {
  refreshKey: PreAggregationRefreshKeySchema.description('When the pre-aggregation is refreshed: on a schedule (every), when a SQL result changes (sql), or never (immutable)'),
  sqlAlias: Joi.string().optional().description("Short alias for the pre-aggregation's table name, for databases that truncate long names"),
  useOriginalSqlPreAggregations: Joi.boolean().strict().description('Use original SQL pre-aggregations'),
  external: Joi.boolean().strict().description('Whether to store pre-aggregation data externally'),
  scheduledRefresh: Joi.boolean().strict().description('Whether to refresh this pre-aggregation on schedule'),
  indexes: Joi.object().pattern(identifierRegex, Joi.alternatives().try(
    Joi.object().keys({
      sql: Joi.func().required().description('SQL expression for the index')
    }),
    Joi.object().keys({
      columns: Joi.func().required().description('Members whose columns the index covers'),
      type: Joi.any().valid('regular', 'aggregate').description('Index type: regular, or aggregate for an aggregating index'),
    })
  )).description("Indexes of the pre-aggregation's table in Cube Store, by name. An index lists columns, or is a SQL expression"),
  // refreshRange was deprecated
  refreshRangeStart: Joi.object().keys({
    sql: Joi.func().required()
  }).meta({ hidden: true }),
  refreshRangeEnd: Joi.object().keys({
    sql: Joi.func().required()
  }).meta({ hidden: true }),
  // new api
  buildRangeStart: Joi.object().keys({
    sql: Joi.func().required().description('SQL query returning the start of the build range')
  }).description('SQL query returning the first timestamp of the range the pre-aggregation is built for. Takes effect only with partition_granularity'),
  buildRangeEnd: Joi.object().keys({
    sql: Joi.func().required().description('SQL query returning the end of the build range')
  }).description('SQL query returning the last timestamp of the range the pre-aggregation is built for. Takes effect only with partition_granularity'),
  readOnly: Joi.boolean().strict().description('When true, Cube will not create any objects in the data source. Data is consumed directly from the backing stream (e.g. Kafka topic).'),
  streamOffset: Joi.any().valid('earliest', 'latest').description("Controls where Cube Store starts consuming from in the Kafka topic. 'latest' consumes only new messages; 'earliest' replays from the beginning."),
  outputColumnTypes: Joi.array().items(Joi.object().keys({
    member: Joi.func().required().description('Reference to a dimension or measure included in the pre-aggregation'),
    type: Joi.string().required().description('Cube Store column type for this member')
  })).description('Declares the output column types for the Cube Store table in streaming pre-aggregations (Kafka streams mode).'),
};

const BasePreAggregation = {
  ...BasePreAggregationWithoutPartitionGranularity,
  partitionGranularity: Joi.any().valid('hour', 'day', 'week', 'month', 'quarter', 'year').description('Partition granularity for the pre-aggregation'),
};

const AutoRollupSchema = inherit(BasePreAggregation, {
  type: Joi.any().valid('autoRollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
  maxPreAggregations: Joi.number().description('Maximum number of pre-aggregations an auto rollup creates'),
});

const OriginalSqlSchema = condition(
  (s) => defined(s.partitionGranularity) || defined(s.timeDimension) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
      partitionGranularity: BasePreAggregation.partitionGranularity.required().description('Partition granularity for the pre-aggregation'),
      timeDimensionReference: Joi.func().required().meta({ hidden: true }),
      allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
    }),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('originalSql').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
      partitionGranularity: BasePreAggregation.partitionGranularity.required().description('Partition granularity for the pre-aggregation'),
      timeDimension: Joi.func().required().description('Time dimension reference for the pre-aggregation'),
      allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
    })
  ),
  inherit(BasePreAggregationWithoutPartitionGranularity, {
    type: Joi.any().valid('originalSql').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
    uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation')
  }),
);

const ReferencesFields = ['timeDimensionReference', 'rollupReferences', 'measureReferences', 'dimensionReferences', 'segmentReferences'];
const NonReferencesFields = ['timeDimension', 'timeDimensions', 'rollups', 'measures', 'dimensions', 'segments'];

function hasAnyField(fields: string[], s: Record<string, unknown>) {
  return !fields.every((f) => !defined(s[f]));
}

function errorOnMixing(schema: Joi.SchemaLike) {
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
      type: Joi.any().valid('rollupJoin').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      scheduledRefresh: Joi.boolean().valid(false).description('Whether to refresh this pre-aggregation on schedule'),
      granularity: GranularitySchema.description('Time granularity for rollup pre-aggregations'),
      allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
      timeDimensionReference: Joi.func().required().meta({ hidden: true }),
      rollupReferences: Joi.func().required().meta({ hidden: true }),
      measureReferences: Joi.func().meta({ hidden: true }),
      dimensionReferences: Joi.func().meta({ hidden: true }),
      segmentReferences: Joi.func().meta({ hidden: true }),
    }),
    // RollupJoin without references
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      scheduledRefresh: Joi.boolean().valid(false).description('Whether to refresh this pre-aggregation on schedule'),
      granularity: GranularitySchema.description('Time granularity for rollup pre-aggregations'),
      timeDimension: Joi.func().required().description('Time dimension reference for the pre-aggregation'),
      allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
      rollups: Joi.func().required().description('Rollup pre-aggregations to combine: joined for rollup_join, unioned for rollup_lambda'),
      measures: Joi.func().description('Measures to include in the pre-aggregation'),
      dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
      segments: Joi.func().description('Segments to include in the pre-aggregation'),
    })
  ),
  condition(
    (s) => defined(s.rollupReferences),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollupJoin').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      scheduledRefresh: Joi.boolean().valid(false).description('Whether to refresh this pre-aggregation on schedule'),
      rollupReferences: Joi.func().required().meta({ hidden: true }),
      measureReferences: Joi.func().meta({ hidden: true }),
      dimensionReferences: Joi.func().meta({ hidden: true }),
      segmentReferences: Joi.func().meta({ hidden: true }),
    }),
    // RollupJoin without references
    condition(
      (s) => defined(s.rollups),
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollupJoin').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
        scheduledRefresh: Joi.boolean().valid(false).description('Whether to refresh this pre-aggregation on schedule'),
        rollups: Joi.func().required().description('Rollup pre-aggregations to combine: joined for rollup_join, unioned for rollup_lambda'),
        measures: Joi.func().description('Measures to include in the pre-aggregation'),
        dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
        segments: Joi.func().description('Segments to include in the pre-aggregation'),
      }),
      requireOneOf('granularity', 'rollups', 'timeDimension')
    )
  )
);

const RollupLambdaSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension),
  {
    type: Joi.any().valid('rollupLambda').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
    granularity: GranularitySchema.description('Time granularity for rollup pre-aggregations'),
    timeDimension: Joi.func().required().description('Time dimension reference for the pre-aggregation'),
    rollups: Joi.func().required().description('Rollup pre-aggregations to combine: joined for rollup_join, unioned for rollup_lambda'),
    measures: Joi.func().description('Measures to include in the pre-aggregation'),
    dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
    segments: Joi.func().description('Segments to include in the pre-aggregation'),
    unionWithSourceData: Joi.boolean().strict().description("Whether to add data newer than the last partition, read from the data source, to the pre-aggregation's results"),
  },
  {
    type: Joi.any().valid('rollupLambda').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
    rollups: Joi.func().required().description('Rollup pre-aggregations to combine: joined for rollup_join, unioned for rollup_lambda'),
    measures: Joi.func().description('Measures to include in the pre-aggregation'),
    dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
    segments: Joi.func().description('Segments to include in the pre-aggregation'),
    unionWithSourceData: Joi.boolean().strict().description("Whether to add data newer than the last partition, read from the data source, to the pre-aggregation's results"),
  },
);

const RollUpSchema = condition(
  (s) => defined(s.granularity) || defined(s.timeDimension) || defined(s.timeDimensions) || defined(s.timeDimensionReference),
  condition(
    (s) => defined(s.timeDimensionReference),
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      timeDimensionReference: Joi.func().required().meta({ hidden: true }),
      granularity: GranularitySchema.description('Time granularity for rollup pre-aggregations'),
      allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
      measureReferences: Joi.func().meta({ hidden: true }),
      dimensionReferences: Joi.func().meta({ hidden: true }),
      segmentReferences: Joi.func().meta({ hidden: true }),
      uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
    }),
    condition(
      (s) => defined(s.timeDimension),
      // Rollup without References postfix
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
        timeDimension: Joi.func().required().description('Time dimension reference for the pre-aggregation'),
        allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
        granularity: GranularitySchema.description('Time granularity for rollup pre-aggregations'),
        measures: Joi.func().description('Measures to include in the pre-aggregation'),
        dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
        segments: Joi.func().description('Segments to include in the pre-aggregation'),
        uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
      }),
      // Rollup with multiple time dimensions
      inherit(BasePreAggregation, {
        type: Joi.any().valid('rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
        timeDimensions: Joi.array().items(Joi.object().keys({
          dimension: Joi.func().description('Time dimension reference'),
          granularity: GranularitySchema.description('Granularity of this time dimension'),
        })).description('Time dimensions of the pre-aggregation, each with its own granularity. For rollup pre-aggregations only'),
        allowNonStrictDateRangeMatch: Joi.bool().description('Allow non-strict date range matching for queries'),
        measures: Joi.func().description('Measures to include in the pre-aggregation'),
        dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
        segments: Joi.func().description('Segments to include in the pre-aggregation'),
        uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
      })
    )
  ),
  Joi.alternatives().try(
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      measureReferences: Joi.func().meta({ hidden: true }),
      dimensionReferences: Joi.func().meta({ hidden: true }),
      segmentReferences: Joi.func().meta({ hidden: true }),
      uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
    }),
    // Rollup without References postfix
    inherit(BasePreAggregation, {
      type: Joi.any().valid('rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.'),
      measures: Joi.func().description('Measures to include in the pre-aggregation'),
      dimensions: Joi.func().description('Dimensions to include in the pre-aggregation'),
      segments: Joi.func().description('Segments to include in the pre-aggregation'),
      uniqueKeyColumns: Joi.array().items(Joi.string()).description('Unique key columns for the pre-aggregation'),
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
            type: Joi.string().valid('autoRollup', 'originalSql', 'rollupJoin', 'rollup').required().description('Type of pre-aggregation. Legacy values rollupJoin, rollupLambda, and originalSql are also supported.')
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
      sql: Joi.func().required().description('SQL query to determine if refresh is needed'),
      // We don't support timezone for this, because it's useless
      // We cannot support cron interval
      every: everyInterval.description("Refresh interval (e.g., '1 hour', '1 day')"),
    }),
    Joi.object().keys({
      every: Joi.alternatives().try(everyInterval, everyCronInterval).required().description("Refresh interval (e.g., '1 hour', '1 day')"),
      timezone: everyCronTimeZone.description('Timezone for refresh scheduling'),
    })
  ),
  condition(
    (s) => defined(s.immutable),
    Joi.object().keys({
      immutable: Joi.boolean().strict().required().description('Whether the data never changes, so the cache is never refreshed')
    }),
    requireOneOf('every', 'sql', 'immutable')
  )
);

const measureType = Joi.string().valid(
  'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'countDistinctApprox'
);

const measureTypeWithCount = Joi.string().valid(
  'count', 'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'countDistinctApprox'
);

const multiStageMeasureType = Joi.string().valid(
  'count', 'number', 'string', 'boolean', 'time', 'sum', 'avg', 'min', 'max', 'countDistinct', 'countDistinctApprox', 'numberAgg',
  'rank'
);

const timeShiftItemRequired = Joi.object({
  timeDimension: Joi.func().required().description('The time dimension to apply the shift to. Optional with a single entry; Cube then infers it'),
  interval: regexTimeInterval.required().description("The time interval to shift by (e.g., '1 year', '3 months', '1 week')"),
  type: Joi.string().valid('next', 'prior').required().description("Direction of time shift: 'prior' for past periods, 'next' for future periods"),
});

const timeShiftItemOptional = Joi.object({
  timeDimension: Joi.func().description('The time dimension to apply the shift to. Optional with a single entry; Cube then infers it'), // not required
  interval: regexTimeInterval.description("The time interval to shift by (e.g., '1 year', '3 months', '1 week')"),
  name: identifier.description("Name of a time shift declared on a calendar cube time dimension (e.g., 'prior_fiscal_year')"),
  type: Joi.string().valid('next', 'prior').description("Direction of time shift: 'prior' for past periods, 'next' for future periods"),
})
  .xor('name', 'interval')
  .and('interval', 'type');

// Top-level predicate inside `filter.include`: same shape as a query-time
// filter. `member` and `values` are plain string/array only — neither
// `filter.include[*].member` nor `filter.include[*].values` is covered by
// `CubePropContextTranspiler.transpiledFieldsPatterns`, so a function form
// here would never receive the `CUBE`/`SECURITY_CONTEXT` arguments and would
// fail at runtime. Use the existing `accessPolicy.rowLevel.filters` if you
// need dynamic predicates resolved against the security context.
const MultiStageIncludeMemberFilterSchema = Joi.object().keys({
  member: Joi.string().required().description('Member the filter applies to'),
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
  ).required().description('Filter operator, as in a query filter'),
  values: Joi.when('operator', {
    is: Joi.valid('set', 'notSet'),
    then: Joi.array().optional(),
    otherwise: Joi.array().required()
  }).description('Values to compare with. Not needed for set and notSet.')
});

const MultiStageIncludeConditionSchema = Joi.object().keys({
  or: Joi.array().items(MultiStageIncludeMemberFilterSchema, Joi.link('...').description('Multi-stage include condition')).description('Predicates of which at least one must match'),
  and: Joi.array().items(MultiStageIncludeMemberFilterSchema, Joi.link('...').description('Multi-stage include condition')).description('Predicates that must all match'),
}).xor('or', 'and');

const MultiStageFilter = Joi.object().keys({
  mode: Joi.string().valid('relative', 'fixed').description('How the override composes along a multi-stage chain: relative (default) or fixed'),
  exclude: Joi.func().description('Members whose query filters are dropped at the inner stage'),
  keepOnly: Joi.func().description('Members whose query filters are kept; all other inherited filters are dropped'),
  include: Joi.array().items(
    MultiStageIncludeMemberFilterSchema,
    MultiStageIncludeConditionSchema
  ).description('Additional filters applied at the inner stage regardless of the query'),
}).nand('exclude', 'keepOnly');

const MultiStageGrain = Joi.object().keys({
  exclude: Joi.func().description("Group the inner stage by the query's dimensions minus these dimensions. Use for ranking within groups."),
  keepOnly: Joi.func().description("Group the inner stage by only these dimensions, ignoring the query's dimensions. Use for fixed-grain calculations such as percent of total."),
  include: Joi.func().description("Group the inner stage by the query's dimensions plus these dimensions. Use for nested aggregates (an aggregate of an aggregate)."),
}).nand('exclude', 'keepOnly');

const CaseSchema = Joi.object().keys({
  when: Joi.array().items(Joi.object().keys({
    sql: Joi.func().required().description('SQL expression when condition matches'),
    label: Joi.alternatives([
      Joi.string(),
      Joi.object().keys({
        sql: Joi.func().required().description('SQL expression for the label')
      })
    ]).description('Value to return when the condition matches: a string, or an object with a SQL expression')
  })).description('Array of condition/value pairs'),
  else: Joi.object().keys({
    label: Joi.alternatives([
      Joi.string(),
      Joi.object().keys({
        sql: Joi.func().required().description('SQL expression for the label')
      })
    ]).description('Value to return when no condition matches: a string, or an object with a SQL expression')
  }).description('Default value when no conditions match')
}).required();

const SwitchCaseSchema = Joi.object().keys({
  switch: Joi.func().required().description('Dimension reference to evaluate for the switch condition'),
  when: Joi.array().items(Joi.object().keys({
    value: Joi.string().required().description('Value to match against the switch dimension'),
    sql: Joi.func().required().description('SQL expression when condition matches')
  })).description('Array of condition/value pairs'),
  else: Joi.object().keys({
    sql: Joi.func().required().description('SQL expression for default case')
  }).description('Default value when no conditions match')
}).required();

const CaseVariants = Joi.alternatives().try(
  CaseSchema,
  SwitchCaseSchema
);

const MeasuresSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().conditional(Joi.ref('.multiStage'), [
  {
    is: true,
    then: inherit(BaseMeasure, {
      multiStage: Joi.boolean().strict().description('Enable multi-stage calculations for this measure. Required for time_shift, group_by, reduce_by, and rank type measures.'),
      type: multiStageMeasureType.required().description('Type of the measure'),
      sql: Joi.func().description('SQL expression for the measure'), // TODO .required(),
      case: CaseVariants.description("Conditional values: when/else pairs, or a switch on a dimension's values"),
      groupBy: Joi.func().description('Dimensions to group by for fixed dimension calculations (e.g., calculate revenue per country regardless of other groupings)'),
      reduceBy: Joi.func().description('Dimensions to reduce by for ranking calculations. The rank will be computed within each unique combination of non-reduced dimensions.'),
      addGroupBy: Joi.func().description('Additional dimensions to include in the inner aggregation stage while preserving query dimensions. Used for nested aggregate calculations in multi-stage measures.'),
      filter: MultiStageFilter.description('Overrides the filters the inner aggregation stage of a multi-stage measure inherits from the query: exclude drops, keep_only keeps only, include adds. exclude and keep_only are mutually exclusive'),
      grain: MultiStageGrain.description("Grain of a multi-stage measure's inner aggregation stage. Supersedes group_by (keep_only), reduce_by (exclude) and add_group_by (include). keep_only and exclude are mutually exclusive"),
      timeShift: Joi.alternatives().conditional(Joi.array().length(1), {
        then: Joi.array().items(timeShiftItemOptional),
        otherwise: Joi.array().items(timeShiftItemRequired)
      }).description('Time shift configurations for period-over-period comparisons (e.g., compare to prior year). A single entry may reference a shift declared on a calendar cube time dimension by name; two or more entries must each spell out time_dimension, interval and type.'),
      // TODO validate for order window functions
      orderBy: Joi.array().items(Joi.object().keys({
        sql: Joi.func().required().description("SQL expression or measure reference to order by (e.g., '{revenue}')"),
        dir: Joi.string().valid('asc', 'desc').description("Sort direction: 'asc' for ascending, 'desc' for descending")
      })).description('Order by configurations for ranking measures. Determines the sort order for rank calculation.'),
    })
  }
]).conditional(
  Joi.ref('.type'), [
    {
      is: 'count',
      then: inherit(BaseMeasure, {
        type: Joi.any().valid('count').required().description('Type of the measure'),
        sql: Joi.func().description('SQL expression for the measure'),
      })
    },
    {
      is: measureType,
      then: inherit(BaseMeasure, {
        sql: Joi.func().required().description('SQL expression for the measure'),
        type: measureType.required().description('Type of the measure')
      }),
      otherwise: Joi.object().keys({
        type: measureTypeWithCount.required().description('Type of the measure')
      })
    }
  ]
));

const CalendarTimeShiftItem = Joi.alternatives().try(
  Joi.object({
    name: identifier.required().description("Name identifier for this time shift (e.g., 'month_ago', 'year_ago')"),
    interval: regexTimeInterval.required().description("The time interval to shift by (e.g., '1 month', '1 year')"),
    type: Joi.string().valid('next', 'prior').required().description("Direction of time shift: 'prior' for past periods, 'next' for future periods"),
    sql: Joi.forbidden()
  }),
  Joi.object({
    name: identifier.required().description("Name identifier for this time shift (e.g., 'month_ago', 'year_ago')"),
    sql: Joi.func().required().description("Custom SQL expression to override default interval behavior (e.g., '{CUBE}.month_ago')"),
    interval: Joi.forbidden(),
    type: Joi.forbidden()
  }),
  Joi.object({
    interval: regexTimeInterval.required().description("The time interval to shift by (e.g., '1 month', '1 year')"),
    type: Joi.string().valid('next', 'prior').required().description("Direction of time shift: 'prior' for past periods, 'next' for future periods"),
    sql: Joi.func().required().description("Custom SQL expression to override default interval behavior (e.g., '{CUBE}.month_ago')"),
    name: Joi.forbidden()
  })
);

const SwitchDimension = Joi.object({
  type: Joi.string().valid('switch').required().description('Type of the dimension'),
  values: Joi.array().items(Joi.string()).min(1)
    .required()
    .description("Predefined set of allowed values for switch dimensions. Required when type is 'switch'.")
});

const DimensionsSchema = Joi.object().pattern(identifierRegex, Joi.alternatives().conditional(Joi.ref('.type'), {
  is: 'switch',
  then: SwitchDimension,
  // Alternatives are tried in order and each miss builds a full error report, so the plain `sql`
  // dimension, which most dimensions are, goes first. Order does not change what passes.
  otherwise: Joi.alternatives().try(
    inherit(BaseDimension, {
      sql: Joi.func().required().description('SQL expression for the dimension'),
    }),
    inherit(BaseDimensionWithoutSubQuery, {
      case: CaseVariants.required().description('Case statement for conditional dimension values'),
      multiStage: Joi.boolean().strict().description('Whether the dimension is computed in a multi-stage calculation'),
    }),
    inherit(BaseDimensionWithoutSubQuery, {
      latitude: Joi.object().keys({
        sql: Joi.func().required().description('SQL expression for the coordinate')
      }).required().description('Latitude of a geo dimension. Use together with longitude instead of sql.'),
      longitude: Joi.object().keys({
        sql: Joi.func().required().description('SQL expression for the coordinate')
      }).required().description('Longitude of a geo dimension. Use together with latitude instead of sql.')
    }),
    inherit(BaseDimension, {
      multiStage: Joi.boolean().valid(true).description('Whether the dimension is computed in a multi-stage calculation. Required for add_group_by and filter'),
      sql: Joi.func().required().description('SQL expression for the dimension'),
      addGroupBy: Joi.func().description('Dimensions to add to the inner aggregation stage of this multi-stage dimension'),
      filter: MultiStageFilter.description('Overrides the filters the inner aggregation stage of this multi-stage dimension inherits from the query: exclude drops, keep_only keeps only, include adds'),
    }),
    // TODO should be valid only for calendar cubes, but this requires significant refactoring
    // of all schemas. Left for the future when we'll switch to zod.
    inherit(BaseDimensionWithoutSubQuery, {
      type: Joi.any().valid('time').required().description('Type of the dimension'),
      sql: Joi.func().required().description('SQL expression for the dimension'),
      timeShift: Joi.array().items(CalendarTimeShiftItem).description('Time shift configurations for calendar cube time dimensions (e.g., month_ago, year_ago)'),
    })
  )
}));

const SegmentsSchema = Joi.object().pattern(identifierRegex, Joi.object().keys({
  aliases: Joi.array().items(Joi.string()).description('Other names for this segment'),
  sql: Joi.func().required().description('SQL expression for the segment filter'),
  title: Joi.string().description('Display title for the segment'),
  description: Joi.string().description('Description of what this segment represents'),
  meta: Joi.any().description('Additional metadata for the segment'),
  shown: Joi.boolean().strict().description('Whether this segment is visible in the UI'),
  public: Joi.boolean().strict().description('Whether this segment is publicly accessible'),
}));

const PolicyFilterSchema = Joi.object().keys({
  member: Joi.func().required().description('Reference to the dimension or measure to filter on'),
  memberReference: Joi.string().meta({ hidden: true }),
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
  ).required().description('Filter operator'),
  values: Joi.when('operator', {
    is: Joi.valid('set', 'notSet'),
    then: Joi.func().optional(),
    otherwise: Joi.func().required()
  }).description('Filter values (can use security context with { securityContext.property } syntax)')
});

const PolicyFilterConditionSchema = Joi.object().keys({
  or: Joi.array().items(PolicyFilterSchema, Joi.link('...').description('Filter Condition schema')).description('Filters of which at least one must match'),
  and: Joi.array().items(PolicyFilterSchema, Joi.link('...').description('Filter Condition schema')).description('Filters that must all match'),
}).xor('or', 'and');

const MemberLevelPolicySchema = Joi.object().keys({
  includes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string())
  ]).description('Members to include in access'),
  excludes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string().required())
  ]).description('Members to exclude from access'),
  includesMembers: Joi.array().items(Joi.string().required()).meta({ hidden: true }),
  excludesMembers: Joi.array().items(Joi.string().required()).meta({ hidden: true }),
})
  // `includes` defaults to '*' in CubeEvaluator.prepareAccessPolicy, so an empty
  // memberLevel grants every member — the opposite of how it reads.
  .or('includes', 'excludes')
  .messages({
    'object.missing': '{{#label}} must define either includes or excludes. An empty memberLevel grants access to all members: spell that out with includes: \'*\', or use excludes to grant all but some. A member granted by memberLevel is unmasked on every row the policy grants, so a member that must always be masked belongs in excludes'
  });

const MemberMaskingPolicySchema = Joi.object().keys({
  includes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string())
  ]).description("Members to mask, or '*' for all members"),
  excludes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(Joi.string().required())
  ]).description('Members not to mask'),
  includesMembers: Joi.array().items(Joi.string().required()).meta({ hidden: true }),
  excludesMembers: Joi.array().items(Joi.string().required()).meta({ hidden: true }),
});

const RowLevelPolicySchema = Joi.object().keys({
  filters: Joi.array().items(PolicyFilterSchema, PolicyFilterConditionSchema).description('Filters to apply for row-level security'),
  allowAll: Joi.boolean().valid(true).strict().description("Grant access to all rows. Mutually exclusive with 'filters'"),
}).xor('filters', 'allowAll');

const GroupPolicySchema = Joi.object().keys({
  group: Joi.string().description("The user group this policy applies to. Use '*' to match all users"),
  groups: Joi.array().items(Joi.string()).description('The user groups this policy applies to. An array of group names.'),
  memberLevel: MemberLevelPolicySchema.description('Members the policy grants access to'),
  memberMasking: MemberMaskingPolicySchema.description('Member-level data masking configuration for sensitive fields'),
  rowLevel: RowLevelPolicySchema.description('Rows the policy grants access to'),
  conditions: Joi.array().items(Joi.object().keys({
    if: Joi.func().required().description('Jinja condition to evaluate'),
  })).description('Conditional access policies evaluated at runtime'),
})
  .nand('group', 'groups') // Cannot have both group and groups
  .or('group', 'groups') // Must have at least one
  .with('memberMasking', 'memberLevel'); // memberMasking requires memberLevel

/* *****************************
 * ATTENTION:
 * In case of adding/removing/changing any Joi.func() field that needs to be transpiled,
 * please run 'cube-validator.test.ts' - transpiledFieldsPatterns
 * and update CubePropContextTranspiler.transpiledFieldsPatterns
 **************************** */

const hierarchySchema = Joi.object().pattern(identifierRegex, Joi.object().keys({
  title: Joi.string().description('Human-readable display title for the hierarchy'),
  public: Joi.boolean().strict().description('Whether this hierarchy is publicly accessible'),
  levels: Joi.func().description("Dimension names in hierarchical order (can include joined dimensions like 'orders.status')")
}));

const baseSchema = {
  name: identifier.description('Unique name of the cube or view'),
  refreshKey: CubeRefreshKeySchema.description('When the cache of the cube or view is refreshed: on a schedule (every), when a SQL result changes (sql), or never (immutable)'),
  fileName: Joi.string().required().meta({ setByCompiler: true }),
  extends: Joi.func().description('Name of the cube or view this one extends'),
  allDefinitions: Joi.func().meta({ setByCompiler: true }), // Helpers function for extending
  rawFolders: Joi.func().meta({ setByCompiler: true }), // Helpers function for extending
  rawCubes: Joi.func().meta({ setByCompiler: true }), // Helpers function for extending
  title: Joi.string().description('Display title'),
  sqlAlias: Joi.string().description('Short alias used in generated SQL, for databases that truncate long names'),
  dataSource: Joi.string().description('Name of the data source to read from'),
  description: Joi.string().description('Description of what this cube or view represents'),
  rewriteQueries: Joi.boolean().strict().description('Whether to rewrite queries for this cube'),
  shown: Joi.boolean().strict().description('Whether this cube or view is visible in the UI'),
  public: Joi.boolean().strict().description('Whether this cube or view is publicly accessible'),
  meta: Joi.any().description('Additional custom metadata'),
  joins: Joi.alternatives([
    Joi.object().pattern(identifierRegex, Joi.object().keys({
      sql: Joi.func().required().description('SQL join condition'),
      relationship: Joi.any().valid(
        'belongsTo', 'belongs_to', 'many_to_one', 'manyToOne',
        'hasMany', 'has_many', 'one_to_many', 'oneToMany',
        'hasOne', 'has_one', 'one_to_one', 'oneToOne'
      ).required().description('Type of join relationship: one_to_one, one_to_many or many_to_one. The older names has_one, has_many and belongs_to also work')
    })),
    Joi.array().items(Joi.object().keys({
      name: identifier.required().description('Name of the cube to join'),
      alias: identifier.description('Own name for this join, so the same cube can be joined more than once on different columns'),
      sql: Joi.func().required().description('SQL join condition'),
      relationship: Joi.any().valid(
        'belongsTo', 'belongs_to', 'many_to_one', 'manyToOne',
        'hasMany', 'has_many', 'one_to_many', 'oneToMany',
        'hasOne', 'has_one', 'one_to_one', 'oneToOne'
      ).required().description('Type of join relationship: one_to_one, one_to_many or many_to_one. The older names has_one, has_many and belongs_to also work')
    }))
  ]).description('Joins to other cubes'),
  measures: MeasuresSchema.description('Measure definitions'),
  dimensions: DimensionsSchema.description('Dimension definitions'),
  segments: SegmentsSchema.description('Segment definitions'),
  preAggregations: PreAggregationsAlternatives.description('Pre-aggregation definitions for performance optimization'),
  accessPolicy: Joi.array().items(GroupPolicySchema.required()).description('Access policies that control who sees which members and rows'),
  hierarchies: hierarchySchema.description('Hierarchies for drill-down functionality'),
};

const cubeSchema = inherit(baseSchema, {
  sql: Joi.func().description("SQL query to define the cube's data source"),
  sqlTable: Joi.func().description('Database table name for the cube'),
  calendar: Joi.boolean().strict().description('Designates this cube as a calendar cube, enabling custom time shifts and granularities for fiscal calendars and other non-standard time calculations.'),
}).xor('sql', 'sqlTable').messages({
  'object.xor': 'You must use either sql or sqlTable within a model, but not both'
});

const folderSchema = Joi.object().keys({
  name: Joi.string().required().description('Name of the folder'),
  includes: Joi.alternatives([
    Joi.string().valid('*'),
    Joi.array().items(
      Joi.alternatives([
        Joi.string().required(),
        Joi.object().keys({
          joinPath: Joi.func().required().description('Join path of a cube in the view; the folder includes the members the view takes from it'),
        }),
        Joi.link('#folderSchema'), // Can contain nested folders
      ]),
    ),
  ]).required().description('Members to include in this folder (can be member names or nested folders)'),
}).id('folderSchema');

// A view group's `includes`: a function, or an array of view references
// (string/function) and nested view group definitions (resolved via the shared
// `#nestedViewGroupSchema` link). Shared between the top-level and nested
// schemas; the nested schema makes it `.required()`.
const viewGroupIncludesSchema = Joi.alternatives([
  Joi.func(),
  Joi.array().items(
    Joi.alternatives([
      Joi.string().required(),
      Joi.func(),
      Joi.link('#nestedViewGroupSchema'), // Can contain nested view groups
    ]),
  ),
]);

// A nested view group authored inside another group's `includes`. Unlike a
// top-level group, it MUST use `includes` (no legacy `views`), and `fileName`
// is meaningless here, so neither is accepted. Enforcing this at validation
// time prevents nested groups from being silently dropped by the evaluator.
const nestedViewGroupSchema = Joi.object().keys({
  name: Joi.string().required().description('Name of the nested view group'),
  title: Joi.string().description('Display title for the view group'),
  description: Joi.string().description('Description of what this view group represents'),
  includes: viewGroupIncludesSchema.required().description('Views and nested view groups in this group'),
})
  .id('nestedViewGroupSchema');

const viewGroupSchema = Joi.object().keys({
  name: Joi.string().required().description('Unique name of the view group'),
  title: Joi.string().description('Display title for the view group'),
  description: Joi.string().description('Description of what this view group represents'),
  // Legacy way of including views into a group, kept for backward compatibility.
  views: Joi.alternatives([Joi.array().items(Joi.string().required()), Joi.func()]).description('Names of the views in this group. Older spelling of includes'),
  // Preferred way of including views (and nested view groups) into a group.
  includes: viewGroupIncludesSchema.description('Views to include in this group (can be view names or nested view groups)'),
  fileName: Joi.string().meta({ setByCompiler: true }),
})
  .oxor('views', 'includes')
  .messages({
    'object.oxor': 'View group must use either "views" or "includes", but not both'
  })
  // Register the nested schema so the `#nestedViewGroupSchema` link above resolves.
  .shared(nestedViewGroupSchema)
  .id('viewGroupSchema');

const ViewDefaultFilterSchema = Joi.object().keys({
  member: Joi.func().required().description('Name of the dimension to filter on; must be exposed by the view'),
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
  ).required().description('Filter operator'),
  values: Joi.when('operator', {
    is: Joi.valid('set', 'notSet'),
    then: Joi.func().optional(),
    otherwise: Joi.func().required()
  }).description('Filter values; required for all operators except set and notSet'),
  unless: Joi.func().description('Members that, when referenced in a query, release this default filter'),
});

const viewSchema = inherit(baseSchema, {
  isView: Joi.boolean().strict().meta({ setByCompiler: true }),
  viewGroup: Joi.alternatives([Joi.string(), Joi.func()]).description('Name of the view group this view belongs to'),
  viewGroups: Joi.alternatives([Joi.array().items(Joi.string().required()), Joi.func()]).description('Names of the view groups this view belongs to'),
  cubes: Joi.array().items(
    Joi.object().keys({
      joinPath: Joi.func().required().description("The path to the cube, including any joins. Use cube name for direct access (e.g., 'Events') or dot notation for joined cubes (e.g., 'Events.Users')"),
      prefix: Joi.boolean().description('Whether to prefix the included members with the cube name'),
      split: Joi.boolean().description("Whether to expose this cube's members in a separate view named <view>_<cube>"),
      alias: Joi.string().description('Override the name of the cube'),
      includes: Joi.alternatives([
        Joi.string().valid('*'),
        Joi.array().items(Joi.alternatives([
          Joi.string().required(),
          Joi.object().keys({
            name: identifier.required().description('Name of the member to include'),
            alias: identifier.description('Name the member gets in the view'),
            title: Joi.string().description('Override the display title of the included member'),
            description: Joi.string().description('Override the description of the included member'),
            format: formatSchema.description('Override the display format of the included member'),
            meta: Joi.any().description('Override the metadata of the included member as a whole'),
          })
        ]))
      ]).required().description("Specify which members to include from the cube. Use '*' for all members, or an array of member names/objects with aliases"),
      excludes: Joi.array().items(Joi.string().required()).description('Members to exclude from the cube'),
    }).oxor('split', 'prefix').messages({
      'object.oxor': 'Using split together with prefix is not supported'
    })
  ).description('List of cubes to include in the view'),
  folders: Joi.array().items(folderSchema).description('Folders for organizing view members into logical groups'),
  defaultFilters: Joi.array().items(ViewDefaultFilterSchema).description('Default filters applied to every query against the view'),
});

function formatErrorMessageFromDetails(explain: Map<string, string>, d: Joi.ValidationErrorItem) {
  if (d?.context?.details) {
    d?.context?.details?.forEach((d2: Joi.ValidationErrorItem) => formatErrorMessageFromDetails(explain, d2));
  } else if (d?.message) {
    const key = d?.context?.message || d?.message;
    const val = key?.replace(`"${d.context?.label}"`, `(${d.context?.label} = ${d.context?.value})`);
    explain.set(key, val);
  }
}

function formatErrorMessage(error: Joi.ValidationError) {
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

// Walks Joi's internal schema object graph, which has no public type.
function collectFunctionFieldsPatterns(patterns: Set<string>, path: string, o: any) {
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
      if (k !== '$_root' && o.hasOwnProperty(k)) {
        collectFunctionFieldsPatterns(patterns, newPath, o[k]);
      }
    }
  }
}

export function functionFieldsPatterns(): string[] {
  const functionPatterns = new Set<string>();
  collectFunctionFieldsPatterns(functionPatterns, '', { ...cubeSchema, ...viewSchema });
  return Array.from(functionPatterns);
}

// Joi's own description of the model schemas, for generating the model-language manifest.
// The JSON round trip drops the functions `.custom()` keeps in its args, leaving plain data.
export function describeModelSchemas(): Record<'cube' | 'view' | 'viewGroup', Joi.Description> {
  return JSON.parse(JSON.stringify({
    cube: cubeSchema.describe(),
    view: viewSchema.describe(),
    viewGroup: viewGroupSchema.describe(),
  }));
}

// Model objects come from the VM context, whose Object.prototype isn't this realm's, so the
// prototype walk stops at whichever realm's Object.prototype it reaches
const isRootPrototype = (o: object) => Object.getPrototypeOf(o) === null && Object.prototype.hasOwnProperty.call(o, 'hasOwnProperty');

/**
 * Everything the schema can see: inherited and non-enumerable keys, getter values, and function
 * source. Keys are sorted because Jinja doesn't keep their order stable.
 */
function definitionFingerprint(definition: unknown): string {
  const hash = crypto.createHash('sha1');
  const path = new Set<object>();

  const write = (value: unknown) => {
    if (typeof value === 'function') {
      const source = value.toString();
      hash.update(`f${source.length}:${source}`);
    } else if (value === null || typeof value !== 'object') {
      const text = String(value);
      hash.update(`${value === null ? 'null' : typeof value}${text.length}:${text}`);
    } else if (!Array.isArray(value) && Object.prototype.toString.call(value) !== '[object Object]') {
      // Dates, regexps and the like carry their state outside own properties
      const text = `${Object.prototype.toString.call(value)}${String(value)}`;
      hash.update(`o${text.length}:${text}`);
    } else if (path.has(value)) {
      hash.update('cycle;');
    } else {
      path.add(value);
      if (Array.isArray(value)) {
        hash.update(`[${value.length}`);
        value.forEach(write);
        hash.update(']');
      } else {
        hash.update('{');
        // A key shadowed further down the chain reads the same value, so it is hashed once, at
        // the depth it first appears
        const seen = new Set<string>();
        let depth = 0;

        for (let o: object | null = value; o && !isRootPrototype(o); o = Object.getPrototypeOf(o), depth++) {
          for (const key of Object.getOwnPropertyNames(o).sort()) {
            if (!seen.has(key)) {
              seen.add(key);
              hash.update(`${depth}.${key.length}:${key}=`);
              write((value as Record<string, unknown>)[key]);
            }
          }
        }
        hash.update('}');
      }
      path.delete(value);
    }
  };

  write(definition);
  return hash.digest('hex');
}

// Fingerprints of definitions that passed the schema. Validation depends on nothing else, so
// tenants compiling the same model, and recompiles of an unchanged cube, skip it. Failures are
// always validated again so their errors are reported afresh.
const validDefinitions = new LRUCache<string, true>({ max: 50000 });

export class CubeValidator implements CompilerInterface {
  protected readonly validCubes: Map<string, boolean> = new Map();

  public constructor(
    protected readonly cubeSymbols: CubeSymbols
  ) {
  }

  public compile(cubes: unknown[], errorReporter: ErrorReporter) {
    return this.cubeSymbols.cubeList.map(
      (v) => this.validate(this.cubeSymbols.getCubeDefinition(v.name), errorReporter.inContext(`${v.name} cube`))
    );
  }

  // Unvalidated model input: it only conforms to CubeDefinition once validation passes.
  public validate(cube: any, errorReporter: ErrorReporter) {
    const options = {
      nonEnumerables: true,
      abortEarly: false, // This will allow all errors to be reported, not just the first one
    };
    const fingerprint = definitionFingerprint(cube);
    let result: Joi.ValidationResult;
    if (validDefinitions.get(fingerprint)) {
      result = { value: cube, error: undefined };
    } else {
      result = cube.isView ? viewSchema.validate(cube, options) : cubeSchema.validate(cube, options);
      if (result.error == null) {
        validDefinitions.set(fingerprint, true);
      }
    }

    let valid = result.error == null;

    if (cube.isView) {
      // We need to verify that leaf cubes in view are present only once
      this.validateUniqueLeafCubes(cube.name, cube.cubes, errorReporter);
    } else if (!this.validateGranularitySql(cube, errorReporter)) {
      valid = false;
    }

    if (result.error != null) {
      errorReporter.error(formatErrorMessage(result.error));
    }

    if (valid) {
      this.validCubes.set(cube.name, true);
    }

    return result;
  }

  // Reported outside the cube schema so the message stands on its own: a
  // granularity rejected by the schema is listed among the reasons every other
  // dimension alternative failed, which buries it.
  private validateGranularitySql(cube: CubeDefinition, errorReporter: ErrorReporter): boolean {
    let valid = true;

    for (const [dimensionName, dimension] of Object.entries<any>(cube.dimensions || {})) {
      for (const [name, granularity] of Object.entries<any>(dimension?.granularities || {})) {
        // Predefined names are resolved case-insensitively, so `Week` names a
        // granularity that resolves and must keep doing so.
        if (granularity?.sql && !isPredefinedGranularity(name.toLowerCase())) {
          errorReporter.error(
            `dimensions.${dimensionName}.granularities.${name}: a granularity defined with 'sql' must be named after one of the predefined granularities (${PREDEFINED_GRANULARITY_NAMES.join(', ')}). Define '${name}' with 'interval' instead`
          );
          valid = false;
        }
      }
    }

    return valid;
  }

  public validateViewGroup(viewGroup: ViewGroupInput, errorReporter: ErrorReporter) {
    const options = {
      nonEnumerables: true,
      abortEarly: false, // This will allow all errors to be reported, not just the first one
    };
    const result = viewGroupSchema.validate(viewGroup, options);

    if (result.error != null) {
      errorReporter
        .inContext(`${viewGroup?.name} view group`)
        .error(formatErrorMessage(result.error));
    }

    return result;
  }

  /**
   * Validates that each cube appears only once within each root path in a view.
   * This ensures that there is only one path to reach each cube within each root's join graph.
   * For example: 'a.b.c' + 'd.b.c' is valid (different roots), but 'a.b.c' + 'a.d.c' is invalid (same root 'a').
   */
  private validateUniqueLeafCubes(viewName: string, cubesArray: any[], errorReporter: ErrorReporter) {
    if (!cubesArray || cubesArray.length === 0) {
      return;
    }

    const rootCubePaths = new Map<string, Map<string, Set<string>>>();

    for (const cube of cubesArray) {
      try {
        const fullPath = this.cubeSymbols.evaluateReferences(null, cube.joinPath as () => ToString, { collectJoinHints: true });
        const split = fullPath.split('.');
        const rootCube = split[0];

        if (!rootCubePaths.has(rootCube)) {
          rootCubePaths.set(rootCube, new Map<string, Set<string>>());
        }

        const cubesJoinPaths = rootCubePaths.get(rootCube)!;

        // A cube reached through a join alias, or below one, is an instance of
        // its own: its path identifies it, so two aliases may reach one cube
        let current = rootCube;
        let aliased = false;

        for (let i = 0; i < split.length; i++) {
          const path = split.slice(0, i + 1).join('.');
          if (i > 0) {
            const aliasTarget = this.cubeSymbols.joinAliasTarget(current, split[i]);
            aliased = aliased || aliasTarget !== undefined;
            current = aliasTarget ?? split[i];
          }
          const cubeName = aliased ? path : split[i];

          if (!cubesJoinPaths.has(cubeName)) {
            cubesJoinPaths.set(cubeName, new Set<string>());
          }
          cubesJoinPaths.get(cubeName)!.add(path);
        }
      } catch (e) {
        // Ignore errors during joinPath evaluation - cube may not be defined yet
      }
    }

    // Check for cubes with multiple paths within each root
    for (const [rootCube, cubesJoinPaths] of rootCubePaths.entries()) {
      for (const [cubeName, paths] of cubesJoinPaths.entries()) {
        if (paths.size > 1) {
          const pathList = Array.from(paths).map(path => `'${path}'`).join(', ');
          errorReporter.error(`Views can't define multiple join paths to the same cube. View '${viewName}' has multiple paths to '${cubeName}' within root '${rootCube}': ${pathList}. Use extends to create a child cube and reference it instead`);
        }
      }
    }
  }

  public isCubeValid(cube: CubeDefinition): boolean {
    return this.validCubes.get(cube.name) ?? cube.isSplitView ?? false;
  }
}
