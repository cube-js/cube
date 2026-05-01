import { format as d3Format } from 'd3-format';
import { timeFormat } from 'd3-time-format';
import { getD3NumericLocale } from './format-d3-numeric-locale.js';

import type { DimensionFormat, MeasureFormat, TCubeMemberType } from './types.js';

// Default d3-format specifiers — aligned with the named _2 formats
// (number_2, currency_2, percent_2) in NAMED_NUMERIC_FORMATS below.
// The `~` modifier trims insignificant trailing zeros so values like 1234
// render as "1,234" rather than "1,234.00".
const DEFAULT_NUMBER_FORMAT = ',.2~f';
const DEFAULT_CURRENCY_FORMAT = '$,.2~f';
const DEFAULT_PERCENT_FORMAT = '.2~%';

const DEFAULT_ID_FORMAT = '.0f';

// Predefined named numeric formats and their d3-format specifiers.
// Mirrors NAMED_NUMERIC_FORMATS in @cubejs-backend/schema-compiler — keep in sync.
// Bare names ('number', 'percent', 'currency') keep their existing API contract
// and are routed through the explicit cases below; only the _N suffixed variants
// (and bare 'abbr', 'accounting', 'decimal') are resolved through this map.
const NAMED_NUMERIC_FORMATS: Record<string, string> = {
  number_0: ',.0f',
  number_1: ',.1~f',
  number_2: ',.2~f',
  number_3: ',.3~f',
  number_4: ',.4~f',
  number_5: ',.5~f',
  number_6: ',.6~f',

  percent_0: '.0%',
  percent_1: '.1~%',
  percent_2: '.2~%',
  percent_3: '.3~%',
  percent_4: '.4~%',
  percent_5: '.5~%',
  percent_6: '.6~%',

  currency_0: '$,.0f',
  currency_1: '$,.1~f',
  currency_2: '$,.2~f',
  currency_3: '$,.3~f',
  currency_4: '$,.4~f',
  currency_5: '$,.5~f',
  currency_6: '$,.6~f',

  decimal: ',.2~f',
  decimal_0: ',.0f',
  decimal_1: ',.1~f',
  decimal_2: ',.2~f',
  decimal_3: ',.3~f',
  decimal_4: ',.4~f',
  decimal_5: ',.5~f',
  decimal_6: ',.6~f',

  abbr: '.2~s',
  abbr_0: '.0~s',
  abbr_1: '.1~s',
  abbr_2: '.2~s',
  abbr_3: '.3~s',
  abbr_4: '.4~s',
  abbr_5: '.5~s',
  abbr_6: '.6~s',

  accounting: '(,.2~f',
  accounting_0: '(,.0f',
  accounting_1: '(,.1~f',
  accounting_2: '(,.2~f',
  accounting_3: '(,.3~f',
  accounting_4: '(,.4~f',
  accounting_5: '(,.5~f',
  accounting_6: '(,.6~f',
};

function detectLocale() {
  try {
    return new Intl.NumberFormat().resolvedOptions().locale;
  } catch (e) {
    console.warn('Failed to detect locale', e);

    return 'en-US';
  }
}

const currentLocale = detectLocale();

const DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S';
const DEFAULT_DATE_FORMAT = '%Y-%m-%d';
const DEFAULT_DATE_WEEK_FORMAT = '%Y-%m-%d W%V';
const DEFAULT_DATE_MONTH_FORMAT = '%Y %b';
const DEFAULT_DATE_QUARTER_FORMAT = '%Y-Q%q';
const DEFAULT_DATE_YEAR_FORMAT = '%Y';

function getFormatByGrain(grain?: string): string {
  // Grains that should show date and time (sub-day granularities)
  const dateTimeGrains = ['second', 'minute', 'hour'];

  // Grains that should show date only (day and above granularities)
  const dateOnlyGrains = ['day', 'week', 'month', 'quarter', 'year'];

  if (grain === 'day') {
    return DEFAULT_DATE_FORMAT;
  }

  if (grain === 'week') {
    return DEFAULT_DATE_WEEK_FORMAT;
  }

  if (grain === 'month') {
    return DEFAULT_DATE_MONTH_FORMAT;
  }

  if (grain === 'quarter') {
    return DEFAULT_DATE_QUARTER_FORMAT;
  }

  if (grain === 'year') {
    return DEFAULT_DATE_YEAR_FORMAT;
  }

  if (!grain || dateTimeGrains.includes(grain)) {
    return DEFAULT_DATETIME_FORMAT;
  }

  if (dateOnlyGrains.includes(grain)) {
    return DEFAULT_DATE_FORMAT;
  }

  // Fallback to datetime for unknown grains
  return DEFAULT_DATETIME_FORMAT;
}

export function formatDateByGranularity(value: Date | string | number, granularity?: string): string {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Invalid date';
  }

  return timeFormat(getFormatByGrain(granularity))(date);
}

function parseNumber(value: any): number {
  if (value === null || value === undefined) {
    return 0;
  }

  return parseFloat(value);
}

export type FormatValueMember = {
  type: TCubeMemberType;
  format?: DimensionFormat | MeasureFormat;
  /** ISO 4217 currency code (e.g. 'USD', 'EUR'). Used when format is 'currency'. */
  currency?: string;
  /** Time dimension granularity (e.g. 'day', 'month', 'year'). Used for time formatting when no explicit format is set. */
  granularity?: string;
};

export type FormatValueOptions = FormatValueMember & {
  /** Locale tag (e.g. 'en-US', 'de-DE', 'nl-NL'). Defaults to the runtime's locale via Intl.NumberFormat. */
  locale?: string,
  /** String to return for null/undefined values. Defaults to '∅'. */
  emptyPlaceholder?: string;
};

export type GetFormatOptions = {
  locale?: string;
};

export type GetFormatResult = {
  formatString: string | null;
  formatFunc: (value: any) => string;
};

function formatBoolean(value: any): string {
  if (typeof value === 'boolean') {
    return value.toString();
  }

  if (typeof value === 'number') {
    return Boolean(value).toString();
  }

  // Some SQL drivers return booleans as '0'/'1' or 'true'/'false' strings, It's incorrect behaivour in Cube,
  // but let's format it as boolean for backward compatibility.
  if (value === '0' || value === 'false') {
    return 'false';
  }

  if (value === '1' || value === 'true') {
    return 'true';
  }

  return String(value);
}

export function getFormat(
  member: FormatValueMember,
  { locale = currentLocale }: GetFormatOptions = {}
): GetFormatResult {
  const { type, format, currency = 'USD', granularity } = member;

  if (type === 'boolean') {
    return { formatString: null, formatFunc: formatBoolean };
  }

  if (format && typeof format === 'object') {
    if (format.type === 'custom-numeric') {
      return {
        formatString: format.value,
        formatFunc: (value) => d3Format(format.value)(parseNumber(value)),
      };
    }

    if (format.type === 'custom-time') {
      return {
        formatString: format.value,
        formatFunc: (value) => {
          const date = new Date(value);
          return Number.isNaN(date.getTime()) ? 'Invalid date' : timeFormat(format.value)(date);
        },
      };
    }

    // { type: 'link', label: string } — return value as string
    return { formatString: null, formatFunc: (value) => String(value) };
  }

  if (typeof format === 'string') {
    switch (format) {
      case 'currency':
        return {
          formatString: DEFAULT_CURRENCY_FORMAT,
          formatFunc: (value) => getD3NumericLocale(locale, currency).format(DEFAULT_CURRENCY_FORMAT)(parseNumber(value)),
        };
      case 'percent':
        return {
          formatString: DEFAULT_PERCENT_FORMAT,
          formatFunc: (value) => getD3NumericLocale(locale).format(DEFAULT_PERCENT_FORMAT)(parseNumber(value)),
        };
      case 'number':
        return {
          formatString: DEFAULT_NUMBER_FORMAT,
          formatFunc: (value) => getD3NumericLocale(locale).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value)),
        };
      case 'id':
        return {
          formatString: DEFAULT_ID_FORMAT,
          formatFunc: (value) => d3Format(DEFAULT_ID_FORMAT)(parseNumber(value)),
        };
      case 'imageUrl':
      case 'link':
        return { formatString: null, formatFunc: (value) => String(value) };
      default: {
        const specifier = NAMED_NUMERIC_FORMATS[format];
        if (specifier) {
          const localeFormatter = specifier.includes('$')
            ? getD3NumericLocale(locale, currency).format(specifier)
            : getD3NumericLocale(locale).format(specifier);
          return {
            formatString: specifier,
            formatFunc: (value) => localeFormatter(parseNumber(value)),
          };
        }
        return { formatString: null, formatFunc: (value) => String(value) };
      }
    }
  }

  // No explicit format — infer from type
  if (type === 'time') {
    return {
      formatString: getFormatByGrain(granularity),
      formatFunc: (value) => formatDateByGranularity(value, granularity),
    };
  }

  if (type === 'number') {
    return {
      formatString: DEFAULT_NUMBER_FORMAT,
      formatFunc: (value) => getD3NumericLocale(locale, currency).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value)),
    };
  }

  return { formatString: null, formatFunc: (value) => String(value) };
}

export function formatValue(
  value: any,
  options: FormatValueOptions
): string {
  const { emptyPlaceholder = '∅' } = options;

  if (value === null || value === undefined) {
    return emptyPlaceholder;
  }

  return getFormat(options, { locale: options.locale }).formatFunc(value);
}
