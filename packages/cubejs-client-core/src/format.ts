import { format as d3Format } from 'd3-format';
import { timeFormat } from 'd3-time-format';
import { getD3NumericLocale } from './format-d3-numeric-locale';

import type { DimensionFormat, MeasureFormat, TCubeMemberType } from './types';

// Default d3-format specifiers — aligned with the named _2 formats
// (number_2, currency_2, percent_2) in named-numeric-formats.ts
const DEFAULT_NUMBER_FORMAT = ',.2f';
const DEFAULT_CURRENCY_FORMAT = '$,.2f';
const DEFAULT_PERCENT_FORMAT = '.2%';

const DEFAULT_ID_FORMAT = '.0f';

function detectLocale() {
  try {
    return new Intl.NumberFormat().resolvedOptions().locale;
  } catch (e) {
    console.warn('Failed to detect locale', e);

    return 'en-US';
  }
}

const currentLocale = detectLocale();

// d3-time-format patterns by granularity.
const DATETIME_FORMAT_BY_GRANULARITY: Record<string, string> = {
  second: '%Y-%m-%d %H:%M:%S',
  minute: '%Y-%m-%d %H:%M',
  hour: '%Y-%m-%d %H:00',
  day: '%Y-%m-%d',
  week: '%Y-%m-%d W%V',
  month: '%Y %b',
  quarter: '%Y Q%q',
  year: '%Y',
};

const DEFAULT_DATETIME_FORMAT = DATETIME_FORMAT_BY_GRANULARITY.second;

export function formatDateByGranularity(
  timestamp: Date | string | number,
  granularity?: string
): string {
  const date = timestamp instanceof Date ? timestamp : new Date(timestamp);
  if (Number.isNaN(date.getTime())) return 'Invalid date';

  const pattern = (granularity && DATETIME_FORMAT_BY_GRANULARITY[granularity]) || DEFAULT_DATETIME_FORMAT;
  return timeFormat(pattern)(date);
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

export function formatValue(
  value: any,
  { type, format, currency = 'USD', granularity, locale = currentLocale, emptyPlaceholder = '∅' }: FormatValueOptions
): string {
  if (value === null || value === undefined) {
    return emptyPlaceholder;
  }

  if (type === 'boolean') {
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

  if (format && typeof format === 'object') {
    if (format.type === 'custom-numeric') {
      return d3Format(format.value)(parseNumber(value));
    }

    if (format.type === 'custom-time') {
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? 'Invalid date' : timeFormat(format.value)(date);
    }

    // { type: 'link', label: string } — return value as string
    return String(value);
  }

  if (typeof format === 'string') {
    switch (format) {
      case 'currency':
        return getD3NumericLocale(locale, currency).format(DEFAULT_CURRENCY_FORMAT)(parseNumber(value));
      case 'percent':
        return getD3NumericLocale(locale).format(DEFAULT_PERCENT_FORMAT)(parseNumber(value));
      case 'number':
        return getD3NumericLocale(locale).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
      case 'id':
        return d3Format(DEFAULT_ID_FORMAT)(parseNumber(value));
      case 'imageUrl':
      case 'link':
      default:
        return String(value);
    }
  }

  // No explicit format — infer from type
  if (type === 'time') {
    return formatDateByGranularity(value, granularity);
  }

  if (type === 'number') {
    return getD3NumericLocale(locale, currency).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
  }

  return String(value);
}
