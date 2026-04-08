import { format as d3Format, formatLocale, FormatLocaleDefinition } from 'd3-format';
import { timeFormat } from 'd3-time-format';

import type { DimensionFormat, MeasureFormat, TCubeMemberType } from './types';

const DEFAULT_NUMBER_FORMAT = ',.2~f';
const DEFAULT_CURRENCY_FORMAT = '$,.2~f';
const DEFAULT_PERCENT_FORMAT = '.2~%';

// d3-format en-US defaults — serves as the base for all locales
const DEFAULT_LOCALE: FormatLocaleDefinition = {
  decimal: '.',
  thousands: ',',
  grouping: [3],
  currency: ['$', ''],
};

function getCurrencySymbol(code: string): string {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: code })
    .formatToParts(0)
    .find((part) => part.type === 'currency')?.value || code;
}

function createLocale(currencyCode: string) {
  return formatLocale({
    ...DEFAULT_LOCALE,
    currency: [getCurrencySymbol(currencyCode), ''],
  });
}

const DEFAULT_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S';
const DEFAULT_DATE_FORMAT = '%Y-%m-%d';
const DEFAULT_DATE_MONTH_FORMAT = '%Y-%m';
const DEFAULT_DATE_QUARTER_FORMAT = '%Y-Q%q';
const DEFAULT_DATE_YEAR_FORMAT = '%Y';

function getTimeFormatByGrain(grain: string | undefined): string {
  switch (grain) {
    case 'day':
    case 'week':
      return DEFAULT_DATE_FORMAT;
    case 'month':
      return DEFAULT_DATE_MONTH_FORMAT;
    case 'quarter':
      return DEFAULT_DATE_QUARTER_FORMAT;
    case 'year':
      return DEFAULT_DATE_YEAR_FORMAT;
    case 'second':
    case 'minute':
    case 'hour':
    default:
      return DEFAULT_DATETIME_FORMAT;
  }
}

function parseNumber(value: any): number {
  if (value === null || value === undefined) {
    return 0;
  }

  return parseFloat(value);
}

export type FormatValueOptions = {
  type: TCubeMemberType;
  format?: DimensionFormat | MeasureFormat;
  /** ISO 4217 currency code (e.g. 'USD', 'EUR'). Used when format is 'currency'. */
  currency?: string;
  /** Time dimension granularity (e.g. 'day', 'month', 'year'). Used for time formatting when no explicit format is set. */
  granularity?: string;
  emptyPlaceholder?: string;
};

export function formatValue(
  value: any,
  { type, format, currency = 'USD', granularity, emptyPlaceholder = '∅' }: FormatValueOptions
): string {
  if (value === null || value === undefined) {
    return emptyPlaceholder;
  }

  if (format && typeof format === 'object') {
    if (format.type === 'custom-numeric') {
      return d3Format(format.value)(parseNumber(value));
    }

    if (format.type === 'custom-time') {
      return timeFormat(format.value)(new Date(value));
    }

    // { type: 'link', label: string } — return value as string
    return String(value);
  }

  if (typeof format === 'string') {
    switch (format) {
      case 'currency':
        return createLocale(currency).format(DEFAULT_CURRENCY_FORMAT)(parseNumber(value));
      case 'percent':
        return d3Format(DEFAULT_PERCENT_FORMAT)(parseNumber(value));
      case 'number':
        return d3Format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
      case 'imageUrl':
      case 'id':
      case 'link':
      default:
        return String(value);
    }
  }

  // No explicit format — infer from type
  if (type === 'time') {
    const fmt = getTimeFormatByGrain(granularity);
    return timeFormat(fmt)(new Date(value));
  }

  if (type === 'number') {
    return createLocale(currency).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
  }

  return String(value);
}
