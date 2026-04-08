import {format as d3Format, formatLocale, FormatLocaleDefinition, FormatLocaleObject} from 'd3-format';
import { timeFormat } from 'd3-time-format';

import type { DimensionFormat, MeasureFormat, TCubeMemberType } from './types';

const DEFAULT_NUMBER_FORMAT = ',.2~f';
const DEFAULT_CURRENCY_FORMAT = '$,.2~f';
const DEFAULT_PERCENT_FORMAT = '.2~%';

function getD3LocaleFromIntl(locale?: string, currencyCode = 'USD'): FormatLocaleDefinition {
  const nf = new Intl.NumberFormat(locale);
  const numParts = nf.formatToParts(1234567.89);
  const find = (type: string) => numParts.find((p) => p.type === type)?.value ?? '';

  const cf = new Intl.NumberFormat(locale, { style: 'currency', currency: currencyCode });
  const currencyParts = cf.formatToParts(1);
  const currencySymbol = currencyParts.find((p) => p.type === 'currency')?.value ?? currencyCode;
  const firstMeaningfulType = currencyParts.find((p) => !['literal', 'nan'].includes(p.type))?.type;
  const symbolIsPrefix = firstMeaningfulType === 'currency';

  return {
    decimal: find('decimal') || '.',
    thousands: find('group') || ',',
    grouping: [3],
    currency: symbolIsPrefix ? [currencySymbol, ''] : ['', currencySymbol],
  };
}

const localeCache: Record<string, FormatLocaleObject> = Object.create(null);

function getCurrentD3Locale(locale: string, currencyCode = 'USD'): FormatLocaleObject {
  const key = `${locale}:${currencyCode}`;
  if (localeCache[key]) {
    return localeCache[key];
  }

  localeCache[key] = formatLocale(getD3LocaleFromIntl(locale, currencyCode));
  return localeCache[key];
}

function getCurrentLocale(): string {
  return new Intl.NumberFormat().resolvedOptions().locale;
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

export type FormatValueMember = {
  type: TCubeMemberType;
  format?: DimensionFormat | MeasureFormat;
  /** ISO 4217 currency code (e.g. 'USD', 'EUR'). Used when format is 'currency'. */
  currency?: string;
  /** Time dimension granularity (e.g. 'day', 'month', 'year'). Used for time formatting when no explicit format is set. */
  granularity?: string;
};

export type FormatValueOptions = FormatValueMember & {
  locale?: string,
  emptyPlaceholder?: string;
};

export function formatValue(
  value: any,
  { type, format, currency = 'USD', granularity, locale = getCurrentLocale(), emptyPlaceholder = '∅' }: FormatValueOptions
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
        return getCurrentD3Locale(locale, currency).format(DEFAULT_CURRENCY_FORMAT)(parseNumber(value));
      case 'percent':
        return getCurrentD3Locale(locale, currency).format(DEFAULT_PERCENT_FORMAT)(parseNumber(value));
      case 'number':
        return getCurrentD3Locale(locale, currency).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
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
    return getCurrentD3Locale(locale, currency).format(DEFAULT_NUMBER_FORMAT)(parseNumber(value));
  }

  return String(value);
}
