import { formatLocale } from 'd3-format';

import type { FormatLocaleDefinition, FormatLocaleObject } from 'd3-format';

// Pre-built d3 locale definitions for the most popular locales.
// Used as a fallback when Intl is unavailable (e.g. some edge runtimes).
export const formatD3NumericLocale: Record<string, Omit<FormatLocaleDefinition, 'currency'>> = {
  'en-US': { decimal: '.', thousands: ',', grouping: [3] },
  'en-GB': { decimal: '.', thousands: ',', grouping: [3] },
  'zh-CN': { decimal: '.', thousands: ',', grouping: [3] },
  'es-ES': { decimal: ',', thousands: '.', grouping: [3] },
  'es-MX': { decimal: '.', thousands: ',', grouping: [3] },
  'de-DE': { decimal: ',', thousands: '.', grouping: [3] },
  'ja-JP': { decimal: '.', thousands: ',', grouping: [3] },
  'fr-FR': { decimal: ',', thousands: '\u00a0', grouping: [3], percent: '\u202f%' },
  'pt-BR': { decimal: ',', thousands: '.', grouping: [3] },
  'ko-KR': { decimal: '.', thousands: ',', grouping: [3] },
  'it-IT': { decimal: ',', thousands: '.', grouping: [3] },
  'nl-NL': { decimal: ',', thousands: '.', grouping: [3] },
  'ru-RU': { decimal: ',', thousands: '\u00a0', grouping: [3] },
};

const currencySymbols: Record<string, string> = {
  USD: '$',
  EUR: '€',
  GBP: '£',
  JPY: '¥',
  CNY: '¥',
  KRW: '₩',
  INR: '₹',
  RUB: '₽',
};

function getCurrencyOverride(locale: string | undefined, currencyCode: string): [string, string] {
  try {
    const cf = new Intl.NumberFormat(locale, { style: 'currency', currency: currencyCode });
    const currencyParts = cf.formatToParts(1);
    const currencySymbol = currencyParts.find((p) => p.type === 'currency')?.value ?? currencyCode;
    const firstMeaningfulType = currencyParts.find((p) => !['literal', 'nan'].includes(p.type))?.type;
    const symbolIsPrefix = firstMeaningfulType === 'currency';

    return symbolIsPrefix ? [currencySymbol, ''] : ['', currencySymbol];
  } catch {
    const symbol = currencySymbols[currencyCode] ?? currencyCode;
    return [symbol, ''];
  }
}

function deriveGrouping(locale: string): number[] {
  // en-US  → "1,234,567,890" → sizes [1,3,3,3] → [3]
  // en-IN  → "1,23,45,67,890" → sizes [1,2,2,2,3] → [3,2]
  const sizes = new Intl.NumberFormat(locale).formatToParts(1234567890)
    .filter((p) => p.type === 'integer')
    .map((p) => p.value.length);

  if (sizes.length <= 1) {
    return [3];
  }

  // d3 repeats the last array element for all remaining groups,
  // so we only need the two rightmost (least-significant) group sizes.
  const first = sizes[sizes.length - 1];
  const second = sizes[sizes.length - 2];

  return first === second ? [first] : [first, second];
}

function getD3NumericLocaleFromIntl(locale: string, currencyCode = 'USD'): FormatLocaleDefinition {
  const nf = new Intl.NumberFormat(locale);
  const numParts = nf.formatToParts(1234567.89);
  const find = (type: string) => numParts.find((p) => p.type === type)?.value ?? '';

  return {
    decimal: find('decimal') || '.',
    thousands: find('group') || ',',
    grouping: deriveGrouping(locale),
    currency: getCurrencyOverride(locale, currencyCode),
  };
}

const localeCache: Record<string, FormatLocaleObject> = Object.create(null);

export function getD3NumericLocale(locale: string, currencyCode = 'USD'): FormatLocaleObject {
  const key = `${locale}:${currencyCode}`;
  if (localeCache[key]) {
    return localeCache[key];
  }

  let definition: FormatLocaleDefinition;

  if (formatD3NumericLocale[locale]) {
    definition = { ...formatD3NumericLocale[locale], currency: getCurrencyOverride(locale, currencyCode) };
  } else {
    try {
      definition = getD3NumericLocaleFromIntl(locale, currencyCode);
    } catch (e: unknown) {
      console.warn('Failed to generate d3 local via Intl, failing back to en-US', e);

      definition = {
        ...formatD3NumericLocale['en-US'],
        currency: getCurrencyOverride(locale, currencyCode)
      };
    }
  }

  localeCache[key] = formatLocale(definition);
  return localeCache[key];
}
