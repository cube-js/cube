import { formatLocale } from 'd3-format';

import type { FormatLocaleDefinition, FormatLocaleObject } from 'd3-format';

import enUS from 'd3-format/locale/en-US.json';
import enGB from 'd3-format/locale/en-GB.json';
import zhCN from 'd3-format/locale/zh-CN.json';
import esES from 'd3-format/locale/es-ES.json';
import esMX from 'd3-format/locale/es-MX.json';
import deDE from 'd3-format/locale/de-DE.json';
import jaJP from 'd3-format/locale/ja-JP.json';
import frFR from 'd3-format/locale/fr-FR.json';
import ptBR from 'd3-format/locale/pt-BR.json';
import koKR from 'd3-format/locale/ko-KR.json';
import itIT from 'd3-format/locale/it-IT.json';
import nlNL from 'd3-format/locale/nl-NL.json';
import ruRU from 'd3-format/locale/ru-RU.json';

// Pre-built d3 locale definitions for the most popular locales.
// Used as a fallback when Intl is unavailable (e.g. some edge runtimes).
export const formatD3NumericLocale: Record<string, FormatLocaleDefinition> = {
  'en-US': enUS as unknown as FormatLocaleDefinition,
  'en-GB': enGB as unknown as FormatLocaleDefinition,
  'zh-CN': zhCN as unknown as FormatLocaleDefinition,
  'es-ES': esES as unknown as FormatLocaleDefinition,
  'es-MX': esMX as unknown as FormatLocaleDefinition,
  'de-DE': deDE as unknown as FormatLocaleDefinition,
  'ja-JP': jaJP as unknown as FormatLocaleDefinition,
  'fr-FR': frFR as unknown as FormatLocaleDefinition,
  'pt-BR': ptBR as unknown as FormatLocaleDefinition,
  'ko-KR': koKR as unknown as FormatLocaleDefinition,
  'it-IT': itIT as unknown as FormatLocaleDefinition,
  'nl-NL': nlNL as unknown as FormatLocaleDefinition,
  'ru-RU': ruRU as unknown as FormatLocaleDefinition,
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

function getCurrencySymbol(locale: string | undefined, currencyCode: string): [string, string] {
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

function deriveGrouping(locale?: string): number[] {
  // Use a large number so that multiple grouping boundaries are visible.
  // en-US  → "1,234,567,890" → [3,3,3,3]  → d3 shorthand [3]
  // en-IN  → "1,23,45,67,890" → [3,2,2,2] → d3 shorthand [3,2]
  const parts = new Intl.NumberFormat(locale).formatToParts(1234567890);
  const integerSegments = parts
    .filter((p) => p.type === 'integer')
    .map((p) => p.value.length);

  if (integerSegments.length <= 1) {
    return [3];
  }

  // Drop the leading (most-significant) segment — it's a partial remainder,
  // not a real group size (e.g. the "1" in "1,234,567,890").
  // d3 reads the array left-to-right (least-significant group first),
  // and repeats the last entry for all remaining groups.
  const groups = integerSegments.slice(1).reverse();

  // Trim trailing duplicates so d3's repeat-last-element rule covers them.
  while (groups.length > 1 && groups[groups.length - 1] === groups[groups.length - 2]) {
    groups.pop();
  }

  return groups;
}

function getD3NumericLocaleFromIntl(locale?: string, currencyCode = 'USD'): FormatLocaleDefinition {
  const nf = new Intl.NumberFormat(locale);
  const numParts = nf.formatToParts(1234567.89);
  const find = (type: string) => numParts.find((p) => p.type === type)?.value ?? '';

  return {
    decimal: find('decimal') || '.',
    thousands: find('group') || ',',
    grouping: deriveGrouping(locale),
    currency: getCurrencySymbol(locale, currencyCode),
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
    definition = { ...formatD3NumericLocale[locale], currency: getCurrencySymbol(locale, currencyCode) };
  } else {
    try {
      definition = getD3NumericLocaleFromIntl(locale, currencyCode);
    } catch (e: unknown) {
      console.warn('Failed to generate d3 local via Intl, failing back to en-US', e);

      definition = formatD3NumericLocale['en-US'];
    }
  }

  localeCache[key] = formatLocale(definition);
  return localeCache[key];
}
