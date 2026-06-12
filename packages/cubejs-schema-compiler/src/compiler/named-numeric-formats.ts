// Predefined named numeric formats and their d3-format specifiers.
//
// "number", "percent", and "currency" (without _X suffix) are already handled
// as separate format types in the existing API contract. Converting them to named
// formats here would be a breaking change. Only the _X suffixed variants are named.
//
// All specifiers use the d3-format `~` modifier to trim insignificant trailing
// zeros — the precision is an upper bound, not a fixed digit count.
export const NAMED_NUMERIC_FORMATS: Record<string, string> = {
  // number: grouped fixed-point
  number_0: ',.0f',
  number_1: ',.1~f',
  number_2: ',.2~f',
  number_3: ',.3~f',
  number_4: ',.4~f',
  number_5: ',.5~f',
  number_6: ',.6~f',

  // percent: .X%
  percent_0: '.0%',
  percent_1: '.1~%',
  percent_2: '.2~%',
  percent_3: '.3~%',
  percent_4: '.4~%',
  percent_5: '.5~%',
  percent_6: '.6~%',

  // currency: $,.Xf
  currency_0: '$,.0f',
  currency_1: '$,.1~f',
  currency_2: '$,.2~f',
  currency_3: '$,.3~f',
  currency_4: '$,.4~f',
  currency_5: '$,.5~f',
  currency_6: '$,.6~f',

  // decimal (Looker compat, same as number): ,.Xf
  // Alias to decimal_2
  decimal: ',.2~f',
  decimal_0: ',.0f',
  decimal_1: ',.1~f',
  decimal_2: ',.2~f',
  decimal_3: ',.3~f',
  decimal_4: ',.4~f',
  decimal_5: ',.5~f',
  decimal_6: ',.6~f',

  // abbr (SI prefix): .Xs
  // Alias to abbr_2
  abbr: '.2~s',
  abbr_0: '.0~s',
  abbr_1: '.1~s',
  abbr_2: '.2~s',
  abbr_3: '.3~s',
  abbr_4: '.4~s',
  abbr_5: '.5~s',
  abbr_6: '.6~s',

  // id: grouped integer (no decimals)
  id: '.0f',

  // accounting (negative in parens): (,.Xf
  // Alias to accounting_2
  accounting: '(,.2~f',
  accounting_0: '(,.0f',
  accounting_1: '(,.1~f',
  accounting_2: '(,.2~f',
  accounting_3: '(,.3~f',
  accounting_4: '(,.4~f',
  accounting_5: '(,.5~f',
  accounting_6: '(,.6~f',
};

export function resolveNamedNumericFormat(value: string): string | undefined {
  return NAMED_NUMERIC_FORMATS[value];
}

/**
 * Maps standard/base format names to their default d3-format specifiers.
 * Used by resolveFormatDescription to produce FormatDescription for
 * formats that pass through as bare strings (percent, currency, number)
 * as well as named formats resolved from NAMED_NUMERIC_FORMATS.
 */
export const STANDARD_FORMAT_SPECIFIERS: Record<string, { name: string; specifier: string }> = {
  percent: { name: 'percent', specifier: '.2~%' },
  currency: { name: 'currency', specifier: '$,.2~f' },
  number: { name: 'number', specifier: ',.2~f' },
  abbr: { name: 'abbr', specifier: '.2~s' },
  accounting: { name: 'accounting', specifier: '(,.2~f' },
  id: { name: 'id', specifier: '.0f' },
};

export const DEFAULT_FORMAT_SPECIFIER: { name: string; specifier: string } = {
  name: 'number',
  specifier: ',.2~f',
};
