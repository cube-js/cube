// Predefined named numeric formats and their d3-format specifiers.
//
// "number", "percent", "currency", and "id" (without _X suffix) are already handled
// as separate format types in the existing API contract. Converting them to named
// formats here would be a breaking change. Only the _X suffixed variants are named.
export const NAMED_NUMERIC_FORMATS: Record<string, string> = {
  // number: grouped fixed-point
  number_0: ',.0f',
  number_1: ',.1f',
  number_2: ',.2f',
  number_3: ',.3f',
  number_4: ',.4f',
  number_5: ',.5f',
  number_6: ',.6f',

  // percent: .X%
  percent_0: '.0%',
  percent_1: '.1%',
  percent_2: '.2%',
  percent_3: '.3%',
  percent_4: '.4%',
  percent_5: '.5%',
  percent_6: '.6%',

  // currency: $,.Xf
  currency_0: '$,.0f',
  currency_1: '$,.1f',
  currency_2: '$,.2f',
  currency_3: '$,.3f',
  currency_4: '$,.4f',
  currency_5: '$,.5f',
  currency_6: '$,.6f',

  // decimal (Looker compat, same as number): ,.Xf
  // Alias to decimal_2
  decimal: ',.2f',
  decimal_0: ',.0f',
  decimal_1: ',.1f',
  decimal_2: ',.2f',
  decimal_3: ',.3f',
  decimal_4: ',.4f',
  decimal_5: ',.5f',
  decimal_6: ',.6f',

  // abbr (SI prefix): .Xs
  // Alias to abbr_2
  abbr: '.2s',
  abbr_0: '.0s',
  abbr_1: '.1s',
  abbr_2: '.2s',
  abbr_3: '.3s',
  abbr_4: '.4s',
  abbr_5: '.5s',
  abbr_6: '.6s',

  // accounting (negative in parens): (,.Xf
  // Alias to accounting_2
  accounting: '(,.2f',
  accounting_0: '(,.0f',
  accounting_1: '(,.1f',
  accounting_2: '(,.2f',
  accounting_3: '(,.3f',
  accounting_4: '(,.4f',
  accounting_5: '(,.5f',
  accounting_6: '(,.6f',
};

export function resolveNamedNumericFormat(value: string): string | undefined {
  return NAMED_NUMERIC_FORMATS[value];
}
