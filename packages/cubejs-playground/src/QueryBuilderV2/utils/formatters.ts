const formatterCurrency = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
});

export const getNumberFixedFormatter = (digits = 2, minDigits = 2) => {
  return new Intl.NumberFormat('en-US', {
    style: 'decimal',
    minimumFractionDigits: minDigits,
    maximumFractionDigits: digits,
  });
};

const getCurrencyFixedFormatter = (digits = 2, minDigits = 2) =>
  new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: minDigits,
    maximumFractionDigits: digits,
  });

const formatterNumber = getNumberFixedFormatter(3, 0);

export function formatNumber(amount: number, digits?: number, minDigits?: number) {
  return typeof digits === 'undefined' && typeof minDigits === 'undefined'
    ? formatterNumber.format(amount)
    : getNumberFixedFormatter(digits ?? 2, minDigits ?? 0).format(amount);
}

export function formatCurrency(amount: number, digits?: number, minDigits?: number) {
  return typeof digits === 'undefined' && typeof minDigits === 'undefined'
    ? formatterCurrency.format(amount)
    : getCurrencyFixedFormatter(digits ?? 2, minDigits ?? digits ?? 2).format(amount);
}
