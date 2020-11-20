export function formatNumber(number) {
  return number.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
}

export function roundNumberWithThousandPrecision(number, precision = 0) {
  return ((number / 1000).toFixed(precision) * 1000).toFixed(0)
}

export function getMedian(values) {
  if (values.length === 0) {
    return 0
  }

  values.sort((a, b) => a - b)

  const half = Math.floor(values.length / 2)

  return values.length % 2
    ? values[half]
    : (values[half - 1] + values[half]) / 2.0
}

export function withFilters(filters, query) {
  return filters.map(filter => ({
    ...query,
    filters: [ ...(filter.values[0] ? [ filter ] : []) ],
  }))
}