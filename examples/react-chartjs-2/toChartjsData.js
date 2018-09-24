const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

const toChartjsOptions = (chartType, resultSet) => {
  if (chartType === 'line' || chartType === 'bar') {
    return { scales: { xAxes: [{ type: `time`, time: { unit: 'month' }}] }};
  }

  return {}
}

const toChartjsData = (chartType, resultSet) => {
  if (chartType === 'pie') {
    return {
      labels: resultSet.categories().map(c => c.category),
      datasets: resultSet.series().map(s => (
        {
          label: s.title,
          data: s.series.map(r => r.value),
          backgroundColor: COLORS_SERIES,
          hoverBackgroundColor: COLORS_SERIES,
        }
      ))
    }
  }

  const formatter = (category) => {
    // TODO: Use Moment/Luxon or rely on the info
    // from API
    if (Date.parse(category)) {
      return new Date(category);
    } else {
      return category
    }
  }

  if (chartType === 'line') {
    return {
      labels: resultSet.categories().map(c => formatter(c.category)),
      datasets: resultSet.series().map((s, index) => (
        {
          label: s.title,
          data: s.series.map(r => r.value),
          borderColor: COLORS_SERIES[index],
          fill: false
        }
      )),
    }
  }

  if (chartType === 'bar') {
    return {
      labels: resultSet.categories().map(c => formatter(c.category)),
      datasets: resultSet.series().map((s, index) => (
        {
          label: s.title,
          data: s.series.map(r => r.value),
          backgroundColor: COLORS_SERIES[index],
          fill: false
        }
      )),
    }
  }

}

const chartjsConfig = (chartType, resultSet) => (
  {
    data: toChartjsData(chartType, resultSet),
    options: toChartjsOptions(chartType, resultSet),
    type: chartType
  }
)

export default chartjsConfig;
