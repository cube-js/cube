const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];

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

  if (chartType === 'line') {
    return {
      labels: resultSet.categories().map(c => c.category),
      datasets: resultSet.series().map(s => (
        {
          label: s.title,
          data: s.series.map(r => r.value),
          borderColor: COLORS_SERIES[0],
          hoverBackgroundColor: COLORS_SERIES,
          fill: false
        }
      )),
    }
  }
}

export default toChartjsData;
