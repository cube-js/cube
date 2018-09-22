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

export default toChartjsData;
