import { BarChart, LineChart, AutoScaleAxis } from 'chartist';

import { getAquisitions, getDimensions } from './api'

(async function () {
  const aquisitions = await getAquisitions();
  const years = aquisitions.years;
  const amountByYear = aquisitions.amountByYear;

  new BarChart('#chartist__acquisitions', {
    labels: years,
    series: [amountByYear]
  }, {
    low: 0,
    axisX: {
      labelInterpolationFnc: (value) => (value % 10 === 0 ? value : null)
    }
  });

  const dimensions = await getDimensions();

  new LineChart(
    '#chartist__artwork',
    {
      series: [dimensions]
    },
    {
      low: 0,
      showLine: false,
      axisX: {
        type: AutoScaleAxis,
        onlyInteger: true
      }
    }
  );
})();
