import React, { useState, useEffect } from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

const staticOptions = {
  chart: {
    type: 'line',
    styledMode: true,
    spacingRight: 25,
    spacingLeft: 20,
  },
  credits: {
    enabled: false,
  },
  title: {
    text: 'Sales comparison by year',
    useHTML: true,
  },

  xAxis: {
    categories: [
      'January',
      'February',
      'March',
      'April',
      'May',
      'June',
      'July',
      'August',
      'September',
      'October',
      'November',
      'December',
    ],
  },

  yAxis: {
    title: {
      enabled: false,
    },
    gridLineColor: '#D0D0DA40',
  },

  legend: {
    align: 'center',
  },
  colors: ['#5251C9', '#FF93A8', '#FFEAE4'],
  plotOptions: {
    line: {
      lineWidth: 1.5,
      marker: {
        symbol: 'circle',
      },
    },
  },
  responsive: {
    rules: [
      {
        condition: {
          maxWidth: 500,
        },
        chartOptions: {
          legend: {
            layout: 'horizontal',
            align: 'center',
            verticalAlign: 'bottom',
          },
        },
      },
    ],
  },
};

export default ({ data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      ...staticOptions,
      series: data,
    });
  }, [data]);

  return <HighchartsReact highcharts={Highcharts} options={options} />;
};
