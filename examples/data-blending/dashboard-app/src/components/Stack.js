import React, { useState, useEffect } from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

const staticOptions = {
  chart: {
    type: 'column',
    styledMode: true,
    spacingRight: 25,
    spacingLeft: 20,
  },
  credits: {
    enabled: false,
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
    max: 110,
  },

  legend: {
    align: 'center',
  },
  colors: ['#FF6492', '#7A77FF', '#FFC3BA'],
  plotOptions: {
    column: {
      stacking: 'normal',
    },
  },
};

export default ({ title, data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    console.log(data);
    setOptions({
      ...staticOptions,
      title: {
        text: `Sales in % by channel <small>implemented with ${title}</small>`,
        useHTML: true,
      },
      series: data,
    });
  }, [data, title]);

  return <HighchartsReact highcharts={Highcharts} options={options} />;
};
