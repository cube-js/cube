import React, { useState, useEffect } from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

const staticOptions = {
  chart: {
    type: 'area',
    styledMode: true,
    spacingRight: 25,
    spacingLeft: 20,
  },
  credits: {
    enabled: false,
  },
  title: {
    text: 'Sales by category<small>Highcharts API, Area Chart</small>',
    useHTML: true,
  },

  yAxis: {
    title: {
      enabled: false,
    },
    gridLineColor: '#D0D0DA30',
  },

  legend: {
    align: 'center',
    width: '90%',
  },

  colors: [
    '#45446F',
    '#BE3D7F',
    '#FF6492',
    '#FF93A8',
    '#FFC3BA',
    '#FFEAE4',
    '#DFD7FF',
    '#B5ACFF',
    '#7A77FF',
    '#5251C9',
  ],
  plotOptions: {
    area: {
      stacking: 'normal',
      lineWidth: 1,
      marker: {
        enabled: false,
      },
    },
    series: {
      label: {
        connectorAllowed: false,
      },
      connectNulls: true,
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

export default ({ categories, data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      ...staticOptions,
      xAxis: {
        categories: categories,
      },
      series: data,
    });
  }, [data, categories]);

  return <HighchartsReact highcharts={Highcharts} options={options} />;
};
