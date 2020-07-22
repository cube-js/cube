import React, { useState, useEffect } from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

export default ({ data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        plotBackgroundColor: null,
        plotBorderWidth: null,
        plotShadow: false,
        type: 'pie',
        styledMode: true,
        spacingRight: 20,
        spacingLeft: 20,
        spacingBottom: 20,
      },
      credits: {
        enabled: false,
      },
      title: {
        text: 'Top Categories<small>Highcharts API, Pie Chart</small>',
        useHTML: true,
      },
      tooltip: {
        pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>',
      },
      accessibility: {
        point: {
          valueSuffix: '',
        },
      },
      plotOptions: {
        pie: {
          shadow: false,
          center: ['50%', '50%'],
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
        },
      },
      series: [
        {
          name: 'Orders amount',
          colorByPoint: true,
          size: '80%',
          innerSize: '50%',
          data: data,
        },
      ],
    });
  }, [data]);

  return <HighchartsReact highcharts={Highcharts} options={options} />;
};
