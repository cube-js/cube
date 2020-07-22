import React, { useState, useEffect } from 'react';
import Highcharts from 'highcharts/highstock';
import HighchartsReact from 'highcharts-react-official';
import DragPanes from 'highcharts/modules/drag-panes.js';
import FullScreen from 'highcharts/modules/full-screen.js';

DragPanes(Highcharts);
FullScreen(Highcharts);

export default ({ data, setRange }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        styledMode: true,
        spacingRight: 20,
        spacingLeft: 20,
      },

      credits: {
        enabled: false,
      },

      title: {
        text: 'Orders by date<small>Highcharts Stock API</small>',
        useHTML: true,
      },

      xAxis: {
        events: {
          afterSetExtremes: function (e) {
            setRange([e.min, e.max]);
          },
        },
      },
      yAxis: {
        title: {
          enabled: false,
        },
        gridLineColor: '#D0D0DA30',
      },

      rangeSelector: {
        buttons: [
          {
            type: 'month',
            count: 3,
            text: '3m',
          },
          {
            type: 'month',
            count: 6,
            text: '6m',
          },
          {
            type: 'all',
            count: 1,
            text: 'All',
          },
        ],
        inputEnabled: false,
      },
      plotOptions: {
        series: {
          connectNulls: true,
        },
      },
      series: [
        {
          name: 'Orders',
          type: 'area',
          data: data,
          gapSize: 5,
          tooltip: {
            valueDecimals: 2,
          },
          color: '#FFC3BA',
          lineWidth: 1,
          fillColor: {
            linearGradient: {
              x1: 0,
              y1: 0,
              x2: 0,
              y2: 1,
            },
            stops: [
              [0, '#FFEAE4'],
              [1, '#FFEAE4'],
            ],
          },
          threshold: null,
        },
      ],
    });
  }, [data]);

  return (
    <div>
      <HighchartsReact
        highcharts={Highcharts}
        constructorType={'stockChart'}
        options={options}
      />
    </div>
  );
};
