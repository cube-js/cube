import React, { useState, useEffect } from "react";
import { useCubeQuery } from "@cubejs-client/react";


import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

import funnel from 'highcharts/modules/funnel';

funnel(Highcharts);

const funnelOptions = {
  chart: {
    type: 'funnel'
  },
  title: {
    text: 'Sales funnel'
  },
  plotOptions: {
    series: {
      dataLabels: {
        enabled: true,
        format: '<b>{point.name}</b> ({point.y:,.0f})',
        softConnector: true
      },
      center: ['40%', '50%'],
      neckWidth: '30%',
      neckHeight: '25%',
      width: '80%'
    }
  },
  legend: {
    enabled: false
  },
  series: [{
    name: 'Unique users',
    data: [
      ['Website visits', 15654],
      ['Downloads', 4064],
      ['Requested price list', 1987],
      ['Invoice sent', 976],
      ['Finalized', 846]
    ]
  }],

  responsive: {
    rules: [{
      condition: {
        maxWidth: 500
      },
      chartOptions: {
        plotOptions: {
          series: {
            dataLabels: {
              inside: true
            },
            center: ['50%', '50%'],
            width: '100%'
          }
        }
      }
    }]
  }
}

export default () => {
  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={funnelOptions}
    />
  )
}