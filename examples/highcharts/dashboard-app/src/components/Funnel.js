import React, { useState, useEffect } from "react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

import funnel from 'highcharts/modules/funnel';
funnel(Highcharts);

export default ({ data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        type: 'funnel'
      },
      title: {
        text: 'Orders Statuses'
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
        name: 'Orders status',
        data: data
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
    })
  }, [data])

  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={options}
    />
  )
}