import React, { useState, useEffect } from "react";

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
        type: 'pie'
      },
      title: {
        text: 'Top Categories (Drill down)'
      },
      tooltip: {
        pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
      },
      accessibility: {
        point: {
          valueSuffix: ''
        }
      },
      plotOptions: {
        pie: {
          shadow: false,
          center: ['50%', '50%']
        }
      },
      series: [{
        name: 'Orders amount',
        colorByPoint: true,
        size: '80%',
        innerSize: '60%',
        data: data
      }]
    });
  }, [data]);


  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={options}
    />
  )
}