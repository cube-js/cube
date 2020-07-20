import React, { useState, useEffect } from "react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

export default ({ categories, data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        type: 'column'
      },
      title: {
        text: 'Stacked column chart/Categories sales by time'
      },
      xAxis: {
        categories: categories
      },
      yAxis: {
        min: 0,
        title: {
          text: 'Total fruit consumption'
        },
        stackLabels: {
          enabled: true,
          style: {
            fontWeight: 'bold',
            color: (
              Highcharts.defaultOptions.title.style &&
              Highcharts.defaultOptions.title.style.color
            ) || 'gray'
          }
        }
      },
      legend: {
        align: 'right',
        x: -30,
        verticalAlign: 'top',
        y: 25,
        floating: true,
        backgroundColor:
          Highcharts.defaultOptions.legend.backgroundColor || 'white',
        borderColor: '#CCC',
        borderWidth: 1,
        shadow: false
      },
      tooltip: {
        headerFormat: '<b>{point.x}</b><br/>',
        pointFormat: '{series.name}: {point.y}<br/>Total: {point.stackTotal}'
      },
      plotOptions: {
        column: {
          stacking: 'normal',
          dataLabels: {
            enabled: true
          }
        }
      },
      series: data
    }
    )
  }, [data, categories]);


  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={options}
    />
  )
}