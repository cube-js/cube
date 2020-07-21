import React, { useState, useEffect } from "react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

export default ({ categories, data }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        style:  {"fontFamily": "\"DM Sans\", sans-serif","fontSize":"14px"},
        type: 'area'
      },
      credits: {
        enabled: false
      },
      title: {
        text: 'Categories sales'
      },
      subtitle: {
        text: 'Highcharts API, Area Chart'
      },

      yAxis: {
        title: {
          text: 'Number of sales'
        }
      },

      xAxis: {
        categories: categories
      },

      legend: {
        align: 'center',
        width: '90%'
      },

      colors: ['#45446F', '#BE3D7F', '#FF6492', '#FF93A8', '#FFC3BA', '#FFEAE4', '#DFD7FF', '#B5ACFF', '#7A77FF', '#5251C9'],
      plotOptions: {
        area: {
            stacking: 'normal',
            lineWidth: 1,
            marker: {
                enabled: false
            }
        },
        series: {
          label: {
            connectorAllowed: false
          }
        }
      },


      series: data,
      responsive: {
        rules: [{
          condition: {
            maxWidth: 500
          },
          chartOptions: {
            legend: {
              layout: 'horizontal',
              align: 'center',
              verticalAlign: 'bottom'
            }
          }
        }]
      }
    },
    )
  }, [data, categories]);


  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={{ ...options }}
    />
  )
}