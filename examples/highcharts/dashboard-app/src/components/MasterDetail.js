import React, { useState, useEffect } from "react";
import Highcharts from "highcharts/highstock";
import HighchartsReact from "highcharts-react-official";
import DragPanes from "highcharts/modules/drag-panes.js";
import FullScreen from "highcharts/modules/full-screen.js";

// init the module
DragPanes(Highcharts);
FullScreen(Highcharts);



export default ({ data, setRange }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions(
      {
        chart: {
          style:  {"fontFamily": "\"DM Sans\", sans-serif","fontSize":"14px"}
        },

        credits: {
          enabled: false
        },

        title: {
          text: 'Orders by date'
        },

        subtitle: {
          text: 'Highcharts Stock API'
        },

        

        xAxis: {
          events: {
            afterSetExtremes: function (e) {
              setRange([e.min, e.max]);
            }
          }
        },
        yAxis: {
          title: {
            text: 'Total orders'
          }
        },


        rangeSelector: {
          buttons: [{
            type: 'month',
            count: 3,
            text: '3m'
          },
          {
            type: 'month',
            count: 6,
            text: '6m'
          }, 
          {
            type: 'all',
            count: 1,
            text: 'All'
          }],
          inputEnabled: true,
          inputBoxBorderColor: '#eaeaea',
        },

        series: [{
          name: 'Orders',
          type: 'area',
          data: data,
          gapSize: 5,
          tooltip: {
            valueDecimals: 2
          },
          color: '#B5ACFF',
          lineWidth: 1,
          fillColor: {
            linearGradient: {
              x1: 0,
              y1: 0,
              x2: 0,
              y2: 1
            },
            stops: [
              [0, '#DFD7FF'],
              [1, '#DFD7FF']
            ]
          },
          threshold: null
        }]
      }
    )
  }, [data]);



  return (
    <div>
      <HighchartsReact
        highcharts={Highcharts}
        constructorType={"stockChart"}
        options={options}
      />
    </div>
  )
};
