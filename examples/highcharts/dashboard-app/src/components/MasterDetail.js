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
        title: {
          text: 'Orders sum amount'
        },

        subtitle: {
          text: 'Using explicit breaks for nights and weekends'
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
            count: 6,
            text: '6m'
          }, {
            type: 'year',
            count: 1,
            text: '1y'
          },
          {
            type: 'all',
            count: 1,
            text: 'All'
          }],
          inputEnabled: true
        },

        series: [{
          name: 'Orders',
          type: 'area',
          data: data,
          gapSize: 5,
          tooltip: {
            valueDecimals: 2
          },
          fillColor: {
            linearGradient: {
              x1: 0,
              y1: 0,
              x2: 0,
              y2: 1
            },
            stops: [
              [0, Highcharts.getOptions().colors[0]],
              [1, Highcharts.color(Highcharts.getOptions().colors[0]).setOpacity(0).get('rgba')]
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
