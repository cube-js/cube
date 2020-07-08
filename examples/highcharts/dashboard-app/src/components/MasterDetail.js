import React, { useState, useEffect } from "react";
import { useCubeQuery } from "@cubejs-client/react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

//https://codesandbox.io/s/10yv629397?file=/src/index.js:0-1394
import Data from "./data";

const data1 = Data();

let data = Data(),
  detailStart = data[0][0];

const LineChart = (props) => {
  return (
    <div>
      <HighchartsReact highcharts={Highcharts} options={props.options} />
      <MasterChart />
    </div>
  );
};

const MasterChart = (props) => {
  return (
    <div>
      <div
        style={{
          position: "absolute",
          top: 300,
          width: "100%"
        }}
      >
        <HighchartsReact highcharts={Highcharts} options={props.options} />
      </div>
    </div>
  );
};

export default () => {
  const [chartLineOptions, setLineChartOptions] = useState({
    chart: {
      marginBottom: 120,
      reflow: false,
      marginLeft: 50,
      marginRight: 20,
      style: {
        position: "absolute"
      }
    },
    credits: {
      enabled: false
    },
    title: {
      text: "Historical USD to EUR Exchange Rate",
      align: "left"
    },
    subtitle: {
      text: "Select an area by dragging across the lower chart",
      align: "left"
    },
    xAxis: {
      type: "datetime"
    },
    yAxis: {
      title: {
        text: null
      },
      maxZoom: 0.1
    },
    tooltip: {
      formatter: function () {
        var point = this.points[0];
        return (
          "<b>" +
          point.series.name +
          "</b><br/>" +
          Highcharts.dateFormat("%A %B %e %Y", this.x) +
          ":<br/>" +
          "1 USD = " +
          Highcharts.numberFormat(point.y, 2) +
          " EUR"
        );
      },
      shared: true
    },
    legend: {
      enabled: false
    },
    plotOptions: {
      series: {
        marker: {
          enabled: false,
          states: {
            hover: {
              enabled: true,
              radius: 3
            }
          }
        }
      }
    },
    series: [
      {
        name: "USD to EUR",
        pointStart: detailStart,
        pointInterval: 24 * 3600 * 1000,
        data: data1
      }
    ],

    exporting: {
      enabled: false
    }
  });

  const [chartOptions, setChartOptions] = useState({
    chart: {
      height: 100,
      reflow: false,
      borderWidth: 0,
      backgroundColor: null,
      marginLeft: 50,
      marginRight: 20,
      zoomType: "x",
      events: {
        // listen to the selection event on the master chart to update the
        // extremes of the detail chart
        selection: function (event) {
          var extremesObject = event.xAxis[0],
            min = extremesObject.min,
            max = extremesObject.max,
            detailData = [],
            xAxis = this.xAxis[0];

          this.series[0].data.forEach(data => {
            if (data.x > min && data.x < max) {
              detailData.push([data.x, data.y]);
            }
          });

          // move the plot bands to reflect the new detail span
          xAxis.removePlotBand("mask-before");
          xAxis.addPlotBand({
            id: "mask-before",
            from: data[0][0],
            to: min,
            color: "rgba(0, 0, 0, 0.2)"
          });

          xAxis.removePlotBand("mask-after");
          xAxis.addPlotBand({
            id: "mask-after",
            from: max,
            to: data[data.length - 1][0],
            color: "rgba(0, 0, 0, 0.2)"
          });

          setLineChartOptions({
            series: [{ data: detailData }]
          });
          return false;
        }
      }
    },
    title: {
      text: null
    },
    accessibility: {
      enabled: false
    },
    xAxis: {
      type: "datetime",
      showLastTickLabel: true,
      maxZoom: 14 * 24 * 3600000, // fourteen days
      plotBands: [
        {
          id: "mask-before",
          from: data[0][0],
          to: data[data.length - 1][0],
          color: "rgba(0, 0, 0, 0.2)"
        }
      ],
      title: {
        text: null
      }
    },
    yAxis: {
      gridLineWidth: 0,
      labels: {
        enabled: false
      },
      title: {
        text: null
      },
      min: 0.6,
      showFirstLabel: false
    },
    tooltip: {
      formatter: function () {
        return false;
      }
    },
    legend: {
      enabled: false
    },
    credits: {
      enabled: false
    },
    plotOptions: {
      series: {
        fillColor: {
          linearGradient: [0, 0, 0, 70],
          stops: [
            [0, Highcharts.getOptions().colors[0]],
            [1, "rgba(255,255,255,0)"]
          ]
        },
        lineWidth: 1,
        marker: {
          enabled: false
        },
        shadow: false,
        states: {
          hover: {
            lineWidth: 1
          }
        },
        enableMouseTracking: false
      }
    },

    series: [
      {
        type: "area",
        name: "USD to EUR",
        pointInterval: 24 * 3600 * 1000,
        pointStart: data[0][0],
        data: Data().slice()
      }
    ],

    exporting: {
      enabled: false
    }
  });

  return (
    <div>

      <LineChart options={chartLineOptions} />
      <MasterChart options={chartOptions} />
    </div>
  )
};
