import React, { useState, useEffect } from "react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

import highchartsMore from "highcharts/highcharts-more"
import solidGauge from "highcharts/modules/solid-gauge";

highchartsMore(Highcharts);
solidGauge(Highcharts);

export default ({ data }) => {
  const [options, setOptions] = useState({});
  console.log(data)
  useEffect(() => {
    setOptions({
      chart: {
          type: 'solidgauge',
          style:  {"fontFamily": "\"DM Sans\", sans-serif","fontSize":"14px"}
      },

      credits: {
        enabled: false
      },
  
      title: {
          text: 'Orders statuses',
      },
      subtitle: {
        text: 'Highcharts API, Solid Gauge Chart'
      },

      tooltip: {
          borderWidth: 0,
          backgroundColor: 'none',
          shadow: false,
          style: {
              fontSize: '14px'
          },
          valueSuffix: '%',
          pointFormat: '{series.name}<br><span style="font-size:2em; color: {point.color}; font-weight: bold">{point.y}</span>',
          positioner: function (labelWidth) {
              return {
                  x: (this.chart.chartWidth - labelWidth) / 2,
                  y: (this.chart.plotHeight / 2) + 15
              };
          }
      },
      colors: ['#45446F', '#BE3D7F', '#FF6492', '#FF93A8', '#FFC3BA', '#FFEAE4', '#DFD7FF', '#B5ACFF', '#7A77FF', '#5251C9'],

      pane: {
          startAngle: 0,
          endAngle: 360,
          background: [{
              outerRadius: '112%',
              innerRadius: '88%',
              backgroundColor: '#45446F40',
              borderWidth: 0
          }, {
              outerRadius: '87%',
              innerRadius: '63%',
              backgroundColor: '#FF649240',
              borderWidth: 0
          }, {
              outerRadius: '62%',
              innerRadius: '38%',
              backgroundColor: '#FFEAE440',
              borderWidth: 0
          }]
      },
  
      yAxis: {
          min: 0,
          max: 100,
          lineWidth: 0,
          tickPositions: [],
      },
  
      plotOptions: {
          solidgauge: {
              dataLabels: {
                  enabled: false
              },
              linecap: 'round',
              stickyTracking: false,
              rounded: true,
              showInLegend: true
          }
      },


      legend: {
        align: 'center',
      },
  
      series: [{
          name: 'Completed',
          marker: {
            enabled: true, 
            fillColor: 'rgba(0,0,0,1)',
          },
          data: [{
              color: '#45446F',
              radius: '112%',
              innerRadius: '88%',
              y: data ? Math.round((data.status[0][1]*100)/data.count) : 0
          }]
      }, {
          name: 'Processing',
          marker: {
            fillColor: '#FF6492',
          },
          data: [{
              color: '#FF6492',
              radius: '87%',
              innerRadius: '63%',
              y: data ? Math.round((data.status[1][1]*100)/data.count) : 0
          }]
      }, {
          name: 'Shipped',
          marker: {
            fillColor: '#FFEAE4',
          },
          data: [{
              color: '#FFEAE4',
              radius: '62%',
              innerRadius: '38%',
              y: data ? Math.round((data.status[2][1]*100)/data.count) : 0
          }]
      }]
    })}, [data])

  return (
    <HighchartsReact
      highcharts={Highcharts}
      options={options}
    />
  )
}