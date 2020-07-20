import React, { useState, useEffect } from "react";

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

import highchartsMap from "highcharts/modules/map";
import mapDataIE from "@highcharts/map-collection/countries/us/us-all.geo.json";
highchartsMap(Highcharts);


export default ({ data, setRegion }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      chart: {
        map: 'countries/us/custom/us-all-mainland'
      },
      title: {
        text: 'Sales by regions'
      },
      credits: {
        enabled: false
      },
      mapNavigation: {
        enabled: true
      },
      colorAxis: {
        min: 0
      },
      tooltip: {
        headerFormat: '',
        pointFormat: `
    <b>{point.freq}</b><br><b>{point.keyword}</b>                      
    <br>lat: {point.lat}, lon: {point.lon}`
      },
      series: [{
        name: 'Basemap',
        mapData: mapDataIE,
        data: data,
        borderColor: '#A0A0A0',
        nullColor: 'rgba(200, 200, 200, 0.3)',
        showInLegend: false,
        dataLabels: {
          enabled: true,
          format: "{point.name}"
        },
        point: {
          events: {
            click: function () {
              setRegion(this['hc-key']);
            }
          }
        }
      }]
    })
  }, [data]);
  return (
    <HighchartsReact
      highcharts={Highcharts}
      constructorType={'mapChart'}
      options={options}
    />
  )
}