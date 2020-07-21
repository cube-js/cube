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
        map: 'countries/us/custom/us-all-mainland',
        style:  {"fontFamily": "\"DM Sans\", sans-serif","fontSize":"14px"},
      },
      credits: {
        enabled: false
      },
      title: {
        text: 'Orders by region'
      },
      subtitle: {
        text: 'Highcharts Map API'
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
      colorAxis:{
        minColor: '#DFD7FF',
        maxColor: '#7A77FF'
      },
      series: [{
        name: 'Basemap',
        mapData: mapDataIE,
        data: data,
        borderColor: '#FFEAE4',
        nullColor: '#FFEAE480',
        showInLegend: false,
        dataLabels: {
          enabled: true,
          format: "{point.name}",
          color: '#000'
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