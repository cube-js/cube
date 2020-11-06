import React, { useState, useEffect } from 'react';

import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

import highchartsMap from 'highcharts/modules/map';
import mapDataIE from '@highcharts/map-collection/countries/us/us-all.geo.json';
highchartsMap(Highcharts);

const staticOptions = {
  chart: {
    map: 'countries/us/custom/us-all-mainland',
    styledMode: true,
  },
  credits: {
    enabled: false,
  },
  title: {
    text: 'Orders by region<small>Highcharts Map API</small>',
    useHTML: true,
  },
  tooltip: {
    headerFormat: '',
    pointFormat: `
      <b>{point.name}</b>: {point.value}`,
  },
  colorAxis: {
    min: 0,
    minColor: '#FFEAE4',
    maxColor: '#FF6492',
  },
  series: [
    {
      name: 'Basemap',
      mapData: mapDataIE,
      borderColor: '#FFC3BA',
      borderWidth: 0.5,
      nullColor: '#FFEAE4',
      showInLegend: false,
      allowPointSelect: true,
      dataLabels: {
        enabled: true,
        format: '{point.name}',
        color: '#000',
      },
      states: {
        select: {
          borderColor: '#B5ACFF',
          color: '#7A77FF',
        },
      },
    },
  ],
};

export default ({ data, setRegion }) => {
  const [options, setOptions] = useState({});
  useEffect(() => {
    setOptions({
      ...staticOptions,
      series: [
        {
          ...staticOptions.series[0],
          data: data,
          point: {
            events: {
              click: function () {
                setRegion(this['hc-key']);
              },
            },
          },
        },
      ],
    });
  }, [data, setRegion]);

  return (
    <HighchartsReact
      highcharts={Highcharts}
      constructorType={'mapChart'}
      options={options}
    />
  );
};
