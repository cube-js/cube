import React, { useState, useEffect } from "react";
import { Row, Col } from "antd";

import { useCubeQuery } from "@cubejs-client/react";


import Funnel from './Funnel';
import MasterDetail from './MasterDetail';


import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import highchartsMap from "highcharts/modules/map";
import mapDataIE from "@highcharts/map-collection/countries/us/us-all.geo.json";

highchartsMap(Highcharts);
const options = {

  title: {
    text: 'Solar Employment Growth by Sector, 2010-2016'
  },

  subtitle: {
    text: 'Source: thesolarfoundation.com'
  },

  yAxis: {
    title: {
      text: 'Number of Employees'
    }
  },

  xAxis: {
    accessibility: {
      rangeDescription: 'Range: 2010 to 2017'
    }
  },

  legend: {
    layout: 'vertical',
    align: 'right',
    verticalAlign: 'middle'
  },

  plotOptions: {
    series: {
      label: {
        connectorAllowed: false
      },
      pointStart: 2010
    }
  },

  series: [{
    name: 'Installation',
    data: [43934, 52503, 57177, 69658, 97031, 119931, 137133, 154175]
  }, {
    name: 'Manufacturing',
    data: [24916, 24064, 29742, 29851, 32490, 30282, 38121, 40434]
  }, {
    name: 'Sales & Distribution',
    data: [11744, 17722, 16005, 19771, 20185, 24377, 32147, 39387]
  }, {
    name: 'Project Development',
    data: [null, null, 7988, 12169, 15112, 22452, 34400, 34227]
  }, {
    name: 'Other',
    data: [12908, 5948, 8105, 11248, 8989, 11816, 18274, 18111]
  }],

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

}

const mapData = [
  ['us-ma', 0],
  ['us-wa', 1],
  ['us-ca', 2],
  ['us-or', 3],
  ['us-wi', 4],
  ['us-me', 5],
  ['us-mi', 6],
  ['us-nv', 7],
  ['us-nm', 8],
  ['us-co', 9],
  ['us-wy', 10],
  ['us-ks', 11],
  ['us-ne', 12],
  ['us-ok', 13],
  ['us-mo', 14],
  ['us-il', 15],
  ['us-in', 16],
  ['us-vt', 17],
  ['us-ar', 18],
  ['us-tx', 19],
  ['us-ri', 20],
  ['us-al', 21],
  ['us-ms', 22],
  ['us-nc', 23],
  ['us-va', 24],
  ['us-ia', 25],
  ['us-md', 26],
  ['us-de', 27],
  ['us-pa', 28],
  ['us-nj', 29],
  ['us-ny', 30],
  ['us-id', 31],
  ['us-sd', 32],
  ['us-ct', 33],
  ['us-nh', 34],
  ['us-ky', 35],
  ['us-oh', 36],
  ['us-tn', 37],
  ['us-wv', 38],
  ['us-dc', 39],
  ['us-la', 40],
  ['us-fl', 41],
  ['us-ga', 42],
  ['us-sc', 43],
  ['us-mn', 44],
  ['us-mt', 45],
  ['us-nd', 46],
  ['us-az', 47],
  ['us-ut', 48]
];

const stackOptions = {
  chart: {
    type: 'column'
  },
  title: {
    text: 'Stacked column chart/Categories sales by time'
  },
  xAxis: {
    categories: ['Apples', 'Oranges', 'Pears', 'Grapes', 'Bananas']
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
        color: ( // theme
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
  series: [{
    name: 'John',
    data: [5, 3, 4, 7, 2]
  }, {
    name: 'Jane',
    data: [2, 2, 3, 2, 1]
  }, {
    name: 'Joe',
    data: [3, 4, 4, 2, 5]
  }]
};

const pieOptions = {
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
  }
};


const mapOptions = {
  chart: {
    map: 'countries/us/custom/us-all-mainland'
  },
  title: {
    text: 'Map Demo'
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
    data: mapData,
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
          console.log(this);
        }
      }
    }
  }]
}
export default () => {
  const { resultSet: pie } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: [
      'ProductCategories.name',
    ],
    order: {
      'Orders.count': 'asc',
    },
  });

  const [pieData, setPieData] = useState([{ "name": "Beauty", "y": 1180 }]);


  useEffect(() => {
    if (pie) {
      let temp = []
      pie.tablePivot().map(item => {
        temp.push(
          {
            name: item['ProductCategories.name'],
            y: parseInt(item['Orders.count']),
          }
        );
      })
      setPieData(temp);
    }
  }, [pie])




  return (
    <React.Fragment>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <HighchartsReact
            highcharts={Highcharts}
            constructorType={'mapChart'}
            options={{ ...mapOptions, title: { text: 'Region map' } }}
          />
        </Col>
        <Col sm={24} lg={16}>
          <MasterDetail />
        </Col>
      </Row>




      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={12}>
          <HighchartsReact
            highcharts={Highcharts}
            options={stackOptions}
          />
        </Col>
        <Col sm={24} lg={12}>
          <HighchartsReact
            highcharts={Highcharts}
            options={{ ...options, title: { text: 'Daily sales/Line && Area' } }}
          />
        </Col>
      </Row>
      <Row gutter={20} className='dashboard__row'>
        <Col sm={24} lg={8}>
          <Funnel />
        </Col>
        <Col sm={24} lg={8}>
          <HighchartsReact
            highcharts={Highcharts}
            options={{
              ...pieOptions, series: [{
                name: 'Brands',
                colorByPoint: true,
                size: '80%',
                innerSize: '60%',
                data: pieData
              }]
            }}
          />
        </Col>
        <Col sm={24} lg={8}>
          <HighchartsReact
            highcharts={Highcharts}
            options={{ ...options, title: { text: 'Gauge chart' } }}
          />
        </Col>
      </Row>
    </React.Fragment>
  )
}