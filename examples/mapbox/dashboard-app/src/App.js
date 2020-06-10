import React from "react";
import "./App.css";
import "./body.css";
import "antd/dist/antd.css";
import { Tabs, Layout } from "antd";
import cubejs from "@cubejs-client/core";
import { CubeProvider } from "@cubejs-client/react";
import Header from "./components/Header";
import Heatmap from './components/Heatmap';
import Ratings from './components/Ratings';
import Points from './components/Q&A';
import Choropleth from './components/Choropleth';

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts';

const API_URL = process.env.NODE_ENV === 'production' ? '' : "http://localhost:4000";

const CUBEJS_TOKEN =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTEyNzEyMTYsImV4cCI6MTU5MTM1NzYxNn0.wW3Agh2YPKo0s09nKpHs1fNREsFiE4OYlqjr7OqYutE";
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
});


class AppLayout extends React.Component {
  constructor() {
    super();
    this.state = {
      heatData: null,
      locationData: null,
      questionsData: null,
      answersData: null,
      choroplethData: null,
      barData: null,
      slider: {
        min: 0,
        max: 100
      },
      viewport: {
        latitude: 34,
        longitude: 5,
        zoom: 1,
      },
    }

  }


  componentDidMount = () => {
    cubejsApi
      .load({
        measures: ['pg__users.total', 'pg__users.avg', 'pg__users.count'],
        dimensions: [
          'pg__users.country',
        ],
      })
      .then((resultSet) => {
        let data = [];
        resultSet
          .tablePivot()
          .map((item) => {
            data.push({
              name: item['pg__users.country'],
              total: parseInt(item['pg__users.total']),
              avg: parseInt(item['pg__users.avg']),
              count: parseInt(item['pg__users.count']),
            });
          });
        this.setState({ barData: data })
      });

    cubejsApi
      .load({
        measures: ['pg__users.max', 'pg__users.min'],
      })
      .then((resultSet) => {
        this.setState({
          slider: {
            min: resultSet.tablePivot()[0]['pg__users.min'],
            max: resultSet.tablePivot()[0]['pg__users.max']
          }
        })
      });



    cubejsApi
      .load({
        measures: ['pg__users.total', 'pg__users.avg'],
        dimensions: [
          'pg__users.country',
          'mapbox__coords.coordinates'
        ],
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet
          .tablePivot()
          .filter((item) => item['mapbox__coords.coordinates'] != null)
          .map((item) => {
            data['features'].push({
              type: 'Feature',
              properties: {
                name: item['pg__users.country'],
                total: parseInt(item['pg__users.total']),
                avg: parseInt(item['pg__users.avg']),
              },
              geometry: {
                type: 'Polygon',
                coordinates: [
                  item['mapbox__coords.coordinates']
                    .split(';')
                    .map((item) => item.split(',')),
                ],
              },
            });
          });
        this.setState({ choroplethData: data })
      });

    cubejsApi
      .load({
        dimensions: [
          'pg__users.id',
          'pg__users.location',
          'pg__users.value',
          'pg__users.json',
        ],
        limit: 20000,
        order: {
          'pg__users.value': 'desc',
        },
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        let usersLocation = {};
        resultSet.tablePivot().map((item) => {
          let geo = JSON.parse(item['pg__users.json']);
          if (geo.features.length > 0) {
            if (geo.features[0]['place_type'] != 'country') {
              data['features'].push({
                type: 'Feature',
                properties: {
                  name: item['pg__users.location'],
                  value: parseInt(item['pg__users.value']),
                },
                geometry: { ...geo.features[0].geometry },
              });
            }

            usersLocation[item['pg__users.id']] = geo.features[0].geometry
          }

        });
        this.setState({ heatData: data, locationData: usersLocation })


        cubejsApi
          .load({
            dimensions: [
              'stackoverflow__questions.id',
              'stackoverflow__questions.owner_user_id',
              'stackoverflow__questions.title',
              'stackoverflow__questions.views',
              'stackoverflow__questions.tags'
            ],
            limit: 10000,
            order: {
              'stackoverflow__questions.views': 'desc',
            },
          })
          .then((resultSet) => {
            let data = {
              type: 'FeatureCollection',
              features: [],
            };
            resultSet.tablePivot().map((item) => {
              if (typeof this.state.locationData[item['stackoverflow__questions.owner_user_id']] != 'undefined') {
                data['features'].push({
                  type: 'Feature',
                  properties: {
                    tags: item['stackoverflow__questions.tags'],
                    views: item['stackoverflow__questions.views'],
                    title: item['stackoverflow__questions.title'],
                  },
                  geometry: this.state.locationData[item['stackoverflow__questions.owner_user_id']],
                });
              }
            });
            this.setState({ questionsData: data })
          });

        cubejsApi
          .load({
            dimensions: [
              'stackoverflow__answers.id'
            ],
            limit: 10000,
            order: {
              'stackoverflow__answers.id': 'asc',
            },
          })
          .then((resultSet) => {
            let data = {
              type: 'FeatureCollection',
              features: [],
            };
            resultSet.tablePivot().map((item) => {
              if (typeof this.state.locationData[item['stackoverflow__answers.id']] != 'undefined') {
                data['features'].push({
                  type: 'Feature',
                  geometry: this.state.locationData[item['stackoverflow__answers.id']],
                });
              }
            });
            this.setState({ answersData: data })
          });
      });
  };

  render() {
    return (<Layout
      style={{
        height: "100%"
      }}
    >
      <Header />
      <Layout.Content style={{ padding: '24px' }}>
        <p>Колорпикер отсюда: http://casesandberg.github.io/react-color. Можно выбрать другие. Пример более сложной реализации https://cssgradient.io/ </p>
        <div className="mapbox__nav">
          <Tabs defaultActiveKey="0">
            <Tabs.TabPane tab='by location' key={0}>
              <Heatmap data={this.state.heatData} />
            </Tabs.TabPane>
            <Tabs.TabPane tab='by rating' key={1}>
              <div className='mapbox__container'>
                <Ratings data={this.state.heatData} slider={this.state.slider} />
              </div>
            </Tabs.TabPane>
            <Tabs.TabPane tab='q&amp;a' key={2}>
              <Points answers={this.state.answersData} questions={this.state.questionsData} />
            </Tabs.TabPane>
            <Tabs.TabPane tab='total rating by country' key={3}>
              <div className='mapbox__container'>
                <Choropleth data={this.state.choroplethData} options={{
                  'fill-color': {
                    property: 'total',
                    stops: [
                      [0, '#ebeded'],
                      [500000, '#ecc1b8'],
                      [1000000, '#e7aba7'],
                      [10000000, '#e29494'],
                      [50000000, '#dd7a7a'],
                      [100000000, '#ce6567'],
                      [150000000, '#bb5656'],
                      [175000000, '#be4545'],
                      [200000000, '#af3636'],
                    ],
                  },
                }} text={{
                  'id': 'earthquake_label',
                  'type': 'symbol',
                  'layout': {
                    'text-field': [
                      'number-format',
                      ['get', 'total'],
                      { 'min-fraction-digits': 0, 'max-fraction-digits': 0 }
                    ],
                    'text-font': ['Open Sans Semibold', 'Arial Unicode MS Bold'],
                    'text-size': {
                      property: 'total',
                      stops: [
                        [{ zoom: 0, value: 100000 }, 10],
                        [{ zoom: 0, value: 50000000 }, 15],
                        [{ zoom: 0, value: 100000000 }, 20],
                      ]
                    }
                  },
                  'paint': {
                    'text-color': [
                      'case',
                      ['<', ['get', 'total'], 100000000],
                      'black',
                      'white'
                    ]
                  }
                }} />
              </div>

              <br /><br />
              <BarChart
                width={1000}
                height={300}
                data={this.state.barData}
                margin={{
                  top: 5, right: 30, left: 20, bottom: 5,
                }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip content={({ active, payload, label }) => {
                  if (active) {
                    return (
                      <div className="custom-tooltip">
                        <p className="label">{`${label} : ${payload[0].value}/${payload[0].payload.count}`}</p>
                      </div>
                    );
                  }
                  return null;
                }} />
                <Legend />
                <Bar dataKey="total" fill="#8884d8" />
              </BarChart>

            </Tabs.TabPane>
            <Tabs.TabPane tab='average rating by country' key={4}>
              <div className='mapbox__container'>
                <Choropleth data={this.state.choroplethData} options={{
                  'fill-color': {
                    property: 'avg',
                    stops: [
                      [0, '#ebeded'],
                      [1000, '#ecc1b8'],
                      [2000, '#e7aba7'],
                      [3000, '#e29494'],
                      [5000, '#dd7a7a'],
                      [8500, '#ce6567'],
                      [9000, '#bb5656'],
                      [10000, '#be4545'],
                      [11000, '#af3636'],
                    ],
                  },
                }} text={{
                  'id': 'earthquake_label',
                  'type': 'symbol',
                  'layout': {
                    'text-field': [
                      'number-format',
                      ['get', 'total'],
                      { 'min-fraction-digits': 0, 'max-fraction-digits': 0 }
                    ],
                    'text-font': ['Open Sans Semibold', 'Arial Unicode MS Bold'],
                    'text-size': {
                      property: 'total',
                      stops: [
                        [{ zoom: 0, value: 100000 }, 10],
                        [{ zoom: 0, value: 50000000 }, 15],
                        [{ zoom: 0, value: 100000000 }, 20],
                      ]
                    }
                  },
                  'paint': {
                    'text-color': [
                      'case',
                      ['<', ['get', 'total'], 100000000],
                      'black',
                      'white'
                    ]
                  }
                }} />
              </div>
              <br /><br />
              <BarChart
                width={1000}
                height={300}
                data={this.state.barData}
                margin={{
                  top: 5, right: 30, left: 20, bottom: 5,
                }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip content={({ active, payload, label }) => {
                  if (active) {
                    return (
                      <div className="custom-tooltip">
                        <p className="label">{`${label} : ${payload[0].value}/${payload[0].payload.count}`}</p>
                      </div>
                    );
                  }
                  return null;
                }} />
                <Legend />
                <Bar dataKey="avg" fill="#82ca9d" />
              </BarChart>
            </Tabs.TabPane>
          </Tabs>
        </div>
      </Layout.Content>
    </Layout>)
  }
}


const App = () => (
  <CubeProvider cubejsApi={cubejsApi}>
    <AppLayout></AppLayout>
  </CubeProvider>
);

export default App;

