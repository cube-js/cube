import React, { useState, useEffect } from 'react';

import { useCubeQuery } from "@cubejs-client/react";
import MapGL, { Source, Layer, NavigationControl } from 'react-map-gl';
import { Col, Row, Slider, Tooltip } from "antd";
import { range } from 'ramda';

export default ({ cubejsApi }) => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  })

  let obj = {
    initMin: 0,
    initMax: 0,
    min: 0,
    max: 0
  };

  const [initMin, setInitMin] = useState(0);
  const [initMax, setInitMax] = useState(0);

  const [min, setMin] = useState(0);
  const [max, setMax] = useState(0);
  const [data, setData] = useState(null);
  const { resultSet } = useCubeQuery({
    measures: ['Users.max', 'Users.min'],
    filters: [{
      member: "Users.geometry",
      operator: "set"
    }],
  });

  if (resultSet) {
    obj = {
      initMin: resultSet.tablePivot()[0]['Users.min'],
      initMax: resultSet.tablePivot()[0]['Users.max'],
      min: resultSet.tablePivot()[0]['Users.max'] * 0.4,
      max: resultSet.tablePivot()[0]['Users.max']
    }
  }

  useEffect(() => { loadData() }, [min, max])

  const loadData = () => {
    cubejsApi
      .load({
        measures: [
          'Users.max'
        ],
        dimensions: [
          'Users.geometry',
        ],
        filters: [
          {
            member: "Users.value",
            operator: "lte",
            values: [max.toString()]
          },

          {
            member: "Users.value",
            operator: "gte",
            values: [min.toString()]
          }
        ]
      })
      .then((resultSet) => {
        let data = {
          type: 'FeatureCollection',
          features: [],
        };
        resultSet.tablePivot().map((item) => {
          data['features'].push({
            type: 'Feature',
            properties: {
              value: parseInt(item['Users.max']),
            },
            geometry: JSON.parse(item['Users.geometry']),
          });
        });
        setData(data);
      });
  }

  const onChange = (value) => {
    obj.min = value[0];
    obj.max = value[1];
    setMin(value[0]);
    setMax(value[1]);
  }


  return (
    <React.Fragment>
      <div className='mapbox__container mapbox__container--slider'>
        <MapGL
          {...viewport}
          onViewportChange={(viewport) => {
            setViewport(viewport)
          }}
          width='100%'
          height='100%'
          mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
          mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
        >

          <div className='mapbox__navi'>
            <NavigationControl />
          </div>
          <Source type='geojson' data={data}>
            <Layer {...{
              type: 'circle',
              /*filter: [ //https://docs.mapbox.com/mapbox-gl-js/style-spec/other/#other-filter
                "all",
                [">", max, ["get", "value"]],
                ["<", min, ["get", "value"]]
              ],*/
              paint: {
                'circle-radius': {
                  property: 'value',
                  stops: [
                    [{ zoom: 0, value: 10000 }, 2],
                    [{ zoom: 0, value: 2000000 }, 20],
                  ]
                },
                'circle-stroke-width': 0,
                'circle-opacity': 0.6,
                'circle-color': '#FF6492'
              },
            }
            } />
          </Source>
        </MapGL>
      </div>
      <Row className="mapbox__slider">
        <Col span={2}><Tooltip placement="top" title='minimal rating'>{Math.round(min / 1000)}k</Tooltip></Col>
        <Col span={20} >
          <Slider
            range
            min={obj.initMin}
            max={obj.initMax}
            step={1}
            defaultValue={[obj.initMax, obj.initMax]}
            value={[obj.min, obj.max]}
            onChange={onChange}
            tooltipVisible={false}
          />
        </Col>
        <Col span={2}><Tooltip placement="top" title='maximal rating'>{Math.round(max / 1000)}k</Tooltip></Col>
      </Row>
    </React.Fragment>)
}