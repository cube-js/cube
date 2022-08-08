import React, { useState, useEffect } from 'react';
import { useCubeQuery } from "@cubejs-client/react";
import { Col, Row, Slider, Tooltip } from "antd";
import MapGL, { Source, Layer, NavigationControl } from 'react-map-gl';

const Points = () => {
  const [viewport, setViewport] = useState({
    latitude: 34,
    longitude: 5,
    zoom: 1.5,
  })

  const [sliderInitMin, setsliderInitMin] = useState(0);
  const [sliderInitMax, setSliderInitMin] = useState(0);
  const [sliderCurMin, setSliderCurMin] = useState(0);
  const [sliderCurMax, setSliderCurMax] = useState(0);
  const [queryMin, setQueryMin] = useState(0);
  const [queryMax, setQueryMax] = useState(0);

  const { resultSet: sliderInitQuery } = useCubeQuery({
    measures: ['Users.max', 'Users.min']
  });

  const { resultSet: points } = useCubeQuery({
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
        values: [queryMax.toString()]
      },
      {
        member: "Users.value",
        operator: "gte",
        values: [queryMin.toString()]
      }
    ]
  });

  const data = {
    type: 'FeatureCollection',
    features: []
  };

  useEffect(() => {
    if (sliderInitQuery) {
      const range = sliderInitQuery.tablePivot()[0];
      setSliderInitMin(range['Users.max']);
      setsliderInitMin(range['Users.min']);
      setSliderCurMax(range['Users.max']);
      setSliderCurMin(range['Users.max'] * 0.4);
      setQueryMax(range['Users.max']);
      setQueryMin(range['Users.max'] * 0.4);
    }
  }, [sliderInitQuery]);

  if (points) {
    points.tablePivot().forEach((item) => {
      data['features'].push({
        type: 'Feature',
        properties: {
          value: parseInt(item['Users.max']),
        },
        geometry: JSON.parse(item['Users.geometry']),
      });
    });
  }


  const onChange = (value) => {
    setSliderCurMin(value[0]);
    setSliderCurMax(value[1]);
  }

  const onAfterChange = (value) => {
    setQueryMin(value[0]);
    setQueryMax(value[1]);
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
          mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/'
          mapboxAccessToken={process.env.REACT_APP_MAPBOX_KEY}
        >
          <div className='mapbox__navi'>
            <NavigationControl />
          </div>
          <Source type='geojson' data={data}>
            <Layer {...{
              type: 'circle',
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
        <Col span={3}>Rating range: </Col>
        <Col span={2}><Tooltip placement="top" title='minimal rating'>{Math.round(sliderCurMin / 1000)}k</Tooltip></Col>
        <Col span={17} >
          <Slider
            range
            min={sliderInitMin}
            max={sliderInitMax}
            step={1000}
            value={[sliderCurMin, sliderCurMax]}
            onChange={onChange}
            onAfterChange={onAfterChange}
            tooltipVisible={false}
          />
        </Col>
        <Col span={2}><Tooltip placement="top" title='maximal rating'>{Math.round(sliderCurMax / 1000)}k</Tooltip></Col>
      </Row>
    </React.Fragment>)
}

export default Points;
