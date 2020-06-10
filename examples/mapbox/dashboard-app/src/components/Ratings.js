import React, { useState, useEffect } from 'react';
import MapGL, { Source, Layer } from 'react-map-gl';
import { Slider } from "antd";




export default (props) => {

    const [viewport, setViewport] = useState({
        latitude: 34,
        longitude: 5,
        zoom: 1,
    })

    const [min, setMin] = useState(0);
    const [max, setMax] = useState(0);

    useEffect(() => {
        setMin(props.slider.min);
        setMax(props.slider.max);
    }, [props.slider]);

    const onChange = (value) => {
        setMin(value[0]);
        setMax(value[1]);
    }


    return (
        <div className='mapbox__container'>
            <MapGL
                {...viewport}
                onViewportChange={(viewport) => {
                    setViewport(viewport)
                }}
                width='100%'
                height='600px'
                mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
                mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
            >
                <Source type='geojson' data={props.data}>
                    <Layer {...{
                        type: 'circle',
                        filter: [ //https://docs.mapbox.com/mapbox-gl-js/style-spec/other/#other-filter
                            "all",
                            [">", max, ["get", "value"]],
                            ["<", min, ["get", "value"]]
                        ],
                        paint: {
                            'circle-radius': {
                                property: 'value',
                                stops: [
                                    [{ zoom: 0, value: 1000 }, 1],
                                    [{ zoom: 0, value: 50000 }, 2],
                                    [{ zoom: 0, value: 500000 }, 4],
                                    [{ zoom: 0, value: 800000 }, 6],
                                ]
                            },
                            'circle-stroke-width': 0,
                            'circle-opacity': 1,
                            'circle-color': {
                                property: 'value',
                                stops: [
                                    [{ zoom: 0, value: 1000 }, '#5bc0eb'],
                                    [{ zoom: 0, value: 50000 }, '#f6ae2d'],
                                    [{ zoom: 0, value: 100000 }, '#f26419'],
                                    [{ zoom: 0, value: 800000 }, '#f71735'],
                                ]
                            }
                        },
                    }
                    } />
                </Source>
            </MapGL>
            <Slider
                range
                min={props.slider.min}
                max={props.slider.max}
                step={10000}
                defaultValue={[props.slider.min, props.slider.max]}
                value={[min, max]}
                onChange={onChange}
                tooltipVisible={true}
            />
        </div>)
}