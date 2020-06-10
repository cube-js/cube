import React, { useState } from 'react';
import MapGL, { Source, Layer, Popup } from 'react-map-gl';
import { Radio } from "antd";


export default (props) => {

    const [viewport, setViewport] = useState({
        latitude: 34,
        longitude: 5,
        zoom: 1,
    })

    const [popupInfo, setPopupInfo] = useState(null);

    const [mode, setMode] = useState('both');

    const onChange = (e) => {
        setMode(e.target.value)
    }

    const onClick = (event) => {
        if (typeof event.features != 'undefined') {
            const feature = event.features.find(
                (f) => f.layer.id === 'questions-point'
            );

            if (feature) {
                setPopupInfo(feature);
            }
            else {
                setPopupInfo(null);
            }
        }
    };

    const renderPopup = () => {
        console.log(popupInfo);
        return popupInfo == null ? (
            <React.Fragment />
        ) : (
                <Popup
                    tipSize={5}
                    anchor='top'
                    longitude={popupInfo.geometry.coordinates[0]}
                    latitude={popupInfo.geometry.coordinates[1]}
                >
                    <h3>{popupInfo.properties.title}</h3>
                    <div>
                        View count: {popupInfo.properties.views}<br />
                        Tags: {popupInfo.properties.tags}
                    </div>
                </Popup>
            );
    };

    return (
        <div className='mapbox__container'>
            <div className="mapbox__legend">
                <Radio.Group onChange={onChange} defaultValue="both">
                    <Radio.Button value="qu">questions</Radio.Button>
                    <Radio.Button value="ans">answers</Radio.Button>
                    <Radio.Button value="both">both</Radio.Button>
                </Radio.Group>
            </div>
            <MapGL
                {...viewport}
                onViewportChange={(viewport) => {
                    setViewport(viewport)
                }}
                width='100%'
                height='600px'
                onClick={onClick}
                onHover={onClick}
                mapStyle='mapbox://styles/kalipsik/ckb2fyfqu123n1ilb5yi7uyns/draft'
                mapboxApiAccessToken='pk.eyJ1Ijoia2FsaXBzaWsiLCJhIjoiY2p3Z3JrdjQ4MDRjdDQzcGFyeXBlN3ZtZiJ9.miVaze_snePdEvitucFWSQ'
            >
                <Source type='geojson' data={props.answers}>
                    <Layer {...{
                        id: 'answers-point',
                        type: 'circle',
                        filter: (mode != 'qu') ? ['!', ['has', 'non_exist']] : ['has', ['get', 'id']],
                        paint: {
                            'circle-radius': 2,
                            'circle-stroke-width': 0,
                            'circle-opacity': 1,
                            'circle-color': '#0f0',
                        }
                    }} />
                </Source>

                <Source type='geojson' data={props.questions}>
                    <Layer {...{
                        id: 'questions-point',
                        type: 'circle',
                        filter: (mode != 'ans') ? ['!', ['has', 'non_exist']] : ['has', ['get', 'id']],
                        paint: {
                            'circle-radius': 2,
                            'circle-stroke-width': 0,
                            'circle-opacity': 1,
                            'circle-color': '#f00',
                        }
                    }} />
                </Source>

                {renderPopup()}
            </MapGL>
        </div>
    )
}