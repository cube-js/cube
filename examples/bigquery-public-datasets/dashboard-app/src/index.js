import React from 'react'
import ReactDOM from 'react-dom'
import 'antd/dist/antd.css'
import './index.css'
import App from './App'
import { Route, HashRouter } from 'react-router-dom'
import createExampleWrapper from "@cube-dev/example-wrapper";

const exampleDescription = {
    title: "BigQuery Public Datasets — COVID-19 impact",
    text: `These reports are based on <a href="https://console.cloud.google.com/marketplace/browse?filter=category:covid19">public datasets for COVID-19 research</a><br>hosted on Google Cloud Platform and queried with Cube.js.`,
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
    <React.StrictMode>
        <HashRouter hashType='noslash'>
            <Route render={props => (
                <App country={props.location.pathname.substring(1)} />
            )} />
        </HashRouter>
    </React.StrictMode>,
    document.getElementById('root')
)