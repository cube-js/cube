import React from 'react'
import ReactDOM from 'react-dom'
import 'antd/dist/antd.css'
import './index.css'
import App from './App'
import { Route, HashRouter } from 'react-router-dom'

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