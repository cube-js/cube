import React from "react";
import ReactDOM from "react-dom";
import { HashRouter as Router, Route } from "react-router-dom";
import { Auth } from '@aws-amplify/auth';

import { AmplifyProvider } from "./libs/amplify";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import DashboardPage from "./pages/DashboardPage";
import "./index.css";
import config from './auth_config';

Auth.configure(config);

ReactDOM.render(
  <React.StrictMode>
    <AmplifyProvider>
      <Router>
        <App>
          <Route key="index" exact path="/" component={DashboardPage} />
        </App>
      </Router>
    </AmplifyProvider>
  </React.StrictMode>,
  document.getElementById("root")
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

serviceWorker.unregister();
