import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { HashRouter as Router, Route } from "react-router-dom";
import { Auth0Provider } from "@auth0/auth0-react";
import DashboardPage from "./pages/DashboardPage";
import config from './auth_config';
import history from './history';

const onRedirectCallback = (appState) => {
  history.push(
    appState && appState.targetUrl
      ? appState.targetUrl
      : window.location.pathname,
  );
};

ReactDOM.render(
   <Auth0Provider
    audience={config.audience}
    domain={config.domain}
    clientId={config.clientId}
    redirectUri={window.location.origin}
    onRedirectCallback={onRedirectCallback}
  >
    <React.StrictMode>
      <Router>
        <App>
          <Route key="index" exact path="/" component={DashboardPage} />
        </App>
      </Router>
    </React.StrictMode>
  </Auth0Provider>,
  document.getElementById("root")
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

serviceWorker.unregister();
