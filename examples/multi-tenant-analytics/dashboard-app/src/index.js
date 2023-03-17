import React from 'react';
import ReactDOM from 'react-dom';
import { HashRouter as Router, Route } from 'react-router-dom';
import { Auth0Provider } from "@auth0/auth0-react";
import createExampleWrapper from "@cube-dev/example-wrapper";
import ExplorePage from './pages/ExplorePage';
import DashboardPage from './pages/DashboardPage';
import App from './App';

const exampleDescription = {
  title: "Multi Tenant Analytics",
};

createExampleWrapper(exampleDescription);

ReactDOM.render(
  <Auth0Provider
    audience={process.env.REACT_APP_AUTH0_AUDIENCE}
    domain={process.env.REACT_APP_AUTH0_DOMAIN}
    clientId={process.env.REACT_APP_AUTH0_CLIENT_ID}
    scope={'openid profile email'}
    redirectUri={process.env.REACT_APP_AUTH0_REDIRECT_URI || window.location.origin}
    onRedirectCallback={() => {}}
  >
    <Router>
      <App>
        <Route key="index" exact path="/" component={DashboardPage} />
        <Route key="explore" path="/explore" component={ExplorePage} />
      </App>
    </Router>
  </Auth0Provider>,
document.getElementById('root'));