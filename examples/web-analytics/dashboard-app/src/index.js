import React from "react";
import ReactDOM from "react-dom";
import 'typeface-roboto';
import "./index.css";
import App from "./App";
import * as serviceWorker from "./serviceWorker";
import { HashRouter as Router, Route } from "react-router-dom";

import ReportPage from "./pages/ReportPage";

import AudiencePage from "./pages/AudiencePage";
import BehaviorPage from "./pages/BehaviorPage";
import AcquisitionPage from "./pages/AcquisitionPage";
import CustomReportPage from "./pages/CustomReportPage";

import CustomReportsOverviewPage from "./pages/CustomReportsOverviewPage";
import CustomReportsBuilderPage from "./pages/CustomReportsBuilderPage";

ReactDOM.render(
  <Router>
    <App>
      <Route exact path="/"
        render={() => <ReportPage report={AudiencePage} />} />
      <Route exact path="/behavior"
        render={() => <ReportPage report={BehaviorPage} />} />
      <Route exact path="/acquisition"
        render={() => <ReportPage report={AcquisitionPage} />} />
      <Route exact path="/custom-reports-overview"
        component={CustomReportsOverviewPage} />
      <Route exact path="/custom-reports-builder/:id?"
        component={CustomReportsBuilderPage} />
      <Route exact path="/custom-reports/:id"
        render={() => <ReportPage report={CustomReportPage} />} />
    </App>
  </Router>,
  document.getElementById("root")
); // If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA

serviceWorker.unregister();
