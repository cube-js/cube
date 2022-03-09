import React from "react";
import ReactDOM from "react-dom";
import "./index.css";
import App from "./App";
import { HashRouter as Router, Route } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";
ReactDOM.render(
  <React.StrictMode>
    <Router>
      <App>
        <Route key="index" exact path="/" component={DashboardPage} />
      </App>
    </Router>
  </React.StrictMode>,
  document.getElementById("root")
); 
