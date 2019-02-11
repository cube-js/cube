import React from 'react';
import { Route } from "react-router-dom";
import App from "../../App";

const SectionRoute = (props) => (
  <App>
    <Route {...props} />
  </App>
)

export default SectionRoute;
