import React from 'react';
import { Route } from "react-router-dom";
import App from "../../App";

const SectionRoute = ({ title, ...props }) => (
  <App title={title}>
    <Route {...props} />
  </App>
)

export default SectionRoute;
