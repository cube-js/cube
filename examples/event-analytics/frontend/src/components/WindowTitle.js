import React from 'react';
import PropTypes from 'prop-types';
import { Helmet } from "react-helmet";

const WindowTitle = ({ title }) => (
  <Helmet>
    <title>{ title }</title>
  </Helmet>
);

WindowTitle.propTypes = {
  title: PropTypes.string.isRequired
}

export default WindowTitle;
