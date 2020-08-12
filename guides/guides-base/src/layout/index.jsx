import React from "react";
import Helmet from "react-helmet";
import { createGlobalStyle } from 'styled-components'
import { normalize } from 'styled-normalize'
import theme from '../theme';
import "../prism.css";

import { page } from 'cubedev-tracking';


const Global = createGlobalStyle`
  ${normalize};
  @import url('https://fonts.googleapis.com/css?family=DM+Sans&display=swap&css');

  body {
    font-family: ${theme.fontFamily};
  }
`

export default class MainLayout extends React.Component {
  componentDidMount() {
    page();
  }
  render() {
    const { children, config } = this.props;
    return (
      <div>
        <Helmet>
          <meta name="description" content={config.siteDescription} />
          <html lang="en" />
        </Helmet>
        <Global />
        {children}
      </div>
    );
  }
}
