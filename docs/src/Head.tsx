import React from 'react';
import { DEPLOY_PREVIEW_NETLIFY } from 'gatsby-env-variables';

type Props = {
  css?: any;
  headComponents: any;
};

const Dev: React.FC<Props> = (props) => (
  <head>
    <meta charSet="utf-8" />
    <meta httpEquiv="x-ua-compatible" content="ie=edge" />
    {props.headComponents}

    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
    />

    <link href="/styles/content.css" rel="stylesheet" />
    
    <link rel="preconnect" href={`https://${process.env.ALGOLIA_APP_ID}-dsn.algolia.net`} crossOrigin="true" />
  </head>
);

const Prod: React.FC<Props> = (props) => (
  <head>
    <meta charSet="utf-8" />
    <meta httpEquiv="x-ua-compatible" content="ie=edge" />
    {props.headComponents}

    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
    />
    <link
      href={`${process.env.PATH_PREFIX}/styles/content.css`}
      rel="stylesheet"
    />
    {props.css}
    
    <link rel="preconnect" href={`https://${process.env.ALGOLIA_APP_ID}-dsn.algolia.net`} crossOrigin="true" />
  </head>
);

const Head: React.FC<Props> = (props) => {
  return process.env.NODE_ENV === 'production' &&
    !DEPLOY_PREVIEW_NETLIFY ? (
    <Prod {...props} />
  ) : (
    <Dev {...props} />
  );
}

export default Head;
