import React from 'react';

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
      name="description"
      content="Documentation for working with Cube.js, the open-source analytics framework."
    />

    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
    />

    <script src="/scripts/prism.js" />
    <script src="https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js" />
    <link href="/styles/content.css" rel="stylesheet" />
  </head>
);

const Prod: React.FC<Props> = (props) => (
  <head>
    <meta charSet="utf-8" />
    <meta httpEquiv="x-ua-compatible" content="ie=edge" />
    {props.headComponents}
    <meta
      name="description"
      content="Documentation for working with Cube.js, the open-source analytics framework."
    />

    <meta
      name="viewport"
      content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
    />
    <script src={`${process.env.PATH_PREFIX}/scripts/prism.js`} />
    <script
      type="text/javascript"
      src="https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js"
    ></script>
    <link
      href={`${process.env.PATH_PREFIX}/styles/content.css`}
      rel="stylesheet"
    />
    {props.css}
  </head>
);

const Head: React.FC<Props> = (props) =>
  process.env.NODE_ENV === 'production' ? (
    <Prod {...props} />
  ) : (
    <Dev {...props} />
  );

export default Head;
