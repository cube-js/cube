import React from 'react';

const Dev = props => (
  <head>
    <meta charSet="utf-8" />
    <meta httpEquiv="x-ua-compatible" content="ie=edge" />
    {props.headComponents}
    <meta name="description" content="Documentation for working with Cube.js, the open-source analytics framework." />

    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"/>

    <script src="/scripts/prism.js"/>
    <script src="https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js"/>
    <link href="/styles/content.css" rel="stylesheet" />
  </head>
);

const Prod = props => (
  <head>
    <meta charSet="utf-8" />
    <meta httpEquiv="x-ua-compatible" content="ie=edge" />
    {props.headComponents}
    <meta name="description" content="Documentation for working with Cube.js, the open-source analytics framework." />

    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/docsearch.js@2/dist/cdn/docsearch.min.js"></script>
    <script src={`${process.env.PATH_PREFIX}/scripts/prism.js`} />
    <script src={`${process.env.PATH_PREFIX}/scripts/analytics.js`} />
    <script src={`${process.env.PATH_PREFIX}/scripts/fullstory.js`} />
    <link href={`${process.env.PATH_PREFIX}/styles/content.css`} rel="stylesheet" />
    <script dangerouslySetInnerHTML={{ __html: `
      (function(h,o,t,j,a,r){
          h.hj=h.hj||function(){(h.hj.q=h.hj.q||[]).push(arguments)};
          h._hjSettings={hjid:1405282,hjsv:6};
          a=o.getElementsByTagName('head')[0];
          r=o.createElement('script');r.async=1;
          r.src=t+h._hjSettings.hjid+j+h._hjSettings.hjsv;
          a.appendChild(r);
      })(window,document,'https://static.hotjar.com/c/hotjar-','.js?sv=');
    `}} />
    <script dangerouslySetInnerHTML={{ __html: `
       (function(m,e,t,r,i,k,a){m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
       m[i].l=1*new Date();k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a)})
       (window, document, "script", "https://mc.yandex.ru/metrika/tag.js", "ym");

       ym(54473377, "init", {
            clickmap:true,
            trackLinks:true,
            accurateTrackBounce:true,
            webvisor:true
       });
    `}} />
<noscript><div><img src="https://mc.yandex.ru/watch/54473377" style={{ position: 'absolute', left:-9999 }} alt="" /></div></noscript>
    {props.css}
  </head>
);

const Head = props => (
  process.env.NODE_ENV === 'production' ? <Prod {...props} /> : <Dev {...props} />
);

export default Head;
