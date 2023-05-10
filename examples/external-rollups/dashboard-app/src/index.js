import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';

import createExampleWrapper from "@cube-dev/example-wrapper";

createExampleWrapper({
    title: "Pre-Aggregations",
    text: `<p>
        This live demo shows 
        a <a href="https://cube.dev/docs/caching#pre-aggregations">pre-aggregations</a>
        with <a href="https://cube.dev/docs/caching/using-pre-aggregations#pre-aggregations-storage">Cube Store</a>.
    </p>
    <p>You can use them to process large datasets with low-latency responses.</p>
    <p>
        Read the <a href="https://cube.dev/blog/when-mysql-is-faster-than-bigquery">story</a>
        or explore 
        the <a href="https://github.com/cube-js/cube.js/tree/master/examples/external-rollups">source code</a> 
        to learn more.
    </p>`
});

ReactDOM.render(<App />, document.getElementById('root'));
