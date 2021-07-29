<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Playground

UI for Cube.js development server environment.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

### React Components

`@cubejs-client/playground` provides standalone components you can embed in your application.

```jsx
import { QueryBuilder } from '@cubejs-client/playground';
// import the antd styles from the `@cubejs-client/playground` package as it overrides some variables
import '@cubejs-client/playground/lib/antd.min.css';
// alternatively you can use the default antd styles
// import 'antd/dist/antd.min.css';

const apiUrl = 'http://localhost:4000/cubejs-api/v1';
const token = 'your.token';

export default function App() {
  const query = {
    measures: ['Orders.count'],
    dimensions:  ['Orders.status']
  };

  return (
    <QueryBuilder
      apiUrl={apiUrl}
      token={token}
      initialVizState={{
        query
      }}
    />
  );
}
```

Also, you will need to move the Playground chart renderers to a public folder. Assuming the publicly accessible folder is `public`, you can execute the following script from the root of your application

```bash
#!/bin/bash

rm -rf ./public/chart-renderers 2> /dev/null
cp -R ./node_modules/@cubejs-client/playground/public/chart-renderers ./public
```

### License

Cube.js Client Core is [MIT licensed](./LICENSE).
