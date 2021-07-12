---
title: Dynamic Schema Creation
permalink: /schema/dynamic-schema-creation
category: Data Schema
menuOrder: 2
---

Cube.js allows schemas to be created on-the-fly using a special
[`asyncModule()`][ref-async-module] function only available in the [schema
execution environment][ref-schema-env]. `asyncModule()` allows registering an
async function to be executed at the end of the data schema compile phase so
additional definitions can be added. This is often useful in situations where
schema properties can be dynamically updated through an API, for example.

<!-- prettier-ignore-start -->
[[warning | Note]]
| Each `asyncModule` call will be invoked only once per schema compilation.
<!-- prettier-ignore-end -->

[ref-schema-env]: /schema-execution-environment
[ref-async-module]: /schema-execution-environment#asyncmodule

When creating schemas via `asyncModule()`, it is important to be aware of the
following differences compared to statically defining schemas with `cube()`:

- The `sql` and `drillMembers` properties for both dimensions and measures must
  be of type `() => string` and `() => string[]` accordingly

Cube.js supports importing JavaScript logic from other files in a schema, so it
is useful to declare utility functions for handling the above differences in a
separate file:

[ref-import-export]: /export-import

```javascript
// schema/utils.js
export const convertStringPropToFunction = (propNames, dimensionDefinition) => {
  let newResult = { ...dimensionDefinition };
  propNames.forEach((propName) => {
    const propValue = newResult[propName];

    if (!propValue) {
      return;
    }

    newResult[propName] = () => propValue;
  });
  return newResult;
};

export const transformDimensions = (dimensions) => {
  return Object.keys(dimensions).reduce((result, dimensionName) => {
    const dimensionDefinition = dimensions[dimensionName];
    return {
      ...result,
      [dimensionName]: convertStringPropToFunction(
        ['sql'],
        dimensionDefinition
      ),
    };
  }, {});
};

export const transformMeasures = (measures) => {
  return Object.keys(measures).reduce((result, dimensionName) => {
    const dimensionDefinition = measures[dimensionName];
    return {
      ...result,
      [dimensionName]: convertStringPropToFunction(
        ['sql', 'drillMembers'],
        dimensionDefinition
      ),
    };
  }, {});
};
```

## Generation

In the following example, we retrieve a JSON object representing all our cubes
using `fetch()`, transform some of the properties to be functions that return a
string, and then finally use the [`cube()` global function][ref-globals] to
generate schemas from that data:

[ref-globals]: /schema-execution-environment#cube-js-globals-cube-and-others

```javascript
// schema/DynamicSchema.js
const fetch = require('node-fetch');
import {
  convertStringPropToFunction,
  transformDimensions,
  transformMeasures,
} from './utils';

asyncModule(async () => {
  const dynamicCubes = await (
    await fetch('http://your-api-endpoint/dynamicCubes')
  ).json();

  console.log(dynamicCubes);
  // [
  //   {
  //      dimensions: {
  //        color: {
  //          sql: `color`,
  //          type: `string`,
  //        },
  //      },
  //      measures: {
  //        price: {
  //          sql: `price`,
  //          type: `number`,
  //        }
  //      },
  //      title: 'DynamicCubeSchema',
  //      sql: 'SELECT * FROM public.my_table',
  //   },
  // ]

  dynamicCubes.forEach((dynamicCube) => {
    const dimensions = transformDimensions(dynamicCube.dimensions);
    const measures = transformMeasures(dynamicCube.measures);

    cube(dynamicCube.title, {
      sql: dynamicCube.sql,
      dimensions,
      measures,
      preAggregations: {
        main: {
          type: `rollup`,
          ...
        },
      },
    });
  });
});
```

## Usage with schemaVersion

It is also useful to be able to recompile the schema when there are changes in
the underlying input data. For this purpose, the [`schemaVersion`
][link-config-schema-version] value in the `cube.js` configuration options can
be specified as an asynchronous function:

```javascript
// cube.js
module.exports = {
  schemaVersion: async ({ securityContext }) => {
    const schemaVersions = await (
      await fetch('http://your-api-endpoint/schemaVersion')
    ).json();

    return schemaVersions[securityContext.tenantId];
  },
};
```

[link-config-schema-version]: /config#options-reference-schema-version

## Usage with COMPILE_CONTEXT

The `COMPILE_CONTEXT` global object can also be used in conjunction with async
schema creation to allow for multi-tenant deployments of Cube.js.

In an example scenario where all tenants share the same cube, but see different
dimensions and measures, you could do the following:

```javascript
// schema/DynamicSchema.js
const fetch = require('node-fetch');
import { convertStringPropToFunction, transformDimensions, transformMeasures } from './utils';

asyncModule(async () => {
  const {
    securityContext: { tenantId },
  } = COMPILE_CONTEXT;

  const dynamicCubes = await (
    await fetch(`http://your-api-endpoint/dynamicCubes`)
  ).json();

  const allowedDimensions = await (
    await fetch(`http://your-api-endpoint/dynamicDimensions/${tenantId}`)
  ).json();

  const allowedMeasures = await (
    await fetch(`http://your-api-endpoint/dynamicMeasures/${tenantId}`)
  ).json();

  dynamicCubes.forEach((dynamicCube) => {
    const dimensions = transformDimensions(allowedDimensions);
    const measures = transformMeasures(allowedMeasures);

    cube(dynamicCube.title, {
      sql: dynamicCube.sql,
      title: `${dynamicCube.title}-${tenantId}`,
      dimensions,
      measures,
      preAggregations: {
        main: {
          type: `rollup`,
          ...
        },
      },
    });
  });
});
```

## Usage with dataSource

When using multiple databases, you'll need to ensure you set the
[`dataSource`][ref-schema-datasource] property for any asynchronously-created
schemas, as well as ensuring the corresponding database drivers are set up with
[`driverFactory()`][ref-config-driverfactory] in your [`cube.js` configuration
file][ref-config].

[ref-schema-datasource]: /cube#parameters-data-source
[ref-config-driverfactory]: /config#options-reference-driver-factory
[ref-config]: /config

For an example scenario where schemas may use either MySQL or Postgres
databases, you could do the following:

```javascript
// schema/DynamicSchema.js
const fetch = require('node-fetch');
import { convertStringPropToFunction, transformDimensions, transformMeasures } from './utils';

asyncModule(async () => {
  const dynamicCubes = await (
    await fetch('http://your-api-endpoint/dynamicCubes')
  ).json();

  dynamicCubes.forEach((dynamicCube) => {
    const dimensions = transformDimensions(dynamicCube.dimensions);
    const measures = transformMeasures(dynamicCube.measures);

    cube(dynamicCube.title, {
      dataSource: dynamicCube.dataSource,
      sql: dynamicCube.sql,
      dimensions,
      measures,
      preAggregations: {
        main: {
          type: `rollup`,
          ...
        },
      },
    });
  });
});
```

```javascript
// cube.js
const MySQLDriver = require('@cubejs-backend/mysql-driver');
const PostgresDriver = require('@cubejs-backend/postgres-driver');

module.exports = {
  driverFactory: ({ dataSource }) => {
    if (dataSource === 'mysql') {
      return new MySQLDriver({ database: dataSource });
    }

    return new PostgresDriver({ database: dataSource });
  },
};
```
