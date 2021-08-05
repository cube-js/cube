---
title: Types and Formats
permalink: /schema/reference/types-and-formats
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 7
redirect_from:
  - /types-and-formats
---

## Measures Types

This section describes the various types that can be assigned to a **measure**.
A measure can only have one type.

### number

The `sql` parameter is required and can take any valid SQL expression that
results in a number or integer. Type `number` is usually used, when performing
arithmetic operations on measures. [Learn more about Calculated
Measures][ref-schema-ref-calc-measures].

```javascript
cube('Orders', {
  measures: {
    purchasesRatio: {
      sql: `${purchases} / ${count} * 100.0`,
      type: `number`,
      format: `percent`,
    },
  },
});
```

You can put any sql into `number` measure as soon as it's an aggregate
expression:

```javascript
cube('Orders', {
  measures: {
    ratio: {
      sql: `sum(${CUBE}.amount) / count(*)`,
      type: `number`,
    },
  },
});
```

### count

Performs a table count, similar to SQL’s `COUNT` function. However, unlike
writing raw SQL, Cube.js will properly calculate counts even if your query’s
joins will produce row multiplication. You do not need to include a `sql`
parameter for this type.

`drillMembers` parameter is commonly used with type `count`. It allows users to
click on the measure in the UI and inspect individual records that make up a
count. [Learn more about Drill Downs][ref-drilldowns].

```javascript
cube('Orders', {
  measures: {
    numberOfUsers: {
      type: `count`,
      // optional
      drillMembers: [id, name, email, company],
    },
  },
});
```

### countDistinct

Calculates the number of distinct values in a given field. It makes use of SQL’s
`COUNT DISTINCT` function.

The `sql` parameter is required and can take any valid SQL expression that
results in a table column, or interpolated Javascript expression.

```javascript
cube('Orders', {
  measures: {
    uniqueUserCount: {
      sql: `user_id`,
      type: 'countDistinct',
    },
  },
});
```

### countDistinctApprox

Calculates approximate number of distinct values in a given field. Unlike
`countDistinct` measure type, `countDistinctApprox` is decomposable aggregate
function or **additive**. This allows its usage in **additive** [rollup
pre-aggregations][ref-schema-ref-preaggs-rollup] which are much more versatile
than **non-additive** ones. It uses special SQL backend-dependent functions to
estimate distinct counts, usually based on HyperLogLog or similar algorithms.
Wherever possible Cube.js will use multi-stage HLL which significantly improves
calculation of distinct counts at scale.

The `sql` parameter is required and can take any valid SQL expression.

```javascript
cube('Orders', {
  measures: {
    uniqueUserCount: {
      sql: `user_id`,
      type: 'countDistinctApprox',
    },
  },
});
```

### sum

Adds up the values in a given field. It is similar to SQL’s `SUM` function.
However, unlike writing raw SQL, Cube.js will properly calculate sums even if
your query’s joins will result in row duplication.

The `sql` parameter is required and can take any valid SQL expression that
results in a numeric table column, or interpolated Javascript expression. `sql`
parameter should contain only expression to sum without actual aggregate
function.

```javascript
cube('Orders', {
  measures: {
    revenue: {
      sql: `${chargesAmount}`,
      type: `sum`,
    },
  },
});
```

```javascript
cube('Orders', {
  measures: {
    revenue: {
      sql: `amount`,
      type: `sum`,
    },
  },
});
```

```javascript
cube('Orders', {
  measures: {
    revenue: {
      sql: `fee * 0.1`,
      type: `sum`,
    },
  },
});
```

### avg

Averages the values in a given field. It is similar to SQL’s AVG function.
However, unlike writing raw SQL, Cube.js will properly calculate averages even
if your query’s joins will result in row duplication.

The sql parameter for type: average measures can take any valid SQL expression
that results in a numeric table column, or interpolated Javascript expression.

```javascript
cube('Orders', {
  measures: {
    averageTransaction: {
      sql: `${transactionAmount}`,
      type: `avg`,
    },
  },
});
```

### min

Type of measure `min` is calculated as a minimum of values defined in `sql`.

```javascript
cube('Orders', {
  measures: {
    dateFirstPurchase: {
      sql: `date_purchase`,
      type: `min`,
    },
  },
});
```

### max

Type of measure `max` is calculated as a maximum of values defined in `sql`.

```javascript
cube('Orders', {
  measures: {
    dateLastPurchase: {
      sql: `date_purchase`,
      type: `max`,
    },
  },
});
```

### runningTotal

Type of measure `runningTotal` is calculated as summation of values defined in
`sql`. Use it to calculate cumulative measures.

```javascript
cube('Orders', {
  measures: {
    totalSubscriptions: {
      sql: `subscription_amount`,
      type: `runningTotal`,
    },
  },
});
```

## Measures Formats

When creating a **measure** you can explicitly define the format you’d like to
see as output.

### percent

`percent` is used for formatting numbers with a percent symbol.

```javascript
cube('Orders', {
  measures: {
    purchaseConversion: {
      sql: `${purchase}/${checkout}*100.0`,
      type: `number`,
      format: `percent`,
    },
  },
});
```

### currency

`currency` is used for monetary values.

```javascript
cube('Orders', {
  measures: {
    totalAmount: {
      sql: `amount`,
      type: `runningTotal`,
      format: `currency`,
    },
  },
});
```

## Dimensions Types

This section describes the various types that can be assigned to a
**dimension**. A dimension can only have one type.

### time

In order to be able to create time series charts, Cube.js needs to identify time
dimension which is a timestamp column in your database.

You can define several time dimensions in schemas and apply each when creating
charts. Note that type of target column should be `TIMESTAMP`. Please use [this
guide][ref-string-time-dims] if your datetime information is stored as a string.

```javascript
cube('Orders', {
  dimensions: {
    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },
  },
});
```

### string

`string` is typically used with fields that contain letters or special
characters. The `sql` parameter is required and can take any valid SQL
expression.

The following JS code creates a field `fullName` by combining 2 fields:
`firstName` and `lastName`:

```javascript
cube('Orders', {
  dimensions: {
    fullName: {
      sql: `CONCAT(${firstName}, ' ', ${lastName})`,
      type: `string`,
    },
  },
});
```

### number

`number` is typically used with fields that contain number or integer.

```javascript
cube('Orders', {
  dimensions: {
    amount: {
      sql: `amount`,
      type: `number`,
    },
  },
});
```

### boolean

`boolean` is used with fields that contain boolean data or data coercible to
boolean. For example:

```javascript
cube('Orders', {
  dimensions: {
    isEnabled: {
      sql: `is_enabled`,
      type: `boolean`,
    },
  },
});
```

### geo

`geo` dimension is used to display data on the map. Unlike other dimension types
it requires to set two fields: latitude and longitude.

```javascript
cube('Orders', {
  dimensions: {
    location: {
      type: `geo`,
      latitude: {
        sql: `${CUBE}.latitude`,
      },
      longitude: {
        sql: `${CUBE}.longitude`,
      },
    },
  },
});
```

## Dimensions Formats

### imageUrl

`imageUrl` is used for displaying images in table visualization. In this case
`sql` parameter should contain full path to the image.

```javascript
cube('Orders', {
  dimensions: {
    image: {
      sql: `CONCAT('https://img.example.com/id/', ${id})`,
      type: `string`,
      format: `imageUrl`,
    },
  },
});
```

### id

`id` is used for IDs. It allows to eliminate applying of comma for 5+ digit
numbers which is default for type `number`. The `sql` parameter is required and
can take any valid SQL expression.

```javascript
cube('Orders', {
  dimensions: {
    image: {
      sql: `id`,
      type: `number`,
      format: `id`,
    },
  },
});
```

### link

`link` is used for creating hyperlinks. `link` parameter could be either String
or Object. Use Object, when you want to give a specific label to link. See
examples below for details.

The `sql` parameter is required and can take any valid SQL expression.

```javascript
cube('Orders', {
  dimensions: {
    orderLink: {
      sql: `'http://myswebsite.com/orders/' || id`,
      type: `string`,
      format: `link`,
    },

    crmLink: {
      sql: `'https://na1.salesforce.com/' || id`,
      type: `string`,
      format: {
        label: `View in Salesforce`,
        type: `link`,
      },
    },
  },
});
```

### currency

`currency` is used for monetary values.

```javascript
cube('Orders', {
  dimensions: {
    amount: {
      sql: `abount`,
      type: `number`,
      format: `currency`,
    },
  },
});
```

### percent

`percent` is used for formatting numbers with a percent symbol.

```javascript
cube('Orders', {
  dimensions: {
    openRate: {
      sql: `COALESCE(100.0 * ${uniqOpenCount} / NULLIF(${deliveredCount}, 0), 0)`,
      type: `number`,
      format: `percent`,
    },
  },
});
```

[ref-string-time-dims]: /working-with-string-time-dimensions
[ref-schema-ref-preaggs-rollup]:
  /schema/reference/pre-aggregations#parameters-type-rollup
[ref-schema-ref-calc-measures]: /schema/reference/measures#calculated-measures
[ref-drilldowns]: /drill-downs
