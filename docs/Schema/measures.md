---
title: Measures
permalink: /measures
scope: cubejs
category: Reference
---

`measures` parameter contains a set of measures and each measure is an aggregation over certain column in your database table. Any measure should have name, sql parameter and type.

When you give a name to measure there are certain rules to follow. Each measure should:
- Be unique within a cube
- Start with a lowercase letter

You can use `0-9`,`_` and letters when naming measure.

```javascript
cube(`Orders`, {
  measures: {
    count: {
      sql: `id`,
      type: `count`
    },

    totalAmount: {
      sql: `amount`,
      type: `sum`
    }
  }
});
```

## Parameters
### type
`type` is required parameter. There are various types that could be assigned to
a measure. Please refer to [Measure Types Guide](types-and-formats#measures-types) for the full list of measure types.

```javascript
ordersCount: {
  sql: `id`,
  type: `count`
}
```

### sql
`sql` is required parameter. It can take any valid SQL expression depending on the `type` of the measure.
Please refer to [Measure Types Guide](types-and-formats#measures-types) for detailed information on corresponding sql parameter.

```javascript
usersCount: {
  sql: `count(*)`,
  type: `number`
}
```

### format
`format` is an optional parameter. It is used to format the output of measures in different ways, for example as currency for `revenue`.
Please refer to [Measure Formats Guide](types-and-formats#measures-formats) for the full list of supported formats.

```javascript
total: {
  sql: `amount`,
  type: `runningTotal`,
  format: `currency`
}
```

### title
You can use `title` parameter to change measure displayed name. By default Cube.js will humanize your measure key to create a display name.
In order to override default behaviour please use `title` parameter.

```javascript
ordersCount: {
  sql: `id`,
  type: `count`,
  title: `Number of Orders Placed`
}
```

### description
You can add details to measure definition via `description` parameter.

```javascript
ordersCount: {
  sql: `id`,
  type: `count`,
  description: `count of all orders`
}
```

### shown
You can manage the visibility of the measure using `shown` parameter. The default value of `shown` is `true`.

```javascript
ordersCount: {
  sql: `id`,
  type: `count`,
  shown: false
}
```

### filters
If you want to add some conditions for metric's calculation, you should use `filters` parameter. Syntax looks the following way:

```javascript
ordersCompletedCount: {
  sql: `id`,
  type: `count`,
  filters: [
    { sql: `${TABLE}.status = 'completed'` }
  ]
}
```

### rollingWindow
If you want to calculate some metric within a window, for example a week or a month, you should use a `rollingWindow` parameter. `trailing` and `leading` parameters define window size.

These parameters has format defined as `(-?\d+) (minute|hour|day|week|month|year)`. `trailing` and `leading` parameters can also be set to `unbounded` value which means infinite size for the corresponding window part. You can define `trailing` and `leading` parameters using negative integers.

The `traling` parameter is a window part size before the `offset` point and the `leading` parameter is after it. You can set the window `offset` parameter to either `start` or `end`, which will match start or end of the selected date range.
By default, `leading` and `trailing` paremeters are set to zero and `offset` is set to `end`.

```javascript
rollingCountMonth: {
    sql: `id`,
    type: `count`,
    rollingWindow: {
      trailing: `1 month`
    }
  }
```

### drillMembers
Using `drillMembers` parameter you can define a set of [drill down](drill-downs) fields for the measure. `drillMembers` is defined as an array of dimensions. Cube.js automatically injects dimensions names and other cubes names with dimensions in the context, so you can reference these variables in `drillMembers` array.
[Learn more how to define and use drill downs](drill-downs)

```javascript
revenue: {
  type: `sum`,
  sql: `price`,
  drillMembers: [id, price, status, Products.name, Products.id]
}
```

## Calculated Measures
In case when you need to specify a formula for measure calculating with other measures, you can compose a formula in `sql`. For example, you want to calculate conversion of buyers of all users.

```javascript
purchasesToCreatedAccountRatio: {
  sql: `${purchases} / ${Users.count} * 100.0`,
  type: `number`,
  format: `percent`
}
```
You can create Calculated Measures from several joined cubes. In this case join will be created automatically.
