---
title: Dimensions
permalink: /dimensions
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 4
proofread: 06/18/2019
---

The `dimensions` parameter contains a set of dimensions. You can think about a dimension as an attribute related to a measure, e.g. the measure `userCount` can have dimensions like `country`, `age`, `occupation`, etc.

Any dimension should have a name, sql parameter, and type.

You can name a dimension by following the same rules as for measure, so each name should:
- Be unique within a cube
- Start with a lowercase letter

You can use `0-9`, `_`, and letters when naming a dimension.

```javascript
cube(`Products`, {
  dimensions: {
    price: {
      sql: `price`,
      type: `number`
    },

    brandName: {
      sql: `brand_name`,
      type: `string`
    }
  }
});
```

## Parameters

### title
You can use the `title` parameter to change a dimension’s displayed name. By default, Cube.js will humanize your dimension key to create a display name.
In order to override default behavior, please use the `title` parameter.

```javascript
dimensions: {
  metaValue: {
    type: `string`,
    sql: `meta_value`,
    title: `Size`
  }
}
```

### description
You can add details to a dimension’s definition via the `description` parameter.

```javascript
dimensions: {
  comment: {
    type: `string`,
    sql: `comments`,
    description: `Comments for orders`
  }
}
```

### shown
You can manage the visibility of the dimension using the `shown` parameter. The default value of `shown` is `true`.

```javascript
dimensions: {
  comment: {
    type: `string`,
    sql: `comments`,
    shown: false
  }
}
```

### case
The `case` statement is used to define if/then/else conditions to display data.
It contains two parameters: `when` and `then`.
The first `when` statement declares a condition and result if the rule returns a true value.
The second `else` statement declares results for options when rules return a false value.


The following static `label` example will create a `size` dimension with values 'xl' and 'xxl'.

```javascript
size: {
  type: `string`,
  case: {
    when: [
        { sql: `${CUBE}.meta_value = 'xl-en'`, label: `xl` },
        { sql: `${CUBE}.meta_value = 'xl'`, label: `xl` },
        { sql: `${CUBE}.meta_value = 'xxl-en'`, label: `xxl` },
        { sql: `${CUBE}.meta_value = 'xxl'`, label: `xxl` },
    ],
    else: { label: `Unknown` }
  }
}
```

`label` can be defined dynamically as an object with a `sql` property.

```javascript
size: {
  type: `string`,
  case: {
    when: [
        { sql: `${CUBE}.meta_value = 'xl-en'`, label: {sql: `${CUBE}.english_size`} },
        { sql: `${CUBE}.meta_value = 'xl'`, label: {sql: `${CUBE}.euro_size`} },
        { sql: `${CUBE}.meta_value = 'xxl-en'`, label: {sql: `${CUBE}.english_size`} },
        { sql: `${CUBE}.meta_value = 'xxl'`, label: {sql: `${CUBE}.euro_size`} },
    ],
    else: { label: `Unknown` }
  }
}
```

### primaryKey
Specify which dimension is a primary key for a cube. The default value is `false`.

A primary key is used to make [joins](joins) work properly.

[[warning | Note]]
| Setting `primaryKey` to `true` will change the default value of `shown` parameter to `false`. If you still want `shown` to be `true` - set it manually.

```javascript
dimensions: {
  id: {
    sql: `id`,
    type: `number`,
    primaryKey: true
  }
}
```

### subQuery
The `subQuery` statement allows you to reference a measure in a dimension. It's an advanced concept and you can learn more about it [here](subquery).

```javascript
dimensions: {
  usersCount: {
    sql: `${Users.count}`,
    type: `number`,
    subQuery: true
  }
}
```

### propagateFiltersToSubQuery
When this statement is set to `true` the filters applied to the query will be passed to the subquery

```javascript
dimensions: {
  usersCount: {
    sql: `${Users.count}`,
    type: `number`,
    subQuery: true,
    propagateFiltersToSubQuery: true
  }
}
```

### meta
Custom metadata. Can be used to pass any information to the frontend.
```javascript
dimensions: {
  usersCount: {
    sql: `${Users.count}`,
    type: `number`,
    //...
    meta: {
      any: 'value'
    }
  }
}
```
