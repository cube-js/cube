---
title: Dimensions
permalink: /dimensions
scope: cubejs
category: Reference
menuOrder: 4
---

`Dimensions` parameter contains a set of dimensions. You can think about dimension as an attribute related to a measure, e.g. measure `userCount` can have dimensions like `country`, `age`, `occupation` and etc.

Any dimension should have name, sql parameter and type.

You can name dimension following the same rules as for measure, so each name should:
- Be unique within a cube
- Start with a lowercase letter

You can use `0-9`, `_` and letters when naming dimension.

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
You can use `title` parameter to change dimension displayed name. By default Cube.js will humanize your dimension key to create a display name.
In order to override default behaviour please use `title` parameter.

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
You can add details to dimension definition via `description` parameter.

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
You can manage the visibility of the dimension using `shown` parameter. The default value of `shown` is `true`.

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
It contains two parameters `when` and `then`.
The first `when` statement declares condition and result if rule returns true value.
The second `else` statement declares result for options when rules return false value.


The following example will create `size` dimension with values 'xl' and 'xxl'.

```javascript
size: {
  type: `string`,
  case: {
    when: [
        { sql: `${TABLE}.meta_value = 'xl-en'`, label: `xl` },
        { sql: `${TABLE}.meta_value = 'xl'`, label: `xl` },
        { sql: `${TABLE}.meta_value = 'xxl-en'`, label: `xxl` },
        { sql: `${TABLE}.meta_value = 'xxl'`, label: `xxl` },
    ],
    else: { label: `Unknown` }
  }
}
```

### primaryKey
Specify which dimension is a primary key for Cube. Default value is `false`.

Primary key is used to make [joins](joins) work properly.

<div class="block help-block">
  <p>
    <b>Note:</b>
    Setting <code>primaryKey</code> to <code>true</code> will change the default value of <code>shown</code>
    parameter to <code>false</code>. If you still want <code>shown</code> to be <code>true</code> - set it manually.
  </p>
</div>

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
The `subQuery` statement allows you to reference measure in a dimension. It's an advanced concept and you can learn more about it [here](subquery).

```javascript
dimensions: {
  usersCount: {
    sql: `${Users.count}`,
    type: `number`,
    subQuery: true
  }
}
```
