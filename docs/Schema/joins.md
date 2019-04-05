---
title: Joins
permalink: /joins
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 5
---

`joins` parameter declares a block to define relationships between cubes.
It allows users to access and compare fields from two or more cubes at the same time.

```javascript
joins: {
  TargetCubeName: {
    relationship: `belongsTo` || `hasMany` || `hasOne`,
    sql: `SQL ON clause`
  }
}
```

## Parameters

### relationship

`relationship` enables you to describe the join relationship between joined cubes.
It’s important to properly define the type of relationship in order for Cube.js
to calculate accurate measures.

<div class="block help-block">
  <p><b>Note:</b> It is very important to define correct order of cubes in join. It affects data in the result-set greatly.</p>
  <p>The basic cube present the left entity in join, all others would be right. That means that all rows of the left cube are selected, while rows of the right depend on the condition.</p>
  <p>For more information and specific examples, please take a look at our <a href="direction-of-joins">Guides</a>.</p>
</div>


The three possible values for relationship are:

#### hasOne

A `hasOne` relationship indicates a one-to-one connection with another cube. This relationship
indicates that the one row in the cube can match only one rows in the joined cube. For example,
in a model containing users and user profiles, users cube would have the following join:

```javascript
cube("Users", {
  joins: {
    Profile: {
      relationship: `hasOne`,
      sql: `${Users}.id = ${Profile}.user_id`
    }
  }
});
```

#### hasMany

A `hasMany` relationship indicates a one-to-many connection with another cube.
You'll often find this relationship on the "other side" of a `belongsTo`
relationship. This relationship indicates that the one row in the cube can match many rows in the joined cube.
For example, in a model containing authors and books, authors cube would have the following join:

```javascript
cube("Authors", {
  joins: {
    Books: {
      relationship: `hasMany`,
      sql: `${Authors}.id = ${Books}.author_id`
    }
  }
});
```

#### belongsTo

A `belongsTo` relationship indicates a many-to-one connection with another cube. You’ll often find
this relationship on the “other side” of a `hasMany` relationship. This relationship indicates that
the one row of the declaring cube match row in the joined instance, while the joined instance can
have many of row in declaring cube. For example, in a model containing orders and customers, orders cube would
have the following join:

```javascript
cube("Orders", {
  joins: {
    Customers: {
      relationship: `belongsTo`,
      sql: `${Orders}.customer_id = ${Customers}.id`
    }
  }
});
```

### sql

`sql` is necessary to indicate a related column between cubes. It is important to properly specify
matching column when creating joins. Take a look at example below:
```javascript
  sql: `${Orders}.customer_id = ${Customers}.id`
```

## Setting Primary Key

In order to make `join` work it is necessary to define `primaryKey` as specified below.
It's required when join is defined because Cube.js takes care about row multiplication issue.

Let's imagine you want to calculate `Order Amount` by `Order Item Product Name`.
In this case `Order` rows will be multiplied by `Order Item` join due to `hasMany` relationship.
In order to produce correct results Cube.js will select distinct primary keys of `Order` first and then will join these primary keys with `Order` to get correct `Order Amount` sum result.
Please note that `primaryKey` should be defined in `dimensions` section.

```javascript
dimensions: {
  authorId: {
    sql: `id`,
    type: `number`,
    primaryKey: true
  }
}
```
<div class="block help-block">
  <p>
    <b>Note:</b>
    Setting <code>primaryKey</code> to <code>true</code> will change the default value of <code>shown</code>
    parameter to <code>false</code>. If you still want <code>shown</code> to be <code>true</code> - set it manually.
  </p>
</div>

```javascript
dimensions: {
  authorId: {
    sql: `id`,
    type: `number`,
    primaryKey: true,
    shown: true
  }
}
```

## CUBE reference

When you have several joined cubes you should accurately use column's names to avoid any mistakes. One way to make no mistake is to use `${CUBE}` reference. It allows to specify column's names in cubes without any ambiguity. During the implementation of the query this reference will be used as an alias for basic cube. Take a look at the following example:

```javascript
dimensions: {
  name: {
    sql: `${CUBE}.name`,
    type: `string`
  }
}
```
