---
title: Direction of Joins
permalink: /direction-of-joins
scope: cubejs
category: Guides
menuOrder: 3
---

As mentioned in [Joins](/joins) the direction of joins influences the result set greatly. For example, we have two cubes Orders and Customers:

```javascript
cube('Orders', {
  sql: `select * from orders`,

  measures: {
    count: {
      sql: 'id',
      type: 'count'
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    }
  }
});
```

```javascript
cube('Customers', {
  sql: `select * from customers`,

  measures: {
    count: {
      sql: 'id',
      type: 'count'
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    customerId: {
      sql: `customer_id`,
      type: `number`
    }
  }
});
```

The first case is to calculate total orders revenue. Let's give `totalRevenue` name for this metric. It is known that order could be placed without customer registration (so-called 'anonymous customer'). Because of anonymous customers we should join Orders then Customers in order not to lose data about anonymous orders. So we should add join to Orders cube. Cubes join and metric calculation look like following:

```javascript
cube('Orders', {
  sql: `select * from orders`,

  joins: {
    Customers: {
      relationship: `hasMany`,
      sql: `${Orders}.customer_id = ${Customers}.id`
    }
  },

  measures: {
    count: {
      sql: 'id',
      type: 'count'
    },

    totalRevenue: {
      sql: 'revenue',
      type: 'sum'
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    }
  }
});
```

The second case is to find customers without any orders. Let's call this metric `count`. In this case we should join Customers with Orders to find customers with 0 orders placed. The reverse order of joins would lead to losing customers without orders. So we add join to Customers cube. Cubes join and metric calculation would look the following way:

```javascript
cube('Customers', {
  sql: `select * from customers`,

  joins: {
    Orders: {
      relationship: `hasMany`,
      sql: `${Customers}.id = ${Orders}.customer_id`
    }
  },

  measures: {
    count: {
      sql: 'id',
      type: 'count'
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    customerId: {
      sql: `customer_id`,
      type: `number`
    }
  }
});
```
