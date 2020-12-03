---
title: Dynamically Union Tables
permalink: /dynamically-union-tables
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 23
---

[comment]: # (PROOFREAD: DONE)

It is quite often the case that you may have a lot of different tables in a database, which actually relate to the same entity. 

For example, you can have “per client” tables with the same data, but related to different customers:  `elon_musk_table`, `john_doe_table`, `steve_jobs_table`, etc. In this case, it would make sense to **create a single Cube for customers**, which should be backed by a union table from all customers tables.

It would be annoying to union all required tables manually. Luckily, since Cube.js is a javascript framework, we have the full power of javascript at our disposal. We **can write a function, which will generate a union table from all our customers’ tables**.


```javascript
const customerTableNames = [
  {name: 'Albert Einstein', tablePrefix: 'albert_einstein'},
  {name: 'Blaise Pascal', tablePrefix: 'blaise_pascal'},
  {name: 'Isaac Newton', tablePrefix: 'isaac_newton'},
  {name: 'Charles Darwin', tablePrefix: 'charles_darwin'},
  {name: 'Michael Faraday', tablePrefix: 'michael_faraday'},
  {name: 'Enrico Fermi', tablePrefix: 'enrico_fermi'},
  {name: 'Thomas Edison', tablePrefix: 'thomas_edison'}
  ];

function unionData() {
  return customerTableNames.map(p => `select
                  name,
                  email,
                  id,
                  order_id,
                  created_at,
                  '${p.name}' customer_name
                  from ${p.tablePrefix}_customer
                  `).join(" UNION ALL ");
}
```

Then we can use the `unionData()` function inside the `Customers` cube. `customer_name` would become a dimension to allow us to break down the data by certain customers.

```javascript
cube(`Customers`, {
 sql: unionData(),

 measures: {
   count: {
     type: `count`
   }
 },

 dimensions: {
   customerName: {
     sql: `customer_name`,
     type: `string`
   }
 }
});
```
