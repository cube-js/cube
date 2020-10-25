---
title: Many-to-Many Relationship
permalink: /many-to-many-relationship
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 17
---
<div class="block attention-block">
  This is an advanced topic that assumes good, pre-existing knowledge of SQL and Cube.js.
</div>

A many-to-many relationship is a type of cardinality that refers to the relationship between two entities, A and B, in which A may contain a parent instance for which there are many children in B and vice versa.

For example, we have Topics and Posts. A Post can cover many Topics, and a Topic could be covered by many Posts.

In a database, in this case, you most likely have an associative table (also known as a junction table or cross-reference table). In our example, this table would be `post_topics`. 

<div class="block help-block">
You can <a href="many-to-many-relationship#many-to-many-relationship-without-an-associative-table">jump to this section</a> if you don’t have an associative table in your database.
</div>

The diagram below shows the tables `posts`, `topics`, `post_topics`, and their relationship.

![many-to-many-1.png](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/Guides/many-to-many-1.png)

In the same way the `PostTopics` table was specifically created to handle this association in DB, we need to create an associative cube `PostTopics`, and declare the relationships from it to `Topics` cube and from `Posts` to `PostTopics`. 
Please note, we’re using the `hasMany` relationship on the `PostTopics` cube and direction of joins is selected to be `Posts -> PostTopics -> Topics`. 
[Read more about direction of joins here](direction-of-joins).

```javascript
cube(`Posts`, {
  sql: `select * from posts`,
  
  // ...

  joins: {
    PostTopics: {
      relationship: `belongsTo`,
      sql: `${PostTopics}.post_id = ${Posts}.id`
    }
  }
});

cube(`Topics`, {
  sql: `select * from topics`
});

cube(`PostTopics`, {
  sql: `select * from post_topics`,

  joins: {
    Topic: {
      relationship: `hasMany`,
      sql: `${PostTopics}.topic_id = ${Topics}.id`
    }
  }
});
```

In case when a table doesn't have a primary key you can define it manually as follows

```javascript
cube(`PostTopics`, {
  // ...
  
  dimensions: {
    id: {
      sql: `CONCAT(${CUBE}.post_id, ${CUBE}.topic_id)`,
      type: `number`,
      primaryKey: true
    },
  }
});
```

## Many-to-Many Relationship Without an Associative Table 
Sometimes there is no associative table in the database, when in reality, there is a many-to-many relationship. In this case, the solution is to extract some data from existing tables and create a virtual (not backed by a real table in the database) associative cube.

Let’s consider the following example. We have tables `Emails` and `Transactions`. The goal is to calculate the amount of transactions per campaign. Both `Emails` and `Transactions` have a `campaign_id` column. We don’t have a campaigns table, but data about campaigns is part of the `Emails` table.

Let’s take a look at the `Emails` cube first.


```javascript
cube(`Emails`, {
  sql: `select * emails`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    campaignName: {
      sql: `campaign_name`,
      type: `string`
    },

    campaignId: {
      sql: `campaign_id`,
      type: `number`
    },
  }
});
```

We can extract campaigns data into a virtual `Campaigns` cube. 

```javascript

cube(`Campaigns`, {
  sql: `select campaign_id, campaign_name, customer_name, min(created_at) started_at from emails GROUP BY 1, 2, 3`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    id: {
      sql: `campaign_id`,
      type: `string`,
      primaryKey: true
    },

    name: {
      sql: `campaign_name`,
      type: `string`
    }
  }
});
```

The following diagram shows our data schema with the `Campaigns` cube.

![many-to-many-1.png](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/Guides/many-to-many-2.png)

The last piece is to finally declare a many-to-many relationship. This should be done by declaring a `hasMany` relationship on the associative cube, `Campaigns` in our case. 

```javascript
cube(`Emails`, {
  sql: `select * emails`,
  
  joins: {
    Campaigns: {
      relationship: `belongsTo`,
      sql: `${Emails}.campaign_id = ${Campaigns}.campaign_id
      AND ${Emails}.customer_name = ${Campaigns}.customer_name`
    },
  }

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    campaignName: {
      sql: `campaign_name`,
      type: `string`
    },

    campaignId: {
      sql: `campaign_id`,
      type: `number`
    },
  }
});


cube(`Campaigns`, {
  joins: {
    Transactions: {
      relationship: `hasMany`,
      sql: `${Transactions}.customer_name = ${Campaigns}.customer_name
      AND ${Transactions}.campaign_id = ${Campaigns}.campaign_id`
    }
  }
});
```


