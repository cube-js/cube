---
title: Pagination
permalink: /recipes/pagination
category: Examples & Tutorials
subCategory: Recipes / Queries
menuOrder: 1
---

## Use case

We might want to display a table of data with hundreds of rows. For the convenience of displaying, we use pagination. To creating the pagination we can use the `limit` and `offset` query properties. At this recipe, we will get the orders list sorted by the order number. Every query will have 5 orders.

## Data schema

Let's create the following data schema with measure `count` and dimensions `number` and `createdAt`. 

```jsx
<GitHubCodeBlock
  href="https://github.com/rchkv/cubejs-pagination-recipe/blob/main/schema/Orders.js"
  titleSuffixCount={2}
  lang="js"
/>

cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    number: {
      sql: `number`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
```

## Query

First, we get the number of all orders that we have. Then we should set the `limit` and `offset` properties for the queries that will get the orders from Cube.js API.

```jsx
<GitHubCodeBlock
  href="https://github.com/rchkv/cubejs-pagination-recipe/blob/main/api.js"
  titleSuffixCount={2}
  part="get-all-orders"
  lang="js"
/>

const GET_ALL_ORDERS_QUERY = {
  measures: ["Orders.count"],
  timeDimensions: [
    {
      dimension: "Orders.createdAt",
    }
  ],
};
```

```jsx
<GitHubCodeBlock
  href="https://github.com/rchkv/cubejs-pagination-recipe/blob/main/api.js"
  titleSuffixCount={2}
  part="get-second-page"
  lang="js"
/>

const GET_SECOND_PAGE_QUERY = {
  measures: [],
  timeDimensions: [],
  order: [
    [
      "Orders.number",
      "asc"
    ]
  ],
  dimensions: ["Orders.number"],
  limit: LIMIT_PER_PAGE,
  offset: LIMIT_PER_PAGE
};
```

```jsx
<GitHubCodeBlock
  href="https://github.com/rchkv/cubejs-pagination-recipe/blob/main/api.js"
  titleSuffixCount={2}
  part="send-queries"
  lang="js"
/>

const sendQuery = (query) => {
  cubejsApi.load(query);
};

sendQuery(GET_ALL_ORDERS_QUERY);
sendQuery(GET_FIRST_PAGE_QUERY);
sendQuery(GET_SECOND_PAGE_QUERY);
```

## Result

We have received five orders per query and can use it as we want.

```jsx
<CubeQueryResultSet
  api="https://managing-aiea.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
  token="JWT_TOKEN"
  query={{ 
    measures: ["Orders.count"],
    timeDimensions: [
      {
        dimension: "Orders.createdAt",
      }
  ],
  }}
/>

<CubeQueryResultSet
  api="https://managing-aiea.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
  token="JWT_TOKEN"
  query={{ 
	measures: [],
	timeDimensions: [],
	order: [
	    [
	      "Orders.number",
	      "asc"
	    ]
	  ],
	dimensions: ["Orders.number"],
	limit: LIMIT_PER_PAGE
  }}
/>

<CubeQueryResultSet
  api="https://managing-aiea.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
  token="JWT_TOKEN"
  query={{ 
	measures: [],
	timeDimensions: [],
	order: [
	    [
	      "Orders.number",
	      "asc"
	    ]
	  ],
	dimensions: ["Orders.number"],
	limit: LIMIT_PER_PAGE,
	offset: LIMIT_PER_PAGE
  }}
/>

```

## Source

Please check out the full source code

```jsx
<GitHubFolderLink
  href="https://github.com/rchkv/cubejs-pagination-recipe"
/>
```