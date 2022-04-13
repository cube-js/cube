```shell
$ yarn install
$ yarn dev
$ curl -G --data-urlencode 'query={
  "order": {
    "Products.name": "asc"
  },
  "dimensions": [
    "Products.name",
    "Suppliers.company",
    "Suppliers.email"
  ],
  "limit": 3
}' http://localhost:4000/cubejs-api/v1/load | jq '{data, usedPreAggregations, error}'
```