# Joining Multiple Databases Data

This example demonstrates how to join data from multiple databases (PostgreSQL and BigQuery) using Cube.

## Setup

1. Configure the `.env` file with your database credentials:

```env
# Suppliers datasource (Postgres)
CUBEJS_DS_SUPPLIERS_DB_HOST=your-postgres-host
CUBEJS_DS_SUPPLIERS_DB_NAME=your-database
CUBEJS_DS_SUPPLIERS_DB_USER=your-user
CUBEJS_DS_SUPPLIERS_DB_PASS=your-password

# Products datasource (BigQuery)
CUBEJS_DS_PRODUCTS_BQ_PROJECT_ID=your-project-id
CUBEJS_DS_PRODUCTS_EXPORT_BUCKET=your-export-bucket
CUBEJS_DS_PRODUCTS_BQ_CREDENTIALS={"type":"service_account",...}
```

2. Start Cube:

```shell
docker-compose up cube
```

3. Run the sample query:

```shell
docker-compose up query
```

Or query manually:

```shell
curl -G --data-urlencode 'query={
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
