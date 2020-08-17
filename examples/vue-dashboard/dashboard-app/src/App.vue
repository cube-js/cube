<template>
  <div class="container-fluid">
    <div class="row">
      <div class="col-sm-4">
        <query-builder :cubejs-api="cubejsApi" :query="usersQuery">
          <template v-slot="{ loading, resultSet }">
            <Chart title="Total Users" type="number" :loading="loading" :result-set="resultSet"/>
          </template>
        </query-builder>
      </div>
      <div class="col-sm-4">
        <query-builder :cubejs-api="cubejsApi" :query="totalOrdersQuery">
          <template v-slot="{ loading, resultSet }">
            <Chart title="Total Orders" type="number" :loading="loading" :result-set="resultSet"/>
          </template>
        </query-builder>
      </div>
      <div class="col-sm-4">
        <query-builder :cubejs-api="cubejsApi" :query="shippedOrdersQuery">
          <template v-slot="{ loading, resultSet }">
            <Chart title="Shipped Users" type="number" :loading="loading" :result-set="resultSet"/>
          </template>
        </query-builder>
      </div>
    </div>
    <br>
    <br>
    <div class="row">
      <div class="col-sm-6">
        <query-builder :cubejs-api="cubejsApi" :query="lineQuery">
          <template v-slot="{ loading, resultSet }">
            <Chart
                    title="New Users Over Time"
                    type="line"
                    :loading="loading"
                    :result-set="resultSet"
            />
          </template>
        </query-builder>
      </div>
      <div class="col-sm-6">
        <query-builder :cubejs-api="cubejsApi" :query="barQuery">
          <template v-slot="{ loading, resultSet }">
            <Chart
                    title="Orders by Status Over time"
                    type="stackedBar"
                    :loading="loading"
                    :result-set="resultSet"
            />
          </template>
        </query-builder>
      </div>
    </div>
  </div>
</template>

<script>
  import cubejs from "@cubejs-client/core";
  import { QueryBuilder } from "@cubejs-client/vue";

  import Chart from "./components/Chart";

  const cubejsApi = cubejs(
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTQ2NjY4OTR9.0fdi5cuDZ2t3OSrPOMoc3B1_pwhnWj4ZmM3FHEX7Aus",
    { apiUrl: "https://ecom.cubecloudapp.dev/cubejs-api/v1" }
  );

  export default {
    name: "App",
    components: {
      Chart,
      QueryBuilder
    },
    data() {
      return {
        cubejsApi,
        usersQuery: { measures: ["Users.count"] },
        totalOrdersQuery: { measures: ["Orders.count"] },
        shippedOrdersQuery: {
          measures: ["Orders.count"],
          filters: [
            {
              dimension: "Orders.status",
              operator: "equals",
              values: ["shipped"]
            }
          ]
        },
        lineQuery: {
          measures: ["Users.count"],
          timeDimensions: [
            {
              dimension: "Users.createdAt",
              dateRange: ["2019-01-01", "2020-12-31"],
              granularity: "month"
            }
          ]
        },
        barQuery: {
          measures: ["Orders.count"],
          dimensions: ["Orders.status"],
          timeDimensions: [
            {
              dimension: "Orders.createdAt",
              dateRange: ["2019-01-01", "2020-12-31"],
              granularity: "month"
            }
          ]
        }
      };
    }
  };
</script>

<style>
  html {
    -webkit-font-smoothing: antialiased;
  }

  body {
    padding-top: 30px;
    padding-bottom: 30px;
    background: #f5f6f7;
  }
</style>
