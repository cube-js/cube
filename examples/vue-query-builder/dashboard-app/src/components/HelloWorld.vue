<template xmlns:v-slot="http://www.w3.org/1999/XSL/Transform">
  <v-container fluid class="text-center background pa-0">
    <query-builder :cubejs-api="cubejsApi" :query="query" style="width: 100%">
      <template
        #builder="{
          measures,
          setMeasures,
          availableMeasures,
          dimensions,
          setDimensions,
          availableDimensions,
          timeDimensions,
          setTimeDimensions,
          availableTimeDimensions,
          availableFilters,
          filters,
          setFilters,
          pivotConfig
        }"
      >
        <v-container fluid class="pa-4 pa-md-8 pt-6 background-white">
          <div>DEBUG: {{ pivotConfig }}</div>
          <div class="wrap">
            <v-btn
              color="primary"
              depressed
              elevation="2"
              raised
              v-on:click="setMeasures(['Orders.count', 'Orders.number'])"
              >add member</v-btn
            >
            <v-btn color="primary" depressed elevation="2" raised v-on:click="setMeasures(['Sales.count'])"
              >remove member</v-btn
            >

            <v-btn color="primary" depressed elevation="2" raised v-on:click="print">Click Me!</v-btn>

            <v-row>
              <v-col cols="12" md="2">
                <v-select
                  multiple
                  label="Measures"
                  outlined
                  hide-details
                  :value="measures.map((i) => i.name)"
                  @change="setMeasures"
                  :items="availableMeasures.map((i) => i.name)"
                />
              </v-col>

              <v-col cols="12" md="2">
                <v-select
                  multiple
                  label="Dimensions"
                  outlined
                  hide-details
                  :value="dimensions.map((i) => i.name)"
                  :items="availableDimensions.map((i) => i.name)"
                  @change="setDimensions"
                />
              </v-col>

              <v-col cols="12" md="2">
                <v-select
                  label="Time Dimensions"
                  outlined
                  hide-details
                  :value="timeDimensions[0] && timeDimensions[0].dimension.name"
                  :items="availableTimeDimensions.map((i) => i.name)"
                  @change="
                    setTimeDimensions([
                      {
                        dimension: $event,
                        granularity: timeDimensions[0].granularity,
                        dateRange: timeDimensions[0].dateRange,
                      },
                    ])
                  "
                />
              </v-col>

              <v-col cols="12" md="1" style="min-width: 160px">
                <v-select
                  label="Granularity"
                  outlined
                  hide-details
                  @change="
                    setTimeDimensions([
                      {
                        dimension: timeDimensions[0].dimension.name,
                        granularity: $event,
                        dateRange: timeDimensions[0].dateRange,
                      },
                    ])
                  "
                  item-text="title"
                  item-value="name"
                  :value="timeDimensions[0] && timeDimensions[0].granularity"
                  :items="GRANULARITIES"
                />
              </v-col>

              <v-col cols="12" md="2">
                <v-select
                  label="Date Range"
                  outlined
                  hide-details
                  :value="timeDimensions[0] && timeDimensions[0].dateRange"
                  :items="dateRangeItems"
                  @change="
                    setTimeDimensions([
                      {
                        dimension: timeDimensions[0].dimension.name,
                        granularity: timeDimensions[0].granularity,
                        dateRange: $event,
                      },
                    ])
                  "
                />
              </v-col>
            </v-row>

            <v-row align="center" >
              <v-col cols="2" md="2">
                <v-select label="Chart Type" outlined hide-details v-model="type" :items="['line', 'table']" />
              </v-col>

              <v-col cols="10" class="settings-button-group">
                Settings:
                <PivotConfig :pivotConfig="pivotConfig" />

                <v-btn>Order</v-btn>
                <v-btn>Limit</v-btn>
              </v-col>
            </v-row>

            <FilterComponent
              :filters="filters"
              :dimensions="availableDimensions.map((i) => i.name)"
              :measures="availableMeasures.map((i) => i.name)"
              :setFilters="setFilters"
            ></FilterComponent>
          </div>
        </v-container>
      </template>

      <template v-slot="{ resultSet }" v-if="false">
        <div class="wrap pa-4 pa-md-8" v-if="resultSet">
          <div class="border-light pa-4 pa-md-12">
            <line-chart legend="bottom" v-if="type === 'line'" :data="series(resultSet)"></line-chart>
            <Table v-if="type === 'table'" :data="resultSet"></Table>
          </div>
        </div>
      </template>
    </query-builder>
  </v-container>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryBuilder, GRANULARITIES } from '@cubejs-client/vue';
import FilterComponent from './FilterComponent.vue';
import Table from './Table';
import PivotConfig from '@/components/dialogs/PivotConfig';

const API_URL = 'https://ecom.cubecloudapp.dev';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7fSwiaWF0IjoxNjA3NDQwMTQ0LCJleHAiOjE2MTAwMzIxNDR9.Za52BRvDvtgzqgy44QC5C35Li2RZ1RZAGy2mDdIWY70';
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

// const API_URL = 'http://localhost:4000';
// const CUBEJS_TOKEN =
//   'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1Ijp7fSwiaWF0IjoxNjA3NDQwMTQ0LCJleHAiOjE2MTAwMzIxNDR9.Za52BRvDvtgzqgy44QC5C35Li2RZ1RZAGy2mDdIWY70';
// const cubejsApi = cubejs(CUBEJS_TOKEN, {
//   apiUrl: `${API_URL}/cubejs-api/v1`,
// });

export default {
  name: 'HelloWorld',

  components: {
    PivotConfig,
    QueryBuilder,
    FilterComponent,
    Table,
  },
  data() {
    let query = {};

    query = {
      measures: ['Orders.count', 'Orders.number'],
      dimensions: ['Orders.status'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'month',
          dateRange: 'this quarter'
        },
      ],
      filters: [],
    };

    console.log('qqq',query)

    // query = {};

    return {
      selectedGranularity: {
        name: 'day',
        title: 'DAyyyy',
      },
      cubejsApi,
      query,
      dateRangeItems: [
        'Today',
        'Yesterday',
        'This week',
        'This month',
        'This quarter',
        'This year',
        'Last 30 days',
        'Last year',
      ],
      type: 'line',
      GRANULARITIES,
      pivotConfigDialog: false
    };
  },
  methods: {
    print() {
      console.log('clicked!');
    },
    series(resultSet) {
      const seriesNames = resultSet.seriesNames();
      const pivot = resultSet.chartPivot();
      const series = [];
      seriesNames.forEach((e) => {
        const data = pivot.map((p) => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
  },
};
</script>

<style scopped>
.background {
  background: #f3f3fb;
  min-height: 100vh;
}
.background-white {
  background: #fff;
}
.border-light {
  background: #ffffff;
  border-radius: 8px;
}

.settings-button-group {
  text-align: left;
}

.settings-button-group button {
  margin-right: 12px;
}
</style>
