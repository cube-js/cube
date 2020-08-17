<template xmlns:v-slot="http://www.w3.org/1999/XSL/Transform">
  <v-container fluid class="text-center background pa-0">
    <query-builder :cubejs-api="cubejsApi" :query="query" style="width: 100%">
      <template
        v-slot:builder="{
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
          }"
      >
        <v-container fluid class="pa-4 pa-md-8 pt-6 background-white">
          <div class="wrap">
            <v-row>
              <v-col cols="12" md="2" >
                <v-select
                  multiple
                  label="Measures"
                  outlined
                  hide-details
                  :value="measures.map(i => (i.name))"
                  @change="setMeasures"
                  :items="availableMeasures.map(i => (i.name))"
                />
              </v-col>
              <v-col cols="12" md="2" >
                <v-select
                  multiple
                  label="Dimensions"
                  outlined
                  hide-details
                  :value="dimensions.map(i => (i.name))"
                  @change="setDimensions"
                  :items="availableDimensions.map(i => (i.name))"
                />
              </v-col>
              <v-col cols="12" md="2" >
                <v-select
                  label="Time Dimensions"
                  outlined
                  hide-details
                  :value="timeDimensions[0]['dimension']['name']"
                  @change="setTimeDimensions([{dimension: $event, granularity: timeDimensions[0]['granularity'], dateRange: timeDimensions[0]['dateRange']}])"
                  :items="availableTimeDimensions.map(i => (i.name))"
                />
              </v-col>
              <v-col cols="12" md="1" style="min-width: 120px">
                <v-select
                  label="Granularity"
                  outlined
                  hide-details
                  @change="setTimeDimensions([{dimension: timeDimensions[0]['dimension']['name'], granularity: $event, dateRange: timeDimensions[0]['dateRange']}])"
                  :value="timeDimensions[0]['granularity']"
                  :items="timeDimensions[0]['dimension']['granularities'].map(obj => obj.name)"
                />
              </v-col>
              <v-col cols="12" md="2" >
                <v-select
                  label="Date Range"
                  outlined
                  hide-details
                  :value="timeDimensions[0]['dateRange']"
                  @change="setTimeDimensions([{dimension: timeDimensions[0]['dimension']['name'], granularity: timeDimensions[0]['granularity'], dateRange: $event}])"
                  :items="dateRangeItems"
                />
              </v-col>
            </v-row>
            <v-row>
              <v-col cols="12" md="2" >
                <v-select
                  label="Chart Type"
                  outlined
                  hide-details
                  v-model="type"
                  :items="['line', 'table']"
                />
              </v-col></v-row>
            <FilterComponent :filters="filters"
                             :dimensions="availableDimensions.map(i => (i.name))"
                             :measures="availableMeasures.map(i => (i.name))"
                             :setFilters="setFilters"
            ></FilterComponent>
          </div>
        </v-container>
      </template>

      <template v-slot="{ resultSet }">
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
import cubejs from '@cubejs-client/core'
import { QueryBuilder } from '@cubejs-client/vue'
import FilterComponent from './FilterComponent.vue'
import Table from './Table'

const API_URL = process.env.NODE_ENV === 'production' ? '' : 'http://localhost:4000'
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTQ2NjY4OTR9.0fdi5cuDZ2t3OSrPOMoc3B1_pwhnWj4ZmM3FHEX7Aus'
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
})

export default {
  name: 'HelloWorld',

  components: {
    QueryBuilder,
    FilterComponent,
    Table
  },
  data: () => {
    const query = {
      limit: 100,
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'day',
          dateRange: 'Last 30 days'
        }
      ],
      filters: [
      ]
    }

    return {
      cubejsApi,
      query,
      dateRangeItems: ['Today', 'Yesterday', 'This week', 'This month', 'This quarter', 'This year', 'Last 30 days', 'Last year'],
      type: 'line',
    }
  },
  methods: {
    series (resultSet) {
      const seriesNames = resultSet.seriesNames()
      const pivot = resultSet.chartPivot()
      const series = []
      seriesNames.forEach((e) => {
        const data = pivot.map(p => [p.x, p[e.key]])
        series.push({ name: e.key, data })
      })
      return series
    },
  }
}
</script>

<style scopped>
  .background {
    background: #F3F3FB;
    min-height: 100vh;
  }
  .background-white {
    background: #fff;
  }
  .border-light {
    background: #FFFFFF;
    border-radius: 8px;
  }
</style>
