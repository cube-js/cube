<template xmlns:v-slot="http://www.w3.org/1999/XSL/Transform">
  <v-container>
    <v-row class="text-center">
      <query-builder :cubejs-api="cubejsApi" :query="query" style="width: 100%">
        <template v-slot:builder="{measures,setMeasures,availableMeasures, dimensions, setDimensions, availableDimensions}"
        >
          <v-row>
            <v-col cols="3">
              <v-select
                multiple
                :value="measures.map(i => (i.name))"
                @change="setMeasures"
                :items="availableMeasures.map(i => (i.name))"
              />
            </v-col>
            <v-col cols="3">
              <v-select
                multiple
                :value="dimensions.map(i => (i.name))"
                @change="setDimensions"
                :items="availableDimensions.map(i => (i.name))"
              />
            </v-col>
          </v-row>

        </template>

        <template v-slot="{ resultSet }">
          <v-col cols="12" v-if="resultSet">
            <line-chart :data="series(resultSet)"></line-chart>
          </v-col>
        </template>
      </query-builder>
    </v-row>
  </v-container>
</template>

<script>
import cubejs from '@cubejs-client/core'
import { QueryBuilder } from '@cubejs-client/vue'

const API_URL = process.env.NODE_ENV === 'production' ? '' : 'http://localhost:4000'
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTE4NjM4MDEsImV4cCI6MTU5NDQ1NTgwMX0.NW6yiMgiZz_LCnkRn-dunzyqTRO9K7L-k5FpNn2-iCA'
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
})

export default {
  name: 'HelloWorld',

  components: {
    QueryBuilder
  },
  data: () => {
    const query = {
      limit: 100,
      measures: [
        'Orders.count'
      ],
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month'
        }
      ]
    }

    return {
      cubejsApi,
      innerMeasures: [],
      query
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
    }
  }
}
</script>
