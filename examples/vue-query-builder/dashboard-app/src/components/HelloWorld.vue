<template xmlns:v-slot="http://www.w3.org/1999/XSL/Transform">
  <v-container>
    <v-row class="text-center">
      <query-builder :cubejs-api="cubejsApi" :query="query">
        <template v-slot:builder="{measures,setMeasures,availableMeasures, dimensions, setDimensions, availableDimensions}"
        >
          <v-row>
            <v-col cols="6">
              <v-select
                multiple
                :value="measures.map(i => (i.name))"
                @change="setMeasures"
                :items="availableMeasures.map(i => (i.name))"
              />
            </v-col>
            <v-col cols="6">
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

const cubejsApi = cubejs(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.K9PiJkjegbhnw4Ca5pPlkTmZihoOm42w8bja9Qs2qJg',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' }
)
export default {
  name: 'HelloWorld',

  components: {
    QueryBuilder
  },
  data: () => {
    const query = {
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
