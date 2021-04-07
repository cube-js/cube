<template>
  <v-container fluid class="text-center background pa-0">
    <query-builder style="width: 100%" :cubejsApi="cubejsApi" :initialVizState="vizState">
      <template
        #builder="{
          validatedQuery,
          chartType,
          updateChartType,
          measures,
          setMeasures,
          availableMeasures,
          dimensions,
          setDimensions,
          availableDimensions,
          timeDimensions,
          setTimeDimensions,
          availableTimeDimensions,
          filters,
          setFilters,
          pivotConfig,
          updatePivotConfig,
          limit,
          setLimit,
          orderMembers,
          setOrder,
          updateOrder,
          isQueryPresent,
        }"
      >
        <v-container fluid class="pa-4 pa-md-8 pt-6 background-white">
          <div class="wrap">
            <v-row>
              <v-col cols="12" md="2">
                <v-select
                  multiple
                  label="Measures"
                  outlined
                  hide-details
                  clearable
                  :value="measures.map((i) => i.name)"
                  :items="availableMeasures.map((i) => i.name)"
                  @change="setMeasures"
                />
              </v-col>

              <v-col cols="12" md="2">
                <v-select
                  multiple
                  label="Dimensions"
                  outlined
                  hide-details
                  clearable
                  :value="dimensions.map((i) => i.name)"
                  :items="availableDimensions.map((i) => i.name)"
                  @change="setDimensions"
                />
              </v-col>

              <v-col cols="12" md="2">
                <TimeDimensionSelect
                  :availableTimeDimensions="availableTimeDimensions"
                  :timeDimensions="timeDimensions"
                  @change="setTimeDimensions"
                />
              </v-col>

              <v-col cols="12" md="1" style="min-width: 160px">
                <v-select
                  label="Granularity"
                  outlined
                  hide-details
                  clearable
                  item-text="title"
                  item-value="name"
                  :value="timeDimensions[0] && timeDimensions[0].granularity"
                  :items="GRANULARITIES"
                  @change="
                    setTimeDimensions([
                      {
                        dimension: timeDimensions[0]['dimension']['name'],
                        granularity: $event,
                        dateRange: timeDimensions[0]['dateRange'],
                      },
                    ])
                  "
                />
              </v-col>

              <v-col cols="12" md="2">
                <DateRangeSelect :timeDimensions="timeDimensions" @change="setTimeDimensions" />
              </v-col>
            </v-row>

            <v-row align="center">
              <v-col cols="2" md="2">
                <v-select
                  label="Chart Type"
                  outlined
                  hide-details
                  :value="chartType"
                  :items="chartTypes"
                  @change="updateChartType"
                />
              </v-col>

              <v-col cols="10" class="settings-button-group">
                Settings:
                <PivotConfig
                  :pivotConfig="pivotConfig"
                  :disabled="!isQueryPresent"
                  @move="(value) => handleMove(value, updatePivotConfig)"
                />

                <Order
                  :orderMembers="orderMembers"
                  :disabled="!isQueryPresent"
                  @orderChange="updateOrder.set"
                  @reorder="updateOrder.reorder"
                />

                <Limit :limit="Number(limit)" :disabled="!isQueryPresent" @update="setLimit" />
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

      <template #default="{ resultSet, isQueryPresent, validatedQuery, chartType }">
        <div v-if="!isQueryPresent">
          <v-alert color="blue" text>Choose a measure or dimension to get started</v-alert>
        </div>

        <div class="wrap pa-4 pa-md-8" v-if="resultSet && isQueryPresent">
          <div class="d-flex justify-end mb-8">
            <AddToDashboard
              @onSave="(name) => createDashboardItem({ name, query: validatedQuery, chartType })"
            ></AddToDashboard>
          </div>

          <div class="border-light pa-4 pa-md-12">
            <ChartRenderer :chart-type="chartType" :result-set="resultSet"></ChartRenderer>
          </div>
        </div>
      </template>
    </query-builder>
  </v-container>
</template>

<script>
import { QueryBuilder, GRANULARITIES } from '@cubejs-client/vue';
import gql from 'graphql-tag';

import FilterComponent from './components/FilterComponent.vue';
import PivotConfig from './components/dialogs/PivotConfig';
import Order from './components/dialogs/Order';
import Limit from './components/dialogs/Limit';
import AddToDashboard from './components/dialogs/AddToDashboard';
import TimeDimensionSelect from './components/TimeDimensionSelect';
import DateRangeSelect from './components/DateRangeSelect';
import ChartRenderer from '@/components/ChartRenderer';

export default {
  name: 'Explore',
  props: {
    cubejsApi: {
      type: Object,
      required: true,
    },
  },

  components: {
    PivotConfig,
    Order,
    Limit,
    QueryBuilder,
    FilterComponent,
    AddToDashboard,
    TimeDimensionSelect,
    DateRangeSelect,
    ChartRenderer,
  },

  data() {
    const query = {
      measures: ['Orders.count'],
      timeDimensions: [
        {
          dimension: 'Orders.createdAt',
          granularity: 'month',
          dateRange: 'This quarter',
        },
      ],
    };

    return {
      GRANULARITIES,
      selectedGranularity: {
        name: 'day',
        title: 'DAyyyy',
      },
      chartTypes: ['line', 'area', 'bar', 'pie', 'table', 'number'],
      vizState: {
        query,
        chartType: 'line',
      },
    };
  },
  methods: {
    handleMove(value, updatePivotConfig) {
      updatePivotConfig.update(value);
    },
    async createDashboardItem({ name, query, chartType }) {
      await this.$apollo.mutate({
        mutation: gql`
          mutation($input: DashboardItemInput) {
            createDashboardItem(input: $input) {
              id
              name
            }
          }
        `,
        variables: {
          input: {
            layout: '',
            vizState: JSON.stringify({
              query,
            }),
            name,
            type: chartType,
          },
        },
      });
    },
  },
};
</script>

<style>
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
