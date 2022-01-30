import Dbt from '@cubejs-backend/dbt-schema-extension';

asyncModule(async () => {
  await Dbt.loadMetricCubesFromDbtProject('dbt-project');
});
