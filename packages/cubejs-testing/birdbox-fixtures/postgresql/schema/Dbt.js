import Dbt from '@cubejs-backend/dbt-schema-extension';

asyncModule(async () => {
  const { MyNewProjectOrdersFiltered } = await Dbt.loadMetricCubesFromDbtProject(
    'dbt-project',
    { toExtend: ['MyNewProjectOrdersFiltered'] }
  );

  cube('OrdersFiltered', {
    extends: MyNewProjectOrdersFiltered
  });
});
