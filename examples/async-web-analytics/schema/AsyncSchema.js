import {
  getEventsSchema,
  transformSqlProps,
  transformSchema,
  getPageViewsSchema,
} from './utils';

asyncModule(async () => {

  const eventsSchema = await getEventsSchema();

  const eventsCube = cube(eventsSchema.title, transformSchema(eventsSchema));

  const pageViewsSchema = await getPageViewsSchema();

  const pageViewsCube = cube(pageViewsSchema.title, {
    ...pageViewsSchema,
    dimensions: transformSqlProps(pageViewsSchema.dimensions),
    measures: transformSqlProps(pageViewsSchema.measures),
  });
});
