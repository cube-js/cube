cube(`PageViews`, {
  extends: Events,
  sql: `
    SELECT
    *
    FROM ${Events.sql()} events
    WHERE events.platform = 'web' AND events.event = 'page_view'
  `
});
