cube(`StatusSnapshots`, {
  extends: Statuses,

  sql: `
    -- Create a range from the earlist date to the latest date
    WITH range AS (
      SELECT date
      FROM GENERATE_SERIES(
        (SELECT MIN(changed_at) FROM ${Statuses.sql()} AS statuses),
        (SELECT MAX(changed_at) FROM ${Statuses.sql()} AS statuses),
        INTERVAL '1 DAY'
      ) AS date
    )
    
    -- Calculate snapshots for every date in the range
    SELECT range.date, statuses.*
    FROM range
    LEFT JOIN ${Statuses.sql()} AS statuses
      ON range.date >= statuses.changed_at
      AND statuses.changed_at = (
        SELECT MAX(changed_at)
        FROM ${Statuses.sql()} AS sub_statuses
        WHERE sub_statuses.order_id = statuses.order_id
      )
  `,
  
  dimensions: {
    date: {
      sql: `date`,
      type: `time`,
    },
  }
});