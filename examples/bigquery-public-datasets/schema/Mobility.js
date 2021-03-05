cube(`Mobility`, {
  sql: `
    SELECT *
    FROM \`bigquery-public-data.covid19_google_mobility.mobility_report\`
  `,

  refreshKey: {
    sql: `
      SELECT COUNT(*)
      FROM \`bigquery-public-data.covid19_google_mobility.mobility_report\`
    `,
  },

  measures: {
    grocery: {
      sql: `grocery_and_pharmacy_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },

    park: {
      sql: `parks_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },

    residential: {
      sql: `residential_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },

    retail: {
      sql: `retail_and_recreation_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },

    transit: {
      sql: `transit_stations_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },

    workplace: {
      sql: `workplaces_percent_change_from_baseline`,
      type: `max`,
      format: 'percent',
    },
  },

  dimensions: {
    key: {
      sql: `CONCAT(country_region, '-', sub_region_1, '-', sub_region_2, '-', ${Mobility}.date)`,
      type: `string`,
      primaryKey: true
    },

    country: {
      sql: `country_region`,
      type: `string`
    },

    date: {
      sql: `TIMESTAMP(${Mobility}.date)`,
      type: `time`
    },
  },

  joins: {
    Measures: {
      sql: `${Measures}.country_name = ${Mobility}.country_region AND ${Measures}.date = ${Mobility}.date`,
      relationship: `hasOne`
    }
  }
});