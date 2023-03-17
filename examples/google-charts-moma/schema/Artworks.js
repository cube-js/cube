cube(`Artworks`, {
  // Cube definition.
  // It says that the data is kept in the "artworks" table.
  // Learn more in the docs: https://cube.dev/docs/schema/getting-started
  sql: `SELECT * FROM public.artworks`,

  // Quantitative information about the data, e.g., count of rows.
  // It makes sense for all rows rather than individual rows
  measures: {
    count: {
      type: `count`,
    },

    minAgeAtAcquisition: {
      type: `number`,
      sql: `MIN(${CUBE.ageAtAcquisition})`
    },

    avgAgeAtAcquisition: {
      type: `number`,
      sql: `SUM(${CUBE.ageAtAcquisition}) / ${CUBE.count}`
    },

    maxAgeAtAcquisition: {
      type: `number`,
      sql: `MAX(${CUBE.ageAtAcquisition})`
    }
  },

  // Qualitative information about the data, e.g., an artwork's title.
  // It makes sense for individual rows of data rather than all rows
  dimensions: {
    title: {
      sql: `${CUBE}."Title"`,
      type: `string`
    },

    artist: {
      sql: `${CUBE}."Artist"`,
      type: `string`
    },

    classification: {
      sql: `${CUBE}."Classification"`,
      type: `string`
    },

    medium: {
      sql: `${CUBE}."Medium"`,
      type: `string`
    },

    // We can use SQL functions here
    year: {
      sql: `SUBSTRING(${CUBE}."Date" FROM '[0-9]{4}')`,
      type: `number`
    },

    date: {
      sql: `${CUBE}."Date"`,
      type: `number`
    },

    dateAcquired: {
      sql: `${CUBE}."DateAcquired"`,
      type: `time`
    },

    yearAcquired: {
      sql: `DATE_PART('year', ${CUBE}."DateAcquired")`,
      type: `number`
    },

    ageAtAcquisition: {
      case: {
        when: [
          {
            sql: `${CUBE.yearAcquired}::INT - ${CUBE.year}::INT > 0`,
            label: { sql: `${CUBE.yearAcquired}::INT - ${CUBE.year}::INT` }
          }
        ],
        else: {
          label: `0`
        }
      },
      type: `number`
    },

    heightCm: {
      sql: `ROUND(${CUBE}."Height (cm)")`,
      type: `number`
    },

    widthCm: {
      sql: `ROUND(${CUBE}."Width (cm)")`,
      type: `number`
    },
  },

  dataSource: `default`
});
