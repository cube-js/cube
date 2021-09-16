cube(`Users`, {
  sql: `
    WITH data AS (
      SELECT 
        users.id AS id,
        users.city AS city,
        users.gender AS gender
      FROM public.users
    ),
    
    cities AS (
      SELECT city
      FROM data
    ),
    
    grouped AS (
      SELECT 
        cities.city AS city_filter,
        data.id AS id,
        data.city AS city,
        data.gender AS gender
      FROM cities, data
      GROUP BY 1, 2, 3, 4
    )
    
    SELECT *
    FROM grouped
  `,

  measures: {
    totalNumberOfWomen: {
      sql: 'id',
      type: 'count',
      filters: [{ sql: `${CUBE}.gender = 'female'` }],
    },

    numberOfPeopleOfAnyGenderInTheCity: {
      sql: 'id',
      type: 'count',
      filters: [{ sql: `${CUBE}.city = ${CUBE}.city_filter` }],
    },

    ratio: {
      title: 'Ratio Women in the City to Total Number of People',
      sql: `1.0 * ${CUBE.numberOfPeopleOfAnyGenderInTheCity} / ${CUBE.totalNumberOfWomen}`,
      type: `number`,
    },
  },

  dimensions: {
    cityFilter: {
      sql: `city_filter`,
      type: `string`,
    },
  }
});
