const fetchStatuses = require('../fetch').fetchStatuses;

asyncModule(async () => {
  const statuses = await fetchStatuses()

  const createValue = (status, index) =>
    `MIN(orders_${index}.created_at) AS createdAt_${index}`

  const createJoin = (status, index) =>
    `LEFT JOIN public.orders AS orders_${index}
      ON users.id = orders_${index}.user_id
      AND orders_${index}.status = '${status}'`

  const createDimension = (status, index) => ({
    [`${status}CreatedAt`]: {
      sql: (CUBE) => `createdAt_${index}`,
      type: `time`,
    }
  })

  cube(`UsersStatuses_Dynamic`, {
    sql: `
      SELECT
        users.first_name,
        users.last_name,
        ${statuses.map(createValue).join(',')}
      FROM public.users AS users
      ${statuses.map(createJoin).join('')}
      GROUP BY 1, 2
    `,
    
    dimensions: Object.assign(
      {
        name: {
          sql: `first_name || ' ' || last_name`,
          type: `string`
        }
      },
      statuses.reduce((all, status, index) => ({
        ...all,
        ...createDimension(status, index)
      }), {})
    )
  });
});