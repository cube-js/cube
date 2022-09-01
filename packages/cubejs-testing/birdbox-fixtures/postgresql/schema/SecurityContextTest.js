cube(`SecurityContextTest`, {
  sql: `
    SELECT r.user, r.uid FROM (
      select 'admin' as user, 1 as uid
      UNION ALL
      select 'moderator' as user, 2 as uid
      UNION ALL
      select 'usr1' as user, 3 as uid
      UNION ALL
      select 'usr2' as user, 4 as uid
    ) as r
    WHERE ${SECURITY_CONTEXT.user.requiredFilter('r.user')}
  `,

  dimensions: {
    user: {
      sql: `user`,
      type: `string`,
    },
    uid: {
      sql: `uid`,
      type: `string`,
    },
  },
});
