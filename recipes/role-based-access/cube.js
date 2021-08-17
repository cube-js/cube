module.exports = {
    queryRewrite: (query, { securityContext }) => {
      if (!securityContext.role) {
        throw new Error('No role found in Security Context!')
      }

      if (securityContext.role == 'manager') {
        query.filters.push({
          member: 'Orders.status',
          operator: 'equals',
          values: ['shipped', 'completed'],
        });
      };

      if (securityContext.role == 'operator') {
        query.filters.push({
          member: 'Orders.status',
          operator: 'equals',
          values: ['processing'],
        });
      }

      return query;
    },
};
