module.exports = {
  queryRewrite: (query) => {
      query.filters.push({
        member: `Orders.createdAt`,
        operator: 'afterDate',
        values: ['2019-12-30'],
      });

    return query;
  },
};
