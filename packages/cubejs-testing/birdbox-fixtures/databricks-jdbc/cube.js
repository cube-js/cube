// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
  queryRewrite: (query) => {
    if (query.measures) {
      query.measures = query.measures.filter(m => m !== 'Orders.toRemove');
    }
    return query;
  }
};
