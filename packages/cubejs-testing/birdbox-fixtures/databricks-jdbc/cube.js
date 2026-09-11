// Cube.js configuration options: https://docs.cube.dev/reference/configuration/config
module.exports = {
  queryRewrite: (query) => {
    if (query.measures) {
      query.measures = query.measures.filter(m => m !== 'Orders.toRemove');
    }
    return query;
  }
};
