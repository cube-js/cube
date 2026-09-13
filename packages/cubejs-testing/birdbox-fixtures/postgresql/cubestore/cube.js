// Cube.js configuration options: https://docs.cube.dev/reference/configuration/config
process.env.CUBEJS_BATCHING_ROW_SPLIT_COUNT = '2';
module.exports = {
  queryRewrite: (query) => {
    if (query.measures) {
      query.measures = query.measures.filter(m => m !== 'Orders.toRemove');
    }
    return query;
  }
};
