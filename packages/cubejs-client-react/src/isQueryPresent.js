export default (query) => ((Array.isArray(query) ? query : [query]).every(
  (q) => (q.measures && q.measures.length)
    || (q.dimensions && q.dimensions.length)
    || (q.timeDimensions && q.timeDimensions.length)
));
