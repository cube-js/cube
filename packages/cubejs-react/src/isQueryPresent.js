export default (query) => (
  query.measures && query.measures.length
    || query.dimensions && query.dimensions.length
    || query.timeDimensions && query.timeDimensions.length
);
