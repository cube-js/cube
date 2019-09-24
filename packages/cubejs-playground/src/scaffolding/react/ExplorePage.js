import React from 'react';
import ExploreQueryBuilder from "./QueryBuilder/ExploreQueryBuilder";

const ExplorePage = ({ cubejsApi }) => (
  <ExploreQueryBuilder
    cubejsApi={cubejsApi}
  />
);

export default ExplorePage;
