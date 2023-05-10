module.exports = {
    queryRewrite: (query, { securityContext }) => {
      if (!securityContext.domain) {
        throw new Error("Please specify the domain");
      }
  
      query.filters.push({
        member: 'GithubCommitStatsCommitsCached.authorDomain',
        operator: 'equals',
        values: [ securityContext.domain ],
      });
  
      return query;
    }
  };
  