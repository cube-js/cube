module.exports = {
  queryRewrite: (query, { securityContext }) => {
    // if (!securityContext.wikipedias) {
    //   throw new Error('Wikipedias in the Security Context are mandatory!!');
    // }

    if (securityContext.wikipedias) {
      query.filters.push({
        member: `Outliers.region`,
        operator: 'in',
        values: securityContext.wikipedias,
      });
    }

    return query;
  }
};