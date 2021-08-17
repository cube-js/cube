module.exports = {
    queryRewrite: (query, { securityContext }) => {
        const queryDimensions = Array.from(query.dimensions, element => element.split('.')[0]);
        const queyMeasures =  Array.from(query.measures, element => element.split('.')[0]);

        if (queryDimensions.includes('Products') || queyMeasures.includes('Products')) {
            query.filters.push(
                {
                    member: `Suppliers.email`,
                    operator: 'equals',
                    values: [securityContext.email],
                }
            )
        };

        return query;
    }
};