module.exports = {
    queryRewrite: (query, { securityContext }) => {
        const cubeNames = [
            ...(Array.from(query.measures, (e) => e.split('.')[0])),
            ...(Array.from(query.dimensions, (e) => e.split('.')[0])),
        ];

        if (cubeNames.includes('Products')) {
            if (!securityContext.email) {
                throw new Error('No email found in Security Context!')
            }

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
