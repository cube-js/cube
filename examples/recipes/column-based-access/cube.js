module.exports = {
    queryRewrite: (query, { securityContext }) => {
        const cubeNames = [
          ...query.dimensions || [],
          ...query.measures || [],
        ].map((e) => e.split('.')[0]);

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
