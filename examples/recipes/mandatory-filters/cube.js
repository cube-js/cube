module.exports = {
    queryRewrite: (query) => {
        const dimensions = [...new Set(Array.from(query.dimensions, element => element.split('.')[0]))]
        const measures = [...new Set(Array.from(query.measures, element => element.split('.')[0]))]
        const filterItems = dimensions.concat(measures);

        filterItems.forEach(
            item => query.filters.push(
                {
                    member: `${item}.createdAt`,
                    operator: 'afterDate',
                    values: ['2019-12-30'],
                }
            ));

        return query;
    },
};
