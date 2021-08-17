module.exports = {
    queryRewrite: (query) => {
        function isEmpty(obj) {
            return Object.keys(obj).length === 0;
        }

        const dimensions = Array.from(query.dimensions, element => element.split('.')[0]);
        const measures =  Array.from(query.measures, element => element.split('.')[0]);
        const filterItems = dimensions.concat(measures);

        filterItems.forEach(
            element => query.filters.push(
                {
                    member: `${element}.createdAt`,
                    operator: 'afterDate',
                    values: ['2019-12-30'],
                }
            ));

        return query;
    },
};