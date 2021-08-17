module.exports = {
    queryRewrite: (query) => {
        function isEmpty(obj) {
            return Object.keys(obj).length === 0;
        }

        let cube = !isEmpty(query.dimensions) ? query.dimensions[0].split('.')[0] : ''

        
        
        query.filters.push({
            member: `${cube}.createdAt`,
            operator: 'afterDate',
            values: ['2021-12-01'],
        });

        return query;
    },
};