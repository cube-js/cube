module.exports = {
    queryRewrite: (query) => {
        function isEmpty(obj) {
            return Object.keys(obj).length === 0;
        }

        let arr = Array.from(query.dimensions, element => element.split('.')[0])

        // let cube = !isEmpty(query.dimensions) ? query.dimensions[0].split('.')[0] : ''

        const reducerFn = (elem) => {
            return {
                member: `${elem}.createdAt`,
                operator: 'afterDate',
                values: ['2019-12-30'],
                }
            };

        query.filters.push(
            arr.reduce(reducerFn)
        );

        return query;
    },
};