cube(`stackoverflow__users`, {
    sql: `select * from \`bigquery-public-data.stackoverflow.users\``,


    dimensions: {
        id: {
            sql: `id`,
            type: `number`
        },
        location: {
            sql: `location`,
            type: `string`
        },
        name: {
            sql: `display_name`,
            type: `string`
        },
        reputation: {
            sql: `reputation`,
            type: `number`
        }
    }
});