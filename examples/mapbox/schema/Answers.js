cube(`stackoverflow__answers`, {
    sql: `select * from \`bigquery-public-data.stackoverflow.posts_answers\``,
    dimensions: {
        id: {
            sql: `owner_user_id`,
            type: `number`
        },
        tags: {
            sql: `tags`,
            type: `string`
        }
    }
});