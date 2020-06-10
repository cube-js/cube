cube(`stackoverflow__questions`, {
    sql: `select * from \`bigquery-public-data.stackoverflow.posts_questions\``,
    dimensions: {
        id: {
            sql: `id`,
            type: `number`,
            primaryKey: true,
            shown: true
        },
        owner_user_id: {
            sql: `owner_user_id`,
            type: `number`,
        },
        title: {
            sql: `title`,
            type: `string`
        },
        tags: {
            sql: `tags`,
            type: `string`
        },
        views: {
            sql: `view_count`,
            type: `number`,
        }
    }
});