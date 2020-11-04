cube(`Issues`, {
  sql: `SELECT * FROM public.issues`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, nodeId, title, createdAt, updatedAt]
    },
    uniqueCount: {
      sql: `issue_id`,
      type: "countDistinct"
    },

    number: {
      sql: `number`,
      type: `sum`
    }
  },

  dimensions: {
    userUrl: {
      sql: `user_url`,
      type: `string`
    },

    pullRequest: {
      sql: `pull_request`,
      type: `string`
    },

    assignee: {
      sql: `assignee`,
      type: `string`
    },

    labelsUrl: {
      sql: `labels_url`,
      type: `string`
    },

    state: {
      sql: `state`,
      type: `string`
    },

    authorAssociation: {
      sql: `author_association`,
      type: `string`
    },

    repositoryUrl: {
      sql: `repository_url`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    userLogin: {
      sql: `user_login`,
      type: `string`
    },

    commentsUrl: {
      sql: `comments_url`,
      type: `string`
    },

    eventsUrl: {
      sql: `events_url`,
      type: `string`
    },

    nodeId: {
      sql: `node_id`,
      type: `string`
    },

    language: {
      sql: `language`,
      type: `string`
    },

    htmlUrl: {
      sql: `html_url`,
      type: `string`
    },

    activeLockReason: {
      sql: `active_lock_reason`,
      type: `string`
    },

    assignees: {
      sql: `assignees`,
      type: `string`
    },

    title: {
      sql: `title`,
      type: `string`
    },

    body: {
      sql: `body`,
      type: `string`
    },

    draft: {
      sql: `draft`,
      type: `string`
    },

    locked: {
      sql: `locked`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    updatedAt: {
      sql: `updated_at`,
      type: `time`
    },

    closedAt: {
      sql: `closed_at`,
      type: `time`
    }
  }
});
