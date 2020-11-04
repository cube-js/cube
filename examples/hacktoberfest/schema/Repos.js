cube(`Repos`, {
  sql: `SELECT * FROM public.repos`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [name, id, fullName, createdAt, updatedAt]
    },
    uniqueCount: {
      sql: `repo_id`,
      type: "countDistinct"
    },

    openIssuesCount: {
      sql: `open_issues_count`,
      type: `sum`
    },

    stargazersCount: {
      sql: `stargazers_count`,
      type: `sum`
    },

    watchersCount: {
      sql: `watchers_count`,
      type: `sum`
    },

    forksCount: {
      sql: `forks_count`,
      type: `sum`
    }
  },

  dimensions: {
    htmlUrl: {
      sql: `html_url`,
      type: `string`
    },

    name: {
      sql: `name`,
      type: `string`
    },

    hasWiki: {
      sql: `has_wiki`,
      type: `string`
    },

    ownerUrl: {
      sql: `owner_url`,
      type: `string`
    },

    hasPages: {
      sql: `has_pages`,
      type: `string`
    },

    ownerLogin: {
      sql: `owner_login`,
      type: `string`
    },

    hasDownloads: {
      sql: `has_downloads`,
      type: `string`
    },

    license: {
      sql: `license`,
      type: `string`
    },

    description: {
      sql: `description`,
      type: `string`
    },

    archived: {
      sql: `archived`,
      type: `string`
    },

    issuesUrl: {
      sql: `issues_url`,
      type: `string`
    },

    hasIssues: {
      sql: `has_issues`,
      type: `string`
    },

    language: {
      sql: `language`,
      type: `string`
    },

    mirrorUrl: {
      sql: `mirror_url`,
      type: `string`
    },

    gitUrl: {
      sql: `git_url`,
      type: `string`
    },

    subscribersUrl: {
      sql: `subscribers_url`,
      type: `string`
    },

    contributorsUrl: {
      sql: `contributors_url`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    commitsUrl: {
      sql: `commits_url`,
      type: `string`
    },

    fullName: {
      sql: `full_name`,
      type: `string`
    },

    pullsUrl: {
      sql: `pulls_url`,
      type: `string`
    },

    disabled: {
      sql: `disabled`,
      type: `string`
    },

    url: {
      sql: `url`,
      type: `string`
    },

    fork: {
      sql: `fork`,
      type: `string`
    },

    hasProjects: {
      sql: `has_projects`,
      type: `string`
    },

    languagesUrl: {
      sql: `languages_url`,
      type: `string`
    },

    private: {
      sql: `private`,
      type: `string`
    },

    defaultBranch: {
      sql: `default_branch`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    updatedAt: {
      sql: `updated_at`,
      type: `time`
    }
  }
});
