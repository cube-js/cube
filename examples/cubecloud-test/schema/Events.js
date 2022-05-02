cube(`Events`, {
    sql: `SELECT * FROM github.events`,
    
    preAggregations: {
      // Pre-Aggregations definitions go here
      // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
    },
    
    joins: {
      
    },
    
    measures: {
      count: {
        type: `count`,
        drillMembers: [repositoryCreatedAt, repositoryName, actorAttributesGravatarId, actorAttributesName, createdAt, payloadPullRequestHeadRepoName, payloadPullRequestHeadRepoCreatedAt, payloadPullRequestHeadRepoUpdatedAt, payloadPullRequestHeadRepoOwnerGravatarId, payloadPullRequestHeadUserGravatarId, payloadPullRequestMergedByGravatarId, payloadPullRequestCreatedAt, payloadPullRequestTitle, payloadPullRequestUpdatedAt, payloadPullRequestUserGravatarId, payloadPullRequestBaseRepoName, payloadPullRequestBaseRepoCreatedAt, payloadPullRequestBaseRepoUpdatedAt, payloadPullRequestBaseRepoOwnerGravatarId, payloadPullRequestBaseUserGravatarId, payloadName, payloadMemberGravatarId, payloadTargetGravatarId, payloadCommentCommitId, payloadCommentUpdatedAt, payloadCommentCreatedAt, payloadCommentUserGravatarId, payloadCommentOriginalCommitId, payloadCommitId, payloadCommitName, payloadPageTitle, payloadPagePageName]
      },
      
      payloadNumber: {
        sql: `payload_number`,
        type: `sum`
      },
      
      payloadPullRequestNumber: {
        sql: `payload_pull_request_number`,
        type: `sum`
      }
    },
    
    dimensions: {
      repositoryUrl: {
        sql: `repository_url`,
        type: `string`
      },
      
      repositoryHasDownloads: {
        sql: `repository_has_downloads`,
        type: `string`
      },
      
      repositoryCreatedAt: {
        sql: `repository_created_at`,
        type: `string`
      },
      
      repositoryHasIssues: {
        sql: `repository_has_issues`,
        type: `string`
      },
      
      repositoryDescription: {
        sql: `repository_description`,
        type: `string`
      },
      
      repositoryFork: {
        sql: `repository_fork`,
        type: `string`
      },
      
      repositoryHasWiki: {
        sql: `repository_has_wiki`,
        type: `string`
      },
      
      repositoryHomepage: {
        sql: `repository_homepage`,
        type: `string`
      },
      
      repositoryPrivate: {
        sql: `repository_private`,
        type: `string`
      },
      
      repositoryName: {
        sql: `repository_name`,
        type: `string`
      },
      
      repositoryOwner: {
        sql: `repository_owner`,
        type: `string`
      },
      
      repositoryPushedAt: {
        sql: `repository_pushed_at`,
        type: `string`
      },
      
      repositoryLanguage: {
        sql: `repository_language`,
        type: `string`
      },
      
      repositoryOrganization: {
        sql: `repository_organization`,
        type: `string`
      },
      
      repositoryIntegrateBranch: {
        sql: `repository_integrate_branch`,
        type: `string`
      },
      
      repositoryMasterBranch: {
        sql: `repository_master_branch`,
        type: `string`
      },
      
      actorAttributesGravatarId: {
        sql: `actor_attributes_gravatar_id`,
        type: `string`
      },
      
      actorAttributesType: {
        sql: `actor_attributes_type`,
        type: `string`
      },
      
      actorAttributesLogin: {
        sql: `actor_attributes_login`,
        type: `string`
      },
      
      actorAttributesName: {
        sql: `actor_attributes_name`,
        type: `string`
      },
      
      actorAttributesCompany: {
        sql: `actor_attributes_company`,
        type: `string`
      },
      
      actorAttributesLocation: {
        sql: `actor_attributes_location`,
        type: `string`
      },
      
      actorAttributesBlog: {
        sql: `actor_attributes_blog`,
        type: `string`
      },
      
      actorAttributesEmail: {
        sql: `actor_attributes_email`,
        type: `string`
      },
      
      createdAt: {
        sql: `created_at`,
        type: `string`
      },
      
      public: {
        sql: `public`,
        type: `string`
      },
      
      actor: {
        sql: `actor`,
        type: `string`
      },
      
      payloadHead: {
        sql: `payload_head`,
        type: `string`
      },
      
      payloadRef: {
        sql: `payload_ref`,
        type: `string`
      },
      
      payloadMasterBranch: {
        sql: `payload_master_branch`,
        type: `string`
      },
      
      payloadRefType: {
        sql: `payload_ref_type`,
        type: `string`
      },
      
      payloadDescription: {
        sql: `payload_description`,
        type: `string`
      },
      
      payloadPullRequestHeadLabel: {
        sql: `payload_pull_request_head_label`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoName: {
        sql: `payload_pull_request_head_repo_name`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoMasterBranch: {
        sql: `payload_pull_request_head_repo_master_branch`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoCreatedAt: {
        sql: `payload_pull_request_head_repo_created_at`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoHasWiki: {
        sql: `payload_pull_request_head_repo_has_wiki`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoCloneUrl: {
        sql: `payload_pull_request_head_repo_clone_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoPrivate: {
        sql: `payload_pull_request_head_repo_private`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoUpdatedAt: {
        sql: `payload_pull_request_head_repo_updated_at`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoLanguage: {
        sql: `payload_pull_request_head_repo_language`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoSshUrl: {
        sql: `payload_pull_request_head_repo_ssh_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoFork: {
        sql: `payload_pull_request_head_repo_fork`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoUrl: {
        sql: `payload_pull_request_head_repo_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoGitUrl: {
        sql: `payload_pull_request_head_repo_git_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoPushedAt: {
        sql: `payload_pull_request_head_repo_pushed_at`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoSvnUrl: {
        sql: `payload_pull_request_head_repo_svn_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoMirrorUrl: {
        sql: `payload_pull_request_head_repo_mirror_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoHasDownloads: {
        sql: `payload_pull_request_head_repo_has_downloads`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoHomepage: {
        sql: `payload_pull_request_head_repo_homepage`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoHasIssues: {
        sql: `payload_pull_request_head_repo_has_issues`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoDescription: {
        sql: `payload_pull_request_head_repo_description`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoHtmlUrl: {
        sql: `payload_pull_request_head_repo_html_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoOwnerAvatarUrl: {
        sql: `payload_pull_request_head_repo_owner_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoOwnerGravatarId: {
        sql: `payload_pull_request_head_repo_owner_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoOwnerUrl: {
        sql: `payload_pull_request_head_repo_owner_url`,
        type: `string`
      },
      
      payloadPullRequestHeadRepoOwnerLogin: {
        sql: `payload_pull_request_head_repo_owner_login`,
        type: `string`
      },
      
      payloadPullRequestHeadSha: {
        sql: `payload_pull_request_head_sha`,
        type: `string`
      },
      
      payloadPullRequestHeadRef: {
        sql: `payload_pull_request_head_ref`,
        type: `string`
      },
      
      payloadPullRequestHeadUserAvatarUrl: {
        sql: `payload_pull_request_head_user_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestHeadUserGravatarId: {
        sql: `payload_pull_request_head_user_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestHeadUserUrl: {
        sql: `payload_pull_request_head_user_url`,
        type: `string`
      },
      
      payloadPullRequestHeadUserLogin: {
        sql: `payload_pull_request_head_user_login`,
        type: `string`
      },
      
      payloadPullRequestIssueUrl: {
        sql: `payload_pull_request_issue_url`,
        type: `string`
      },
      
      payloadPullRequestMergedBy: {
        sql: `payload_pull_request_merged_by`,
        type: `string`
      },
      
      payloadPullRequestMergedByGravatarId: {
        sql: `payload_pull_request_merged_by_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestMergedByAvatarUrl: {
        sql: `payload_pull_request_merged_by_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestMergedByUrl: {
        sql: `payload_pull_request_merged_by_url`,
        type: `string`
      },
      
      payloadPullRequestMergedByLogin: {
        sql: `payload_pull_request_merged_by_login`,
        type: `string`
      },
      
      payloadPullRequestCreatedAt: {
        sql: `payload_pull_request_created_at`,
        type: `string`
      },
      
      payloadPullRequestMerged: {
        sql: `payload_pull_request_merged`,
        type: `string`
      },
      
      payloadPullRequestBody: {
        sql: `payload_pull_request_body`,
        type: `string`
      },
      
      payloadPullRequestTitle: {
        sql: `payload_pull_request_title`,
        type: `string`
      },
      
      payloadPullRequestDiffUrl: {
        sql: `payload_pull_request_diff_url`,
        type: `string`
      },
      
      payloadPullRequestUpdatedAt: {
        sql: `payload_pull_request_updated_at`,
        type: `string`
      },
      
      payloadPullRequestLinksHtmlHref: {
        sql: `payload_pull_request__links_html_href`,
        type: `string`,
        title: `Payload Pull Request  Links Html Href`
      },
      
      payloadPullRequestLinksSelfHref: {
        sql: `payload_pull_request__links_self_href`,
        type: `string`,
        title: `Payload Pull Request  Links Self Href`
      },
      
      payloadPullRequestLinksCommentsHref: {
        sql: `payload_pull_request__links_comments_href`,
        type: `string`,
        title: `Payload Pull Request  Links Comments Href`
      },
      
      payloadPullRequestLinksReviewCommentsHref: {
        sql: `payload_pull_request__links_review_comments_href`,
        type: `string`,
        title: `Payload Pull Request  Links Review Comments Href`
      },
      
      payloadPullRequestUrl: {
        sql: `payload_pull_request_url`,
        type: `string`
      },
      
      payloadPullRequestPatchUrl: {
        sql: `payload_pull_request_patch_url`,
        type: `string`
      },
      
      payloadPullRequestMergeable: {
        sql: `payload_pull_request_mergeable`,
        type: `string`
      },
      
      payloadPullRequestMergedAt: {
        sql: `payload_pull_request_merged_at`,
        type: `string`
      },
      
      payloadPullRequestClosedAt: {
        sql: `payload_pull_request_closed_at`,
        type: `string`
      },
      
      payloadPullRequestUserAvatarUrl: {
        sql: `payload_pull_request_user_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestUserGravatarId: {
        sql: `payload_pull_request_user_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestUserUrl: {
        sql: `payload_pull_request_user_url`,
        type: `string`
      },
      
      payloadPullRequestUserLogin: {
        sql: `payload_pull_request_user_login`,
        type: `string`
      },
      
      payloadPullRequestHtmlUrl: {
        sql: `payload_pull_request_html_url`,
        type: `string`
      },
      
      payloadPullRequestState: {
        sql: `payload_pull_request_state`,
        type: `string`
      },
      
      payloadPullRequestBaseLabel: {
        sql: `payload_pull_request_base_label`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoName: {
        sql: `payload_pull_request_base_repo_name`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoMasterBranch: {
        sql: `payload_pull_request_base_repo_master_branch`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoCreatedAt: {
        sql: `payload_pull_request_base_repo_created_at`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoHasWiki: {
        sql: `payload_pull_request_base_repo_has_wiki`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoCloneUrl: {
        sql: `payload_pull_request_base_repo_clone_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoPrivate: {
        sql: `payload_pull_request_base_repo_private`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoUpdatedAt: {
        sql: `payload_pull_request_base_repo_updated_at`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoLanguage: {
        sql: `payload_pull_request_base_repo_language`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoSshUrl: {
        sql: `payload_pull_request_base_repo_ssh_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoFork: {
        sql: `payload_pull_request_base_repo_fork`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoUrl: {
        sql: `payload_pull_request_base_repo_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoGitUrl: {
        sql: `payload_pull_request_base_repo_git_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoPushedAt: {
        sql: `payload_pull_request_base_repo_pushed_at`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoSvnUrl: {
        sql: `payload_pull_request_base_repo_svn_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoMirrorUrl: {
        sql: `payload_pull_request_base_repo_mirror_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoHasDownloads: {
        sql: `payload_pull_request_base_repo_has_downloads`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoHomepage: {
        sql: `payload_pull_request_base_repo_homepage`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoHasIssues: {
        sql: `payload_pull_request_base_repo_has_issues`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoDescription: {
        sql: `payload_pull_request_base_repo_description`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoHtmlUrl: {
        sql: `payload_pull_request_base_repo_html_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoOwnerAvatarUrl: {
        sql: `payload_pull_request_base_repo_owner_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoOwnerGravatarId: {
        sql: `payload_pull_request_base_repo_owner_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoOwnerUrl: {
        sql: `payload_pull_request_base_repo_owner_url`,
        type: `string`
      },
      
      payloadPullRequestBaseRepoOwnerLogin: {
        sql: `payload_pull_request_base_repo_owner_login`,
        type: `string`
      },
      
      payloadPullRequestBaseSha: {
        sql: `payload_pull_request_base_sha`,
        type: `string`
      },
      
      payloadPullRequestBaseRef: {
        sql: `payload_pull_request_base_ref`,
        type: `string`
      },
      
      payloadPullRequestBaseUserAvatarUrl: {
        sql: `payload_pull_request_base_user_avatar_url`,
        type: `string`
      },
      
      payloadPullRequestBaseUserGravatarId: {
        sql: `payload_pull_request_base_user_gravatar_id`,
        type: `string`
      },
      
      payloadPullRequestBaseUserUrl: {
        sql: `payload_pull_request_base_user_url`,
        type: `string`
      },
      
      payloadPullRequestBaseUserLogin: {
        sql: `payload_pull_request_base_user_login`,
        type: `string`
      },
      
      payloadAction: {
        sql: `payload_action`,
        type: `string`
      },
      
      payloadName: {
        sql: `payload_name`,
        type: `string`
      },
      
      payloadUrl: {
        sql: `payload_url`,
        type: `string`
      },
      
      payloadDesc: {
        sql: `payload_desc`,
        type: `string`
      },
      
      payloadMemberAvatarUrl: {
        sql: `payload_member_avatar_url`,
        type: `string`
      },
      
      payloadMemberGravatarId: {
        sql: `payload_member_gravatar_id`,
        type: `string`
      },
      
      payloadMemberUrl: {
        sql: `payload_member_url`,
        type: `string`
      },
      
      payloadMemberLogin: {
        sql: `payload_member_login`,
        type: `string`
      },
      
      payloadCommit: {
        sql: `payload_commit`,
        type: `string`
      },
      
      payloadTargetGravatarId: {
        sql: `payload_target_gravatar_id`,
        type: `string`
      },
      
      payloadTargetLogin: {
        sql: `payload_target_login`,
        type: `string`
      },
      
      payloadCommentCommitId: {
        sql: `payload_comment_commit_id`,
        type: `string`
      },
      
      payloadCommentUpdatedAt: {
        sql: `payload_comment_updated_at`,
        type: `string`
      },
      
      payloadCommentCreatedAt: {
        sql: `payload_comment_created_at`,
        type: `string`
      },
      
      payloadCommentPath: {
        sql: `payload_comment_path`,
        type: `string`
      },
      
      payloadCommentUserAvatarUrl: {
        sql: `payload_comment_user_avatar_url`,
        type: `string`
      },
      
      payloadCommentUserUrl: {
        sql: `payload_comment_user_url`,
        type: `string`
      },
      
      payloadCommentUserLogin: {
        sql: `payload_comment_user_login`,
        type: `string`
      },
      
      payloadCommentUserGravatarId: {
        sql: `payload_comment_user_gravatar_id`,
        type: `string`
      },
      
      payloadCommentUrl: {
        sql: `payload_comment_url`,
        type: `string`
      },
      
      payloadCommentBody: {
        sql: `payload_comment_body`,
        type: `string`
      },
      
      payloadCommentOriginalCommitId: {
        sql: `payload_comment_original_commit_id`,
        type: `string`
      },
      
      payloadAfter: {
        sql: `payload_after`,
        type: `string`
      },
      
      payloadBefore: {
        sql: `payload_before`,
        type: `string`
      },
      
      payloadCommitId: {
        sql: `payload_commit_id`,
        type: `string`
      },
      
      payloadCommitEmail: {
        sql: `payload_commit_email`,
        type: `string`
      },
      
      payloadCommitMsg: {
        sql: `payload_commit_msg`,
        type: `string`
      },
      
      payloadCommitName: {
        sql: `payload_commit_name`,
        type: `string`
      },
      
      payloadCommitFlag: {
        sql: `payload_commit_flag`,
        type: `string`
      },
      
      payloadPageSha: {
        sql: `payload_page_sha`,
        type: `string`
      },
      
      payloadPageTitle: {
        sql: `payload_page_title`,
        type: `string`
      },
      
      payloadPageAction: {
        sql: `payload_page_action`,
        type: `string`
      },
      
      payloadPagePageName: {
        sql: `payload_page_page_name`,
        type: `string`
      },
      
      payloadPageSummary: {
        sql: `payload_page_summary`,
        type: `string`
      },
      
      payloadPageHtmlUrl: {
        sql: `payload_page_html_url`,
        type: `string`
      },
      
      url: {
        sql: `url`,
        type: `string`
      },
      
      type: {
        sql: `type`,
        type: `string`
      }
    }
  });
  