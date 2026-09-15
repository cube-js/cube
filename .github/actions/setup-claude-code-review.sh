#!/usr/bin/env bash
#
# Configures three narrowly-scoped `gh` aliases used by the Claude PR review workflow
# to list and resolve review threads without granting `gh api graphql:*` broadly.
#
# Aliases installed:
#   gh list-review-threads <owner> <repo> <pr> [cursor]
#   gh show-review-thread <thread-id>
#   gh resolve-thread <thread-id>

set -euo pipefail

# Paged at 50 because the bodies dominate the payload — on a PR with a few review
# rounds behind it the full list outweighs the diff. Callers page in a subagent so
# that weight never lands in the review's own context.
# `after: null` starts from the beginning; pass pageInfo.endCursor for the next page.
gh alias set --clobber --shell list-review-threads "$(cat <<'EOF'
gh api graphql \
  -f query='
    query($owner: String!, $repo: String!, $pr: Int!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr) {
          reviewThreads(first: 50, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              isResolved
              isOutdated
              path
              line
              originalLine
              comments(first: 1) {
                totalCount
                nodes { author { login } body }
              }
            }
          }
        }
      }
    }
  ' \
  -F owner="$1" -F repo="$2" -F pr="$3" -F cursor="${4:-null}"
EOF
)"

gh alias set --clobber --shell show-review-thread "$(cat <<'EOF'
gh api graphql \
  -f query='
    query($id: ID!) {
      node(id: $id) {
        ... on PullRequestReviewThread {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 20) {
            nodes { author { login } body }
          }
        }
      }
    }
  ' \
  -F id="$1"
EOF
)"

gh alias set --clobber --shell resolve-thread "$(cat <<'EOF'
gh api graphql \
  -f query='
    mutation($id: ID!) {
      resolveReviewThread(input: { threadId: $id }) {
        thread { isResolved }
      }
    }
  ' \
  -F id="$1"
EOF
)"
