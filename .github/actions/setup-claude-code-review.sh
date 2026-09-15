#!/usr/bin/env bash
#
# Configures three narrowly-scoped `gh` aliases used by the Claude PR review workflow
# to list and resolve review threads without granting `gh api graphql:*` broadly.
#
# Aliases installed:
#   gh list-review-threads <owner> <repo> <pr> [thread-cursor]
#   gh show-review-thread <thread-id> [comment-cursor]
#   gh resolve-thread <thread-id>

set -euo pipefail

gh alias set --clobber --shell list-review-threads "$(cat <<'EOF'
gh api graphql \
  -f query='
    query($owner: String!, $repo: String!, $pr: Int!, $cursor: String) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr) {
          reviewThreads(first: 100, after: $cursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              isResolved
              isOutdated
              path
              line
              originalLine
              comments(first: 25) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes { author { login } body }
              }
            }
          }
        }
      }
    }
  ' \
  -F owner="$1" -F repo="$2" -F pr="$3" -F cursor="${4:-null}" \
  --jq '.data.repository.pullRequest.reviewThreads
        | {pageInfo, nodes: [.nodes[] | select(.isResolved | not) | del(.isResolved)]}'
EOF
)"

# Only needed past the 25 comments the listing already inlines.
gh alias set --clobber --shell show-review-thread "$(cat <<'EOF'
gh api graphql \
  -f query='
    query($id: ID!, $cursor: String) {
      node(id: $id) {
        ... on PullRequestReviewThread {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 25, after: $cursor) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes { author { login } body }
          }
        }
      }
    }
  ' \
  -F id="$1" -F cursor="${2:-null}" \
  --jq '.data.node'
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
