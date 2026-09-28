#!/usr/bin/env bash
#
# Configures two narrowly-scoped `gh` aliases used by the Claude PR review workflow
# to list and resolve review threads without granting `gh api graphql:*` broadly.
#
# Aliases installed:
#   gh list-review-threads <owner> <repo> <pr>
#   gh resolve-thread <thread-id>

set -euo pipefail

gh alias set --shell list-review-threads "$(cat <<'EOF'
gh api graphql \
  -f query='
    query($owner: String!, $repo: String!, $pr: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr) {
          reviewThreads(first: 100) {
            nodes {
              id
              isResolved
              isOutdated
              comments(first: 1) {
                nodes { author { login } body path line }
              }
            }
          }
        }
      }
    }
  ' \
  -F owner="$1" -F repo="$2" -F pr="$3"
EOF
)"

gh alias set --shell resolve-thread "$(cat <<'EOF'
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
