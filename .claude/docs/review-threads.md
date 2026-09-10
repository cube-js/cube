# Review threads

Rules for handling your own prior review threads on a PR. Referenced by
`.claude/commands/cube-review.md`.

**Resolving your own stale review threads:**

Before posting new comments, list existing review threads on this PR using the
preconfigured alias:

  gh list-review-threads cube-js cube <pr-number>

For each thread where ALL of the following hold:
  - `isResolved` is false
  - the first comment's `author.login` is yours — `claude` in CI, otherwise
    the login `gh api user -q .login` returns
  - the concern is no longer applicable in the current diff (file/line gone,
    code rewritten, issue addressed)

resolve it with:

  gh resolve-thread <thread-id>

Do not resolve threads from human reviewers under any circumstance, even if the
concern looks addressed — leave that decision to the reviewer. That includes the
human whose login you are running under: resolve only threads you recognise as
output of an earlier review round. Only the two aliases above are available;
raw `gh api graphql` is not permitted.

**Avoiding duplicate inline comments:**

Use the same `gh list-review-threads` output (from the resolve step above) to
deduplicate against your own prior comments. Before calling
`mcp__github_inline_comment__create_inline_comment` for a new issue, check whether
an existing thread already covers it. Skip creating a new inline comment when ALL
of the following hold for any thread in the list:
  - `isResolved` is false
  - the first comment's `author.login` is yours — `claude` in CI, otherwise
    the login `gh api user -q .login` returns
  - the thread is on the same `path` and `line` as the new issue you would post
  - the existing comment's body raises substantively the same concern (same root
    cause, same fix direction — wording does not need to match)

When you skip, briefly note it in your top-level summary (e.g. "Re-affirmed N
prior threads still apply") so the reader knows you considered the issue. Do not
post a "still applies" reply on the thread — silence is fine; the unresolved
state already communicates that.

If the issue is on a different line or the fix direction has shifted (different
root cause), post a new inline comment as usual.
