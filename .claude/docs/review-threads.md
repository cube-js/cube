# Review threads

Rules for handling your own prior review threads on a PR. Referenced by
`.claude/commands/cube-review.md`.

**Listing threads:**

Before posting new comments, list existing review threads on this PR using the
preconfigured alias:

  gh list-review-threads cube-js cube <pr-number>

This returns an index — `id`, `isResolved`, `isOutdated`, `path`, `line`, the
first comment's author and the thread's comment count — but no comment bodies.
It pages 50 threads at a time; when `pageInfo.hasNextPage` is true, fetch the
next page by passing `pageInfo.endCursor` as a fourth argument:

  gh list-review-threads cube-js cube <pr-number> <endCursor>

Work from the index and pull a body only for a thread you actually have to
decide about — on a long-running PR the bodies together are larger than the
whole diff, and most threads are settled by `path`/`line`/`isResolved` alone:

  gh show-review-thread <thread-id>

Only the three aliases above are available; raw `gh api graphql` is not
permitted.

**Resolving your own stale review threads:**

A thread is a candidate when ALL of the following hold in the index:
  - `isResolved` is false
  - the first comment's `author.login` is yours — `claude` in CI, otherwise
    the login `gh api user -q .login` returns

For each candidate, read the body with `gh show-review-thread` and resolve it
when the concern is no longer applicable in the current diff (file/line gone,
code rewritten, issue addressed):

  gh resolve-thread <thread-id>

`isOutdated` true with the file untouched in this round is enough on its own —
resolve without fetching the body. A later comment on the thread by someone
else (`comments.totalCount` > 1) means a human is still in the conversation:
read it before deciding, and do not resolve if they are pushing back.

Do not resolve threads from human reviewers under any circumstance, even if the
concern looks addressed — leave that decision to the reviewer. That includes the
human whose login you are running under: resolve only threads you recognise as
output of an earlier review round.

**Avoiding duplicate inline comments:**

Use the same index to deduplicate against your own prior comments. Before calling
`mcp__github_inline_comment__create_inline_comment` for a new issue, check whether
an existing thread already covers it. Skip creating a new inline comment when ALL
of the following hold for any thread in the index:
  - `isResolved` is false
  - the first comment's `author.login` is yours — `claude` in CI, otherwise
    the login `gh api user -q .login` returns
  - the thread is on the same `path` and `line` as the new issue you would post
  - the existing comment's body raises substantively the same concern (same root
    cause, same fix direction — wording does not need to match)

The first three are answerable from the index; fetch the body with
`gh show-review-thread` only for the threads that survive them, which is at most
one or two per finding.

When you skip, briefly note it in your top-level summary (e.g. "Re-affirmed N
prior threads still apply") so the reader knows you considered the issue. Do not
post a "still applies" reply on the thread — silence is fine; the unresolved
state already communicates that.

If the issue is on a different line or the fix direction has shifted (different
root cause), post a new inline comment as usual.
