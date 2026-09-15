# Review threads

Rules for handling your own prior review threads on a PR. Referenced by
`.claude/commands/cube-review.md`.

**Run this in a subagent.**

Judging a prior thread means reading what it says, and on a PR with a few review
rounds behind it those bodies outweigh the diff under review. Reading them in
your own context costs you the room you need for the review itself, and none of
it is worth keeping afterwards — what you need back is a verdict per thread, not
the threads.

So do the whole of this file in one `Task` subagent, launched once you have your
findings and before you post any of them. Hand it the findings; it pages the
threads, resolves your stale ones, and returns the verdicts. Give it:

- the repo and PR number, and your login — `claude` in CI
- each finding you are about to post, as `path:line` plus a sentence of the
  concern (the root cause, enough that a duplicate is recognisable)
- this file's rules, and a request for exactly two things back: the thread ids
  it resolved, and per finding POST or SKIP with the thread id behind a SKIP

Three aliases are available to it; raw `gh api graphql` is not permitted:

  gh list-review-threads <owner> <repo> <pr> [cursor]
  gh show-review-thread <thread-id>
  gh resolve-thread <thread-id>

**Paging the threads:**

  gh list-review-threads cube-js cube <pr-number>

Returns 50 threads a page — `id`, `isResolved`, `isOutdated`, `path`, `line`,
`originalLine`, and the first comment's author, body and count. When
`pageInfo.hasNextPage` is true, pass `pageInfo.endCursor` as a fourth argument
for the next page:

  gh list-review-threads cube-js cube <pr-number> <endCursor>

Page to the end before deciding anything — a thread that duplicates a finding is
as likely to be on the last page as the first.

`comments.totalCount` above 1 means someone replied and you are seeing only the
opening comment. Read the rest before judging that thread:

  gh show-review-thread <thread-id>

GitHub nulls `line` once a thread goes outdated; the `originalLine` it keeps is
the position before the edits that outdated it, so it is a tiebreaker between
threads on one path and never a current-diff line.

**Resolving your own stale review threads:**

For each thread where ALL of the following hold:
  - `isResolved` is false
  - the first comment's `author.login` is yours
  - the concern is no longer applicable in the current diff (file/line gone,
    code rewritten, issue addressed)

resolve it with:

  gh resolve-thread <thread-id>

`isOutdated` true means only that the surrounding code moved — on a file this
round touches it is not evidence the concern was addressed, so read the code at
the site before resolving. Where a human replied, do not resolve if they are
pushing back.

Do not resolve threads from human reviewers under any circumstance, even if the
concern looks addressed — leave that decision to the reviewer. That includes the
human whose login you are running under: resolve only threads you recognise as
output of an earlier review round.

**Avoiding duplicate inline comments:**

Skip a finding when ALL of the following hold for any thread in the list:
  - `isResolved` is false
  - the first comment's `author.login` is yours
  - the thread is on the same `path` as the finding, and on the same `line` when
    `line` is non-null — an outdated thread has no line to test, so it is judged
    on its body alone
  - the body raises substantively the same concern (same root cause, same fix
    direction — wording does not need to match)

Report the skip so the review can note it in its top-level summary (e.g.
"Re-affirmed N prior threads still apply"). Do not post a "still applies" reply
on the thread — silence is fine; the unresolved state already communicates that.

If the finding is on a different line or the fix direction has shifted
(different root cause), it is not a duplicate.
