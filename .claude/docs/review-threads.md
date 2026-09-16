# Review threads

Rules for handling your own prior review threads on a PR. Referenced by
`.claude/commands/cube-review.md`.

**Run this in a subagent, once per round, on every round.**

Prior thread bodies on a PR with a few rounds behind it outweigh the diff under
review, and none of them are worth keeping — what you need back is a verdict per
thread, not the threads. So do the whole of this file in one `Task` subagent,
launched after you have your findings and before you post any of them. Give it:

- the repo and PR number, and your login — `claude` in CI
- each finding you are about to post, as `path:line` plus a sentence of the
  concern (the root cause, enough that a duplicate is recognisable)
- this file's rules, and a request for exactly two things back: the thread ids
  it resolved, and per finding POST or SKIP with the thread id behind a SKIP

A round with zero findings still launches it. Stale-thread resolution is about
what a *previous* round left behind, and the round that fixes everything is
exactly the one that posts nothing inline — skip the subagent there and those
threads stay open forever. Pass an empty findings list and ask only for the
resolved ids.

The tracking comment is a top-level comment, not a thread: it never appears in
the list below, and reading it tells you nothing about whether a thread is still
live. The code at the site and the thread's own replies do.

Four aliases are available; raw `gh api graphql` is not permitted:

  gh list-review-threads <owner> <repo> <pr> [thread-cursor]
  gh show-review-thread <thread-id> [comment-cursor]
  gh reply-to-thread <thread-id> <body>
  gh resolve-thread <thread-id>

`reply-to-thread` is the only way to write into an existing thread — the
inline-comment tool opens a new one every time, i.e. a second unresolved entry
rather than a reply.

**Paging the threads:**

  gh list-review-threads cube-js cube <pr-number> [endCursor]

Returns the **unresolved** threads out of each page of 100 — `id`, `isOutdated`,
`path`, `line`, `originalLine`, and up to 25 comments (author + body) with
`comments.totalCount` and `comments.pageInfo`. Resolved threads are filtered
out, so nothing in this list is settled business.

Page to the end before deciding anything: the filter is applied after the page
is cut, so a page can come back with two nodes or none and still have a next
one. Follow the top-level `pageInfo.hasNextPage` — the one beside `nodes`, not a
thread's own `comments.pageInfo` — by passing its `endCursor` as the fourth
argument.

Judge a thread from the whole conversation, not its opening line — a reply is
often where the concern was answered or pushed back on. Where a thread's own
`comments.pageInfo.hasNextPage` is true, fetch the rest with

  gh show-review-thread <thread-id> <endCursor>

and repeat until `hasNextPage` is false. Without a cursor it re-reads from the
first comment, which you already have.

GitHub nulls `line` once a thread goes outdated; `originalLine` is the position
before the edits that outdated it — a tiebreaker between threads on one path,
never a current-diff line.

**Resolving your own stale review threads:**

Resolve a thread with `gh resolve-thread <thread-id>` when BOTH hold:
  - the first comment's `author.login` is yours
  - the concern is no longer live — the current diff addressed it (file/line
    gone, code rewritten, issue fixed), or it was never real and you are
    withdrawing the finding

`isOutdated` true means only that the surrounding code moved; on a file this
round touches it is not evidence the concern was addressed. Read the code at the
site before resolving.

Where a human replied, what they dispute decides. Pushing back on your verdict —
the fix is not in, the concern stands — keeps the thread open. Refuting the
finding itself, with you agreeing, closes it: a withdrawn finding is not live,
and leaving it open feeds it back to you every round. Resolve it and say so on
the thread with `gh reply-to-thread` — resolution alone does not distinguish
withdrawn from fixed, and the next round reads the thread, not this file.

Never resolve a thread a human opened, even if the concern looks addressed —
that is the reviewer's call. This includes the human whose login you run under:
resolve only threads you recognise as output of an earlier review round.

**Avoiding duplicate inline comments:**

Skip a finding when ALL hold for some thread in the list:
  - the first comment's `author.login` is yours
  - same `path`, and same `line` when `line` is non-null — an outdated thread
    has no line to test and is judged on its body alone
  - the body raises substantively the same concern (same root cause, same fix
    direction — wording need not match)

A different line or a shifted fix direction (different root cause) is not a
duplicate.

Report the skip so the review can note it in its top-level summary (e.g.
"Re-affirmed N prior threads still apply"). Do not post a "still applies" reply
on the thread — the unresolved state already says that, and a reply per round
buries the concern under its own echoes.

Resolving a thread and skipping a finding as its duplicate say opposite things
about whether the concern is live, so no thread may end up in both lists. When
one does, the code at the site decides: either the fix is in — drop the finding
and resolve the thread — or it is not, and the thread stays open with the
finding folded into it.
