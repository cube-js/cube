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

The tracking comment is not one of these threads — it is a top-level comment, so
it never appears in the list below, and fetching the top-level comments to find it
will not help the subagent: it restates a whole past review round, several
kilobytes of it, and none of that says whether a thread is still live. The code at
the site and the thread's own replies say that.

Four aliases are available to it; raw `gh api graphql` is not permitted:

  gh list-review-threads <owner> <repo> <pr> [thread-cursor]
  gh show-review-thread <thread-id> [comment-cursor]
  gh reply-to-thread <thread-id> <body>
  gh resolve-thread <thread-id>

`reply-to-thread` is the only way to write into a thread that already exists —
the inline-comment tool opens a new one every time, which on a thread you are
answering is a second unresolved entry rather than a reply.

**Paging the threads:**

  gh list-review-threads cube-js cube <pr-number>

Returns the **unresolved** threads out of each page of 100 — `id`, `isOutdated`,
`path`, `line`, `originalLine`, and the comment chain: every comment's author and
body, up to 25, plus `comments.totalCount` and `comments.pageInfo`.

Resolved threads are dropped before you see them, so every rule below is already
satisfied on that count and nothing in this list is settled business. When the
top-level `pageInfo.hasNextPage` is true — the one beside `nodes`, not a thread's
own `comments.pageInfo` — pass its `endCursor` as a fourth argument for the next
page:

  gh list-review-threads cube-js cube <pr-number> <endCursor>

Page to the end before deciding anything — a thread that duplicates a finding is
as likely to be on the last page as the first, and because the filter is applied
after the page is cut, a page can come back with two nodes or none and still have
a next one.

You therefore judge a thread from the whole conversation, not its opening line —
a reply is often where the concern was answered or pushed back on. You are missing
part of it only where `comments.pageInfo.hasNextPage` is true, and then the
thread's own `comments.pageInfo.endCursor` picks up from the last comment you have:

  gh show-review-thread <thread-id> <endCursor>

That returns the next 25 with a `comments.pageInfo` of its own, so repeat until
`hasNextPage` is false. Called without a cursor it re-reads the thread from the
first comment, which you already have.

GitHub nulls `line` once a thread goes outdated; the `originalLine` it keeps is
the position before the edits that outdated it, so it is a tiebreaker between
threads on one path and never a current-diff line.

**Resolving your own stale review threads:**

For each thread where BOTH of the following hold:
  - the first comment's `author.login` is yours
  - the concern is no longer live — the current diff addressed it (file/line
    gone, code rewritten, issue fixed), or it was never real and you are
    withdrawing the finding

resolve it with:

  gh resolve-thread <thread-id>

`isOutdated` true means only that the surrounding code moved — on a file this
round touches it is not evidence the concern was addressed, so read the code at
the site before resolving.

Where a human replied, what they are disputing decides. Pushing back on your
verdict — the fix is not in, the concern still stands — keeps the thread open.
Refuting the finding itself, with you agreeing, closes it: a withdrawn finding is
not live, and leaving it open feeds it back to you every round as unresolved.
Resolve it, and say so on the thread with `gh reply-to-thread` — resolution alone
does not distinguish a withdrawn finding from a fixed one, and the next round
reads the thread, not this file.

Do not resolve threads from human reviewers under any circumstance, even if the
concern looks addressed — leave that decision to the reviewer. That includes the
human whose login you are running under: resolve only threads you recognise as
output of an earlier review round.

**Avoiding duplicate inline comments:**

Skip a finding when ALL of the following hold for any thread in the list:
  - the first comment's `author.login` is yours
  - the thread is on the same `path` as the finding, and on the same `line` when
    `line` is non-null — an outdated thread has no line to test, so it is judged
    on its body alone
  - the body raises substantively the same concern (same root cause, same fix
    direction — wording does not need to match)

Report the skip so the review can note it in its top-level summary (e.g.
"Re-affirmed N prior threads still apply"). Do not post a "still applies" reply
on the thread, `reply-to-thread` notwithstanding — the unresolved state already
communicates that, and a reply per round buries the concern under its own echoes.

If the finding is on a different line or the fix direction has shifted
(different root cause), it is not a duplicate.

Resolving a thread and skipping a finding as that thread's duplicate say
opposite things about whether the concern is live, so no thread may end up in
both lists. When one does, the code at the site decides: either the fix is in,
which makes the finding stale — drop it and resolve the thread — or it is not,
and the thread stays open with the finding folded into it.
