---
description: Comprehensive PR review — inline comments, stale-thread resolution, collapsed tracking comment
argument-hint: "[<pr-number>]"
---

**Target:** the `cube-js/cube` PR whose number was passed as an argument or
given in a `PR NUMBER:` line above. Without either, resolve the PR for the
current branch with `gh pr view`.

If the PR references an issue, read it with `gh issue view` — the reported
symptom is what the fix has to actually cover. Read the changed files
themselves, not just the diff — a hunk that looks fine in isolation often is
not. When a finding hinges on runtime behaviour and `docker` / `psql` are
available, reproduce it instead of guessing. `gh issue list` / `gh search
issues` are available when the change looks related to other open reports.

Perform a comprehensive code review with the following focus areas:

1. **Code Quality**
   - Clean code principles and best practices
   - Proper error handling and edge cases
   - Code readability and maintainability

2. **Security**
   - Check for potential security vulnerabilities
   - Validate input sanitization
   - Review authentication/authorization logic

3. **Performance**
   - Identify potential performance bottlenecks
   - Review database queries for efficiency
   - Check for memory leaks or resource issues

4. **Testing**
   - Verify adequate test coverage
   - Review test quality and edge cases
   - Check for missing test scenarios

5. **User-facing docs**
   - A PR that adds or changes something users can notice — a configuration
     option or `CUBEJS_*` environment variable, a data model property, an API
     endpoint, parameter or response field, a SQL API function, a CLI flag, a
     driver option, or a change to default behaviour — must document each such
     change in `docs-mintlify/` in the same PR
   - When it doesn't, read the PR conversation (`gh pr view <N> --comments`). If
     the PR author left a comment explaining why the user-facing changes now in
     the diff need no docs, the requirement is met for those changes: don't raise
     it, and note it in the tracking comment. A change the comment doesn't cover
     still needs docs or its own comment
   - Otherwise post a **high** finding naming each undocumented change and the
     docs page it belongs on, anchored on the diff line that introduces the first
     user-facing change, documented or not. Say in it that adding the docs or a
     top-level PR comment from the author on why none are needed addresses it,
     and that a reply on the finding's thread doesn't. (Resolved threads drop out
     of later rounds, so a thread reply would be lost.)
   - Once the requirement is met (author comment or docs added), tell the
     review-threads subagent that earlier docs threads are addressed, so it
     resolves them — it can't see either from the code at the thread's site
   - Skip changes users cannot notice: internal refactors, tests, CI, tooling,
     and bug fixes that bring behaviour in line with what the docs already say

6. **Comments**
   - An explanatory comment is **3 lines max**. Flag a longer one and ask for the
     load-bearing sentence, unless the reason genuinely cannot be stated shorter
   - A comment earns its place only when its absence would let a later edit
     reintroduce a bug. Flag one a reader would lose nothing by deleting: a
     restatement of the code under it, a banner over self-describing code,
     narration of the change or of the review round that produced it, or a JSDoc
     block whose tags only re-spell already-typed names
   - Prefer making the code carry the meaning — a named constant, a named
     intermediate value, an extracted function whose name states the intent
   - Never raise this to ask for a comment to be **added**, and skip generated
     files and comments another rule or tool requires

Provide detailed feedback using inline comments for specific issues.
Use top-level comments for general observations or praise.

Write every comment in the Cube output style (`.claude/output-styles/cube.md`,
selected for the CI review run): open with the defect itself, no preamble and no
recap of what the diff does, and cite code as `path/to/file.ts:123`. A finding
keeps whatever length it needs to be actionable — the concrete failing input,
the full error output, the reason a security issue is exploitable.

**Tracking comment formatting:**

Applies only to the FINAL update of the tracking comment (the one that contains
the completed review). While work is still in progress — todos partially checked,
"working…" spinner, intermediate status updates — keep everything visible so the
reader can watch progress without expanding anything.

Subagents run in the foreground, so the comment cannot change while one is
working. Update it before every `Task` launch, naming the step now in progress,
and again as each todo completes. Launch independent subagents in a single
message, so they run in parallel rather than one after another.

On the final update, hide EVERYTHING — including the todo checklist itself —
inside a single collapsed `<details>` block. The completed checklist is just
stale progress signal at that point; it belongs behind the spoiler alongside the
rest of the review. Only a short one-line headline with the verdict and issue
counts (e.g. "1 high, 2 medium, 5 low") stays visible outside the spoiler — the
reader expands when they want details.

Leave a blank line after `<summary>` and before `</details>`, otherwise GitHub
won't render the markdown inside (tables and lists especially).

Every round, including one with nothing to post inline, resolve your own stale
review threads and check new findings against existing ones, following
@.claude/docs/review-threads.md

State the outcome inside the tracking comment's `<details>` — how many of your
own threads the listing returned, and of those how many you resolved and how
many you re-affirmed ("no prior threads" when the count is 0). A review silent
on threads reads as one that skipped the step.
