---
description: Comprehensive PR review — inline comments, stale-thread resolution, collapsed tracking comment
argument-hint: "[<pr-number>]"
---

**Target:** the `cube-js/cube` PR whose number was passed as an argument.
Without an argument, resolve the PR for the current branch with `gh pr view`.

If the PR references an issue, read it with `gh issue view` — the reported
symptom is what the fix has to actually cover. `gh issue list` / `gh search
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

5. **Documentation**
   - Ensure code is properly documented
   - Verify README updates for new features
   - Check API documentation accuracy

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

**Tracking comment formatting:**

Applies only to the FINAL update of the tracking comment (the one that contains
the completed review). While work is still in progress — todos partially checked,
"working…" spinner, intermediate status updates — keep everything visible so the
reader can watch progress without expanding anything.

On the final update, hide EVERYTHING — including the todo checklist itself —
inside a single collapsed `<details>` block. The completed checklist is just
stale progress signal at that point; it belongs behind the spoiler alongside the
rest of the review. Only a short one-line headline with the verdict and issue
counts (e.g. "1 high, 2 medium, 5 low") stays visible outside the spoiler — the
reader expands when they want details.

Leave a blank line after `<summary>` and before `</details>`, otherwise GitHub
won't render the markdown inside (tables and lists especially).

For review-thread hygiene — resolving your own stale threads and not
re-posting an inline comment you already have — follow @.claude/docs/review-threads.md
