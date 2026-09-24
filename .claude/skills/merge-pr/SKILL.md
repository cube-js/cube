---
name: merge-pr
description: Squash-merge a cube-js/cube pull request with a clean commit message body. Use whenever you are about to merge a PR — `gh pr merge`, "merge it", "ship it", "land this PR" — so the squash commit body gets a written summary instead of the raw PR description.
argument-hint: "[<pr-number>]"
---

# Merge a PR

Master only allows squash merges, and the repo default squash message is the
PR title plus the PR description. Without `--body`, the raw description —
template checklist, HTML comments, review chatter — lands in master history
forever. Always pass `--body`.

Do not pass `--subject`: the commit title is the PR title. If the PR title
itself is wrong, ask the author to fix it on the PR instead of overriding it
at merge time.

## 1. Check the PR is mergeable

```bash
gh pr view <n> --json number,title,body,state,isDraft,mergeable,commits,headRefName
```

Stop and report back if the PR is a draft or has conflicts. Do not use
`--auto`: repo auto-merge is off, so `gh pr merge --auto` merges immediately
(exit 0, no output) instead of waiting for CI. Do not use `--admin` unless
the user explicitly asked for it.

## 2. Write the body

Describe the final state of the change, not how the branch got there:

- 1–15 lines (not counting benchmark tables and CVE lists below): what
  changed and why. Take it from the PR description, not from
  the commit list. Omit the PR template checklist and HTML comments.
- Benchmark / measurement results from the PR are welcome — keep them, as a
  markdown table (`| case | before | after |`) with the setup in one line.
- Fixed CVEs / security advisories (typical for dependency bumps) are welcome
  — list them one per line with the affected package, e.g.
  `CVE-2025-12345 (qs)`, `GHSA-xxxx-xxxx-xxxx (undici)`.
- Linked issues / tickets: `Fixes #123`, `CUB-1234`.
- `BREAKING CHANGE: ...` footer if applicable.
- Trailers last: one `Co-authored-by:` per distinct co-author across the PR's
  commits (deduplicated, human authors other than the PR author, plus the AI
  attribution line if any commit carried one).

An empty body is fine for a trivial PR whose title says it all — pass
`--body ""` explicitly, never omit the flag. Never paste the per-commit
messages.

## 3. Merge

```bash
gh pr merge <n> --squash --body "$(cat <<'EOF'
The __MAX_SOURCE_ROW_LIMIT sentinel was parsed as usize and silently
dropped, so queries ran without any LIMIT.

Fixes #12300

Co-authored-by: Jane Doe <jane@example.com>
EOF
)"
```

Then confirm with `gh pr view <n> --json mergeCommit --jq .mergeCommit.oid`
and `git fetch && git show -s --format=%B <oid>` (not `git log -1
origin/master` — another merge may have landed since), and report the merged
commit to the user.
