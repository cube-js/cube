use std::io::Read;

use anyhow::{bail, Context as _, Result};
use serde_json::{Map, Value};

use crate::client::Query;

/// Parse a `--data` argument into a JSON object.
///
/// Accepts inline JSON (`'{"name": "x"}'`), `@path/to/file.json`, or `-`
/// to read from stdin — the same convention as `gh api` / `curl -d`.
pub fn parse_data(data: Option<&str>) -> Result<Map<String, Value>> {
    let Some(data) = data else {
        return Ok(Map::new());
    };
    let raw = if data == "-" {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    } else if let Some(path) = data.strip_prefix('@') {
        std::fs::read_to_string(path).with_context(|| format!("failed to read {path}"))?
    } else {
        data.to_string()
    };
    let value: Value = serde_json::from_str(&raw).context("--data is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(map),
        _ => bail!("--data must be a JSON object"),
    }
}

/// Insert a flag value into a JSON body if it was provided on the CLI.
pub fn set<T: serde::Serialize>(body: &mut Map<String, Value>, key: &str, value: &Option<T>) {
    if let Some(v) = value {
        body.insert(key.to_string(), serde_json::to_value(v).unwrap());
    }
}

/// Push a query parameter if the flag was provided.
pub fn push<T: ToString>(query: &mut Query, key: &str, value: &Option<T>) {
    if let Some(v) = value {
        query.push((key.to_string(), v.to_string()));
    }
}

/// How one endpoint implements the deprecated offset paging it still accepts,
/// which is what decides whether the CLI has to read the deprecated `data`
/// field to honor it. Verified per endpoint against the server, not assumed —
/// the two shapes want opposite handling, so there is no safe blanket answer.
#[derive(Clone, Copy)]
pub struct ListPaging {
    /// The command's own deprecated flag names, for the error message.
    flags: &'static str,
    /// Whether `data` alone is sliced. When false the slicing happens in the
    /// query, so `items` already holds exactly the requested page.
    data_only: bool,
}

/// Sliced by the database query: `items` and `data` hold the same rows, so
/// render `items` and stay correct after `data` is eventually removed.
pub const OFFSET_LIMIT_IN_QUERY: ListPaging = ListPaging {
    flags: "--offset/--limit",
    data_only: false,
};

/// As [`OFFSET_LIMIT_IN_QUERY`], for `reports list`, which deprecates a
/// different pair of flags.
pub const LIMIT_PAGE_IN_QUERY: ListPaging = ListPaging {
    flags: "--limit/--page",
    data_only: false,
};

/// Applied in memory to `data` alone, while `items` is the full cursor page
/// regardless of the flags. Only reading `data` honors them.
pub const OFFSET_LIMIT_DATA_ONLY: ListPaging = ListPaging {
    flags: "--offset/--limit",
    data_only: true,
};

/// Which response field a list command should render, given how its endpoint
/// pages and the flags it was invoked with — the two deprecated ones (in the
/// order `paging` names them), then `--first`/`--after`. `Some("data")`
/// selects the deprecated offset-sliced page; `None` the canonical `items`.
///
/// The API deprecated offset paging in favor of the `first`/`after` cursor
/// and the two address different pages of the same response, so an endpoint
/// given both either rejects the request (deployments, reports) or silently
/// honors just one (environments, user attributes). Reject it here instead,
/// so the CLI never prints a page the caller did not ask for.
///
/// Reading `data` is only ever right for a [`ListPaging::data_only`]
/// endpoint. Doing it everywhere "for uniformity" would be actively harmful:
/// on a query-sliced endpoint `items` is already the requested page and stays
/// so after `data` is dropped, whereas asking for `data` would then fail a
/// command the server still honors perfectly well.
pub fn paging_field(
    paging: ListPaging,
    deprecated_a: Option<u64>,
    deprecated_b: Option<u64>,
    first: Option<u64>,
    after: Option<&str>,
) -> Result<Option<&'static str>> {
    let uses_offset = deprecated_a.is_some() || deprecated_b.is_some();
    let uses_cursor = first.is_some() || after.is_some();
    if uses_offset && uses_cursor {
        bail!(
            "{} are deprecated and cannot be combined with --first/--after",
            paging.flags
        );
    }
    Ok((uses_offset && paging.data_only).then_some("data"))
}

/// Normalize a user-supplied Cube Cloud URL: trim whitespace and trailing
/// slashes, and default to `https://` when no scheme is given (reqwest
/// otherwise fails with "relative URL without a base").
pub fn normalize_url(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.is_empty() || url.contains("://") {
        url.to_string()
    } else {
        format!("https://{url}")
    }
}

/// Parse a `KEY=VALUE` pair (for `cube variables set`).
pub fn parse_kv(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(format!("`{s}` is not in KEY=VALUE format")),
    }
}

pub fn body(map: Map<String, Value>) -> Value {
    Value::Object(map)
}

/// The phrase both refusal messages open with, and what
/// `only_the_listed_branch_arguments_refuse_an_empty_value` asserts a refusal was actually
/// about — that test partitions on whether parsing failed at all, and uses this phrase to
/// tell a refusal of the empty value from a placeholder argv that failed for a reason of
/// its own. Written once so the three cannot drift apart.
const EMPTY_VALUE_REFUSED: &str = "an empty value";

/// Reject a supplied-but-empty branch at parse time, where what an empty value means to
/// the server has not been measured. [`nonempty_target`] carries the two arguments where
/// it has — one of them a `--branch`, so the split is measured-vs-not, not branch-vs-ref.
///
/// On the declaration rather than in a match arm: written by hand it was added three
/// times, each round missing an argument — last `github connect --branch`, which sends
/// under the key `branch` and escaped a grep for `branchName`.
///
/// It no longer guards every branch argument;
/// `only_the_listed_branch_arguments_refuse_an_empty_value` holds the set it can see.
/// `create-branch <NAME>` carries this too, under the id `name`, which the filter misses.
///
/// The reachable case is a CI gate whose variable came out empty — `$GITHUB_HEAD_REF` is
/// unset off `pull_request`, and `jq -r` prints nothing for empty input — where
/// `dev-mode ""` would enter dev mode on a branch the caller never named.
pub fn nonempty(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Err(format!(
            "{EMPTY_VALUE_REFUSED} names no branch — and it is not dropped, but sent as an \
             empty field, leaving what that means to the server rather than to you"
        ));
    }

    Ok(s.to_string())
}

/// `nonempty` for the two flags whose empty-value behaviour was measured against a
/// live tenant: `--branch` on `deployments build-status` and `--ref` on `dbt sync`.
///
/// Both are read as "not specified" and fall back to something that already exists —
/// the deployment's active or deploy branch, and the dbt integration's tracked
/// branch. Neither errors: the empty-ref sync ran to COMPLETED with four cubes and
/// exit 0. That is what makes an empty value dangerous rather than untidy in a CI
/// gate, and it is reachable without anything failing, since `$GITHUB_HEAD_REF` is
/// empty on every trigger except `pull_request`.
pub fn nonempty_target(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Err(format!(
            "{EMPTY_VALUE_REFUSED} is read as \"not specified\" and falls back to the branch \
             the server would have picked anyway, so this would run against code you \
             did not name"
        ));
    }

    Ok(s.to_string())
}

/// A branch name to PRINT, when the payload might not have carried one.
///
/// Only for prose and suggested commands, never for a JSON document: a gate reading
/// `.branchName` must see the empty string it was actually given rather than a plausible
/// name it would then try to use.
///
/// Quoting already handles the command half — an empty name renders as a visible `''`.
/// This exists for the half quoting can't reach: the same name is interpolated into the
/// sentence around the command, where `branch  is not building` reads as a formatting bug
/// rather than as a payload that named nothing. These messages are read out of CI logs
/// after the fact, where the message is all the reader gets.
///
/// A name that is merely PADDED is returned as it is, not trimmed. `nonempty` accepts
/// `--branch '  x  '` and sends it on unchanged, so such a branch can exist, and the
/// messages carrying this name also carry a `delete-branch` for it — trimming would print
/// a command naming a different branch than the one that exists, which is the failure
/// every quoting commit on this branch has been closing. `shell_quote` renders it
/// `'  x  '`, so the padding is visible rather than silently eaten; ugly and true beats
/// tidy and wrong.
pub fn branch_or_placeholder(branch: &str) -> String {
    if is_blank(branch) {
        "<branch>".to_string()
    } else {
        branch.to_string()
    }
}

/// Whether a server said nothing in a field a message is about to quote.
///
/// Trim-aware, like `nonempty` above: blanks are the other spelling of "said nothing", and
/// the only one an `is_empty` check still reads as an answer. "Named no branch" and
/// "reported no reason" are the same question, so they get the same predicate rather than
/// one each — the second was written as `is_empty` precisely because the first's rule
/// lived somewhere it couldn't be reused.
///
/// It matters most where the answer picks BETWEEN two sentences rather than filling one
/// in: taking the wrong arm states something false — a fork that didn't happen, a reason
/// that isn't there — where filling one in merely leaves a hole.
pub fn is_blank(s: &str) -> bool {
    s.trim().is_empty()
}

/// How much of a server complaint to keep in a POLL LABEL. Long enough for the known
/// verdicts and a sentence of context, short enough that one poll stays one line.
pub const COMPLAINT_LIMIT: usize = 120;

/// How much to keep when the text IS the message: a terminal failure's reason.
///
/// Generous where [`COMPLAINT_LIMIT`] is tight, and the difference is the point. A label
/// is repeated every poll and is a dedupe key, so it has to be short and stable. A
/// terminal reason is printed once, is the last thing anyone reads, and is the whole
/// purpose of the message — a dbt compile error naming the model and the missing node
/// runs well past 120 characters, and cutting it there would leave a gate's log saying
/// that something failed without saying what.
pub const REASON_LIMIT: usize = 800;

/// Squash arbitrary server text into a single line.
///
/// Collapsing matters on its own, before any truncation: these land in `anyhow` chains
/// that render with `{err:#}`, joining links with ": ", so a multi-line compile error
/// turns one error into several lines with no shape. Truncation keeps the leading text,
/// which is where these verdicts put their meaning.
pub fn one_line(text: &str, max_chars: usize) -> String {
    // Bounded as it goes, rather than collapsed and then cut. The inputs are response
    // bodies, which can be megabytes, and collapsing first would materialise the whole of
    // one twice over — a token per whitespace run plus the joined copy — to keep a few
    // hundred characters. The per-character inner loop is what bounds the other shape,
    // a body with no whitespace in it at all.
    let mut out = String::new();
    let mut len = 0;
    let mut cut = false;
    'words: for word in text.split_whitespace() {
        if !out.is_empty() {
            if len == max_chars {
                cut = true;
                break;
            }
            out.push(' ');
            len += 1;
        }
        for c in word.chars() {
            if len == max_chars {
                cut = true;
                break 'words;
            }
            out.push(c);
            len += 1;
        }
    }
    if cut {
        out.push('…');
    }

    out
}

/// A status field, read for comparison against the states a wait knows — or for the
/// `--json` document, where the comparison is the caller's rather than ours.
///
/// Trimmed for the reason every other comparison on this branch is: padding is a spelling
/// of the same value, not a different one. It matters more here than in a message, though
/// — an untrimmed `"COMPLETED "` matches no known state, and a wait treats what it doesn't
/// know as progress ON PURPOSE, so the sync would be over and the CLI would poll it until
/// the timeout. Trimming can't cause the failure that rule protects against: it recognises
/// a state, it never invents one.
pub fn status_of(res: &serde_json::Value, path: &str) -> String {
    crate::output::field(res, path).trim().to_string()
}

/// Wrap a value so it survives being pasted into a shell.
///
/// The CLI's failure messages hand over commands to run — `delete-branch <branch>`,
/// `dev-mode <branch>` — and they are read out of CI logs, where they get copied without
/// being reread. A branch name is user-supplied and `git check-ref-format` permits `#`,
/// `$`, `&`, `;` and `!`, so `feat#1234` pastes as `feat` plus a comment: a command that
/// looks like it ran and silently addressed the wrong thing.
///
/// Single quotes rather than escaping each metacharacter, because inside them the shell
/// interprets nothing — except a single quote, which is itself legal in a ref name, so it
/// is closed, escaped and reopened the POSIX way.
///
/// The convention is every user-supplied value inside a suggested command, not only the
/// ones whose character set is known to be dangerous. Sync ids are opaque hex in practice
/// and a wrong one 404s loudly rather than acting on the wrong thing — but branch names
/// were "usually fine" too, until a ref called `#1234-fix` turned up, and a rule with an
/// exemption list is one somebody has to re-derive at each new call site.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// Parse a wait duration: a bare number of seconds, or a number with a `s`/`m`/`h`
/// suffix (`90`, `30s`, `15m`, `1h`).
///
/// Written for the `--wait` flags, whose useful range spans seconds (a poll
/// interval) to tens of minutes (a dbt sync). A bare number would have to pick one
/// of those as its unit and silently surprise anyone who meant the other, so the
/// suffix carries it — with seconds as the bare-number default, since that is what
/// every other CLI does.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let raw = s.trim();
    // Case-insensitive: `30S` and `15M` are obviously meant, and rejecting them
    // teaches nothing.
    let (digits, multiplier) = match raw.chars().last().map(|c| c.to_ascii_lowercase()) {
        Some('s') => (&raw[..raw.len() - 1], 1),
        Some('m') => (&raw[..raw.len() - 1], 60),
        Some('h') => (&raw[..raw.len() - 1], 3600),
        _ => (raw, 1),
    };
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{s}` is not a duration (try 30s, 15m, or 1h)"))?;
    if value == 0 {
        return Err(format!("`{s}` must be greater than zero"));
    }
    // Checked: the whole job of this function is to reject input it can't honour,
    // so it must not be the thing that panics (debug) or wraps to a nonsense
    // duration (release) on an absurd number of hours.
    let seconds = value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("`{s}` is longer than this can represent"))?;

    Ok(std::time::Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    #[test]
    fn one_line_collapses_and_truncates() {
        assert_eq!(
            one_line("Branch is not active", 120),
            "Branch is not active"
        );
        assert_eq!(one_line("", 120), "");
        // A multi-line error becomes one progress line, not several.
        assert_eq!(
            one_line("Build failed:\n  cube orders\n  line 3", 120),
            "Build failed: cube orders line 3"
        );
        // Long text is cut, keeping the leading meaning.
        let long = "x".repeat(200);
        let cut = one_line(&long, 10);
        assert_eq!(cut.chars().count(), 11, "10 chars plus the ellipsis");
        assert!(cut.starts_with("xxxxxxxxxx"));
    }

    #[test]
    fn a_terminal_reason_keeps_what_a_poll_label_would_cut() {
        // The two limits exist to differ: a label repeats every poll and is a dedupe key,
        // while a reason is printed once and is why the message exists. A dbt compile
        // error naming the model and the missing node runs past the label's 120.
        let reason = "Compilation Error in model fct_orders\n  Model \
                      'model.x.fct_orders' depends on a node named 'stg_orders' which \
                      was not found\n  > in macro ref (macros/ref.sql)";
        let kept = one_line(reason, REASON_LIMIT);
        assert!(!kept.contains('\n'), "still one line: {kept}");
        assert!(kept.ends_with("(macros/ref.sql)"), "not truncated: {kept}");
        assert!(
            one_line(reason, COMPLAINT_LIMIT).ends_with('…'),
            "the label limit would have cut it, which is why they differ"
        );
    }

    #[test]
    fn one_line_caps_a_body_of_any_size_or_shape() {
        // Both shapes a response body arrives in: many small tokens, and one token with
        // no whitespace to break it up. The second is what the per-character inner loop
        // is for — a word-at-a-time bound would copy all five million characters.
        //
        // What this does NOT assert is the bound on WORK, which is why the loop is
        // written the way it is: there's no allocation hook here, and a wall-clock
        // assertion would be flaky, so the collapse-then-cut version this replaced would
        // pass too — it produces the same 41 characters after allocating about ten
        // megabytes. The size below is chosen so that version is slow enough to notice
        // rather than to fail; the reason it isn't used lives on `one_line` itself.
        let many = "word ".repeat(500_000);
        let one = "x".repeat(5_000_000);
        for input in [many, one] {
            let cut = one_line(&input, 40);
            assert_eq!(cut.chars().count(), 41, "40 chars plus the ellipsis");
            assert!(cut.ends_with('…'));
        }
        // The boundary the `cut` flag turns on, which the rewrite could plausibly get
        // wrong: input ending exactly at the budget was not cut, so it must not say it
        // was — an off-by-one here would put an ellipsis on every full-length message.
        assert_eq!(one_line("abcde", 5), "abcde");
        assert_eq!(one_line("abcdef", 5), "abcde…");
        // And a separator landing exactly on the budget is the other side of it.
        assert_eq!(one_line("ab cd", 2), "ab…");
    }

    #[test]
    fn one_line_counts_characters_not_bytes() {
        // Truncating on bytes would panic mid-character here.
        let cut = one_line(&"é".repeat(50), 10);
        assert_eq!(cut.chars().count(), 11);
    }

    #[test]
    fn a_padded_status_still_names_the_state_it_reports() {
        // The consequential half of the same rule: a wait treats an unrecognised state as
        // progress on purpose, so an untrimmed "COMPLETED " would leave a finished sync
        // being polled until the timeout.
        let res = serde_json::json!({"status": " COMPLETED\n"});
        assert_eq!(status_of(&res, "status"), "COMPLETED");
        assert_eq!(status_of(&serde_json::json!({}), "status"), "");
    }

    #[test]
    fn blanks_say_nothing() {
        assert!(!is_blank("main"));
        assert!(is_blank(""));
        // The spelling an `is_empty` guard mistakes for an answer: it would print
        // "Entered dev mode on     (forked from main)", suggest `--branch '   '`, and
        // report a failed sync as "failed: " with the reason slot full of blanks.
        assert!(is_blank("   "));
        assert!(is_blank("\n"));
        assert_eq!(branch_or_placeholder("   "), "<branch>");
        assert_eq!(branch_or_placeholder("main"), "main");
        // Padded but real: returned verbatim, because the same name goes into a
        // `delete-branch` in the same sentence and a trimmed one would address a
        // different branch. `shell_quote` is what makes the padding visible.
        assert_eq!(branch_or_placeholder("  main  "), "  main  ");
        assert_eq!(
            shell_quote(&branch_or_placeholder("  main  ")),
            "'  main  '"
        );
    }

    #[test]
    fn shell_quote_survives_what_a_ref_name_may_legally_carry() {
        // `git check-ref-format` permits all of these, and each one changes what an
        // unquoted command does — `#` comments the rest away, `;` starts a new command,
        // `$` expands to nothing at all.
        assert_eq!(shell_quote("feat#1234"), "'feat#1234'");
        assert_eq!(shell_quote("a;rm -rf b"), "'a;rm -rf b'");
        assert_eq!(shell_quote("$HOME"), "'$HOME'");
        // An empty value stays visible rather than vanishing into the command line —
        // `deployments::named_branch`'s fallback reasons from this.
        assert_eq!(shell_quote(""), "''");
        // A single quote is legal too, so it can't simply be wrapped: close, escape,
        // reopen — the only form that works inside single quotes.
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn parse_kv_accepts_key_value_pairs() {
        assert_eq!(parse_kv("A=1").unwrap(), ("A".to_string(), "1".to_string()));
        assert_eq!(
            parse_kv("A=b=c").unwrap(),
            ("A".to_string(), "b=c".to_string())
        );
        assert!(parse_kv("no-equals").is_err());
        assert!(parse_kv("=value").is_err());
    }

    #[test]
    fn parse_data_accepts_inline_json_objects_only() {
        let map = parse_data(Some(r#"{"name": "x", "n": 1}"#)).unwrap();
        assert_eq!(map.get("name"), Some(&json!("x")));
        assert_eq!(map.get("n"), Some(&json!(1)));
        assert!(parse_data(Some("[1, 2]")).is_err());
        assert!(parse_data(Some("not json")).is_err());
        assert!(parse_data(None).unwrap().is_empty());
    }

    #[test]
    fn normalize_url_defaults_to_https() {
        assert_eq!(
            normalize_url("cloud.cubecloud.dev"),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url(" cloud.cubecloud.dev/ "),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url("https://cloud.cubecloud.dev/"),
            "https://cloud.cubecloud.dev"
        );
        assert_eq!(
            normalize_url("http://localhost:4000"),
            "http://localhost:4000"
        );
        assert_eq!(normalize_url(""), "");
    }

    #[test]
    fn nonempty_rejects_supplied_but_empty() {
        assert!(nonempty("main").is_ok());
        assert!(nonempty("dbt-sync/x").is_ok());
        assert!(nonempty("").is_err());
        assert!(nonempty("   ").is_err());
        assert!(nonempty_target("main").is_ok());
        assert!(nonempty_target("").is_err());
        assert!(nonempty_target("\t").is_err());
    }

    /// Where every `--branch`/`--ref` argument in the command tree stands on an empty
    /// value, as two lists nobody can change without saying so in the diff.
    ///
    /// It used to assert they all refused one. Most optional branch flags now accept it
    /// and send it as an empty field, so the property worth holding is no longer
    /// "everywhere" but "exactly this partition". Both halves are asserted because each
    /// closes a direction the other is blind to: a guard dropped off an argument, and a
    /// new `--branch` declared without anyone deciding which side it belongs on. The
    /// old "does everything refuse?" shape covered the second for free; a one-sided
    /// list would not.
    ///
    /// It goes through real argument parsing rather than introspecting the parser
    /// (clap keeps `ValueParser::parse_ref` private), filling every other required
    /// argument with a placeholder so that the only thing left to object to is the
    /// empty value.
    #[test]
    fn only_the_listed_branch_arguments_refuse_an_empty_value() {
        use clap::{CommandFactory, Parser};

        // On the action, not on `get_num_args()`: the derive leaves num_args
        // unresolved until `Command::build`, so an unbuilt tree reports `None` for
        // `--delete-branch`-style bools and they read as value-taking.
        fn takes_a_value(arg: &clap::Arg) -> bool {
            matches!(
                arg.get_action(),
                clap::ArgAction::Set | clap::ArgAction::Append
            )
        }

        /// `cube <path...>` with `target` empty and every other required argument at
        /// a placeholder. Options come before positionals, which clap requires.
        fn argv(path: &[String], cmd: &clap::Command, target: &clap::Arg) -> Vec<String> {
            let mut opts = Vec::new();
            let mut positionals = Vec::new();
            for arg in cmd.get_arguments() {
                let is_target = arg.get_id() == target.get_id();
                if !is_target && !arg.is_required_set() {
                    continue;
                }
                let value = if is_target { "" } else { "1" };
                if arg.is_positional() {
                    positionals.push(value.to_string());
                } else {
                    opts.push(format!(
                        "--{}",
                        arg.get_long().unwrap_or(arg.get_id().as_str())
                    ));
                    if takes_a_value(arg) {
                        opts.push(value.to_string());
                    }
                }
            }
            [path, &opts[..], &positionals[..]].concat()
        }

        fn walk(
            cmd: &clap::Command,
            path: Vec<String>,
            refuses: &mut Vec<String>,
            accepts: &mut Vec<String>,
        ) {
            for arg in cmd.get_arguments() {
                // Matched by name, not by exact id, so a flag *added* under a name I
                // did not anticipate is caught too — `--base-branch` alongside an
                // existing `--branch` is the plausible next edit. `branch` is safe as
                // a substring (`base_branch`, `source_branch`, `branch_name` all mean
                // a branch); `ref` is not, since `prefix` and `refresh` contain it,
                // and a false positive here is worse than a gap — it would fail
                // confidently on an ordinary `--prefix` and invite a parser whose
                // message describes nothing about that flag. `takes_a_value` keeps the
                // `--delete-branch`-style bools out.
                let id = arg.get_id().as_str();
                let branchy = id.contains("branch")
                    || id == "ref"
                    || id.ends_with("_ref")
                    || id.starts_with("ref_");
                if !takes_a_value(arg) || !branchy {
                    continue;
                }
                // As the user would type it, not as the field is named: ids are
                // snake_case, so `--{id}` prints `--base_ref` for a `--base-ref`, and
                // `--branch` for the four positional BRANCH arguments. Both are flags
                // that don't exist, and this message is the whole interface of the
                // test — someone reading it in CI greps for what it printed.
                let flag = if arg.is_positional() {
                    // Ask clap here too: an explicit `value_name` on a branchy
                    // positional would otherwise be reported as a placeholder that
                    // appears nowhere in --help.
                    let name = arg
                        .get_value_names()
                        .and_then(|names| names.first())
                        .map(|name| name.to_string())
                        .unwrap_or_else(|| id.to_uppercase());
                    format!("{} <{name}>", path.join(" "))
                } else {
                    format!("{} --{}", path.join(" "), arg.get_long().unwrap_or(id))
                };
                let err = crate::Cli::try_parse_from(argv(&path, cmd, arg))
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default();
                if err.is_empty() {
                    accepts.push(flag);
                    continue;
                }
                // Anything else means the placeholder argv failed to parse for a reason
                // of its own, which would otherwise be recorded as a refusal the command
                // never expressed.
                assert!(
                    err.contains(EMPTY_VALUE_REFUSED),
                    "{flag} failed to parse for a reason other than its empty value, so \
                     this test learned nothing about it: {err}"
                );
                refuses.push(flag);
            }
            for sub in cmd.get_subcommands() {
                let mut path = path.clone();
                path.push(sub.get_name().to_string());
                walk(sub, path, refuses, accepts);
            }
        }

        let (mut refuses, mut accepts) = (Vec::new(), Vec::new());
        walk(
            &crate::Cli::command(),
            vec!["cube".into()],
            &mut refuses,
            &mut accepts,
        );
        refuses.sort();
        accepts.sort();
        // Both sides, not just the guarded one. A list rather than a count because the
        // answer is no longer the same for every branch argument and a number can only
        // say that something moved, not what. Both lists, because `refuses` catches a
        // guard dropped off an argument and `accepts` catches a new one declared without
        // anyone deciding its side — the walk records parse time only.
        //
        // Exact, not a floor: the filter only sees ids naming a branch or ref, so a
        // rename out of that family or a removal shrinks the walk silently.
        //
        // The split is not by what a blank costs. `deploy --branch` and `build-status
        // --branch` share a fallback and only the second is guarded, though a blank
        // `deploy` uploads and prunes; `merge-to-default` merges to deploy and deletes
        // the source. Both were raised in review and left accepting.
        assert_eq!(
            refuses,
            [
                // Required, so clap won't let you omit them: a blank is a name the
                // caller failed to supply. `dev-mode ""` from a CI gate whose variable
                // came out empty would otherwise enter dev mode on an unnamed branch.
                "cube data-model delete-branch <BRANCH>",
                "cube data-model dev-mode <BRANCH>",
                "cube data-model disable-branch <BRANCH>",
                "cube data-model enable-branch <BRANCH>",
                // Optional, and guarded anyway. The pair whose empty-value behaviour was
                // measured rather than assumed — see `nonempty_target` — plus
                // `dbt sync --branch`, which names the branch a sync creates and so
                // decides whether `verify_ref_applied` runs at all.
                "cube dbt sync --branch",
                "cube dbt sync --ref",
                "cube deployments build-status --branch",
            ],
            "the set of branch arguments refusing an empty value changed. Adding one is \
             `value_parser = util::nonempty` on the declaration; dropping one means a \
             blank for that argument now travels as an empty field, so say why here."
        );
        assert_eq!(
            accepts,
            [
                "cube agents skills --branch",
                "cube data-model commit --branch",
                "cube data-model delete --branch",
                "cube data-model file-hashes --branch",
                "cube data-model get --branch",
                "cube data-model list --branch",
                "cube data-model merge --branch",
                "cube data-model merge-to-default --branch",
                "cube data-model pull --branch",
                "cube data-model put --branch",
                "cube data-model rename --branch",
                "cube deploy --branch",
                "cube github connect --branch",
            ],
            "a branch argument that takes an empty value at parse time was added, \
             removed or renamed. This list records the parser's answer alone — that a \
             blank then travels as an empty field is a property of whatever builds the \
             request, which is usually `set`, `push` or `write_body` but not always. A \
             new one belongs here only if you want the parser to take a blank; check \
             what the command then does with it."
        );
    }

    #[test]
    fn parse_duration_reads_the_unit_suffix() {
        use std::time::Duration;
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration(" 5m ").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("30S").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15M").unwrap(), Duration::from_secs(900));
    }

    #[test]
    fn parse_duration_rejects_what_it_cannot_honour() {
        // A zero interval would spin, and a zero timeout would expire before the
        // first poll — both are mistakes, not configurations.
        assert!(parse_duration("0").is_err());
        assert!(parse_duration("0m").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("-5s").is_err());
        assert!(parse_duration("5d").is_err());
        // Rejected, not panicked on in debug or wrapped in release.
        assert!(parse_duration("9999999999999999999h").is_err());
        assert!(parse_duration(&format!("{}h", u64::MAX)).is_err());
    }

    #[test]
    fn set_skips_missing_flags() {
        let mut map = Map::new();
        set(&mut map, "present", &Some("v"));
        set(&mut map, "absent", &None::<String>);
        assert_eq!(map.get("present"), Some(&json!("v")));
        assert!(!map.contains_key("absent"));
    }

    #[test]
    fn only_a_data_only_endpoint_renders_the_deprecated_field() {
        // The in-memory list: `items` ignores offset paging, so `data` it is.
        for (a, b) in [(Some(10), None), (None, Some(5)), (Some(10), Some(5))] {
            let field = paging_field(OFFSET_LIMIT_DATA_ONLY, a, b, None, None).unwrap();
            assert_eq!(field, Some("data"));
        }

        // Query-sliced endpoints must NOT ask for `data`: `items` is already
        // the requested page, and stays right after `data` is removed —
        // asking would fail a command the server still honors.
        for paging in [OFFSET_LIMIT_IN_QUERY, LIMIT_PAGE_IN_QUERY] {
            let field = paging_field(paging, Some(10), Some(5), None, None).unwrap();
            assert_eq!(field, None);
        }
    }

    #[test]
    fn paging_field_renders_items_whenever_offset_paging_is_unused() {
        for paging in [
            OFFSET_LIMIT_DATA_ONLY,
            OFFSET_LIMIT_IN_QUERY,
            LIMIT_PAGE_IN_QUERY,
        ] {
            assert_eq!(paging_field(paging, None, None, None, None).unwrap(), None);
            for (first, after) in [(Some(5), None), (None, Some("cursor"))] {
                assert_eq!(
                    paging_field(paging, None, None, first, after).unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn paging_field_rejects_mixing_and_names_the_command_s_own_flags() {
        assert!(paging_field(OFFSET_LIMIT_DATA_ONLY, Some(10), None, Some(5), None).is_err());
        assert!(paging_field(OFFSET_LIMIT_IN_QUERY, None, Some(5), None, Some("cursor")).is_err());

        // `reports list` deprecates --limit/--page, not --offset/--limit.
        let err = paging_field(LIMIT_PAGE_IN_QUERY, Some(50), None, Some(5), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--limit/--page"), "got: {err}");
        assert!(err.contains("--first/--after"), "got: {err}");
    }
}
