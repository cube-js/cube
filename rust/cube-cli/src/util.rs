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

/// Reject a supplied-but-empty branch or ref at parse time.
///
/// Attached to flag declarations as a clap `value_parser`, so it holds for a flag
/// the moment the flag exists — including through `Option<String>`, where `None`
/// stays legal and `Some("")` can no longer be constructed. Hand-written calls in
/// match arms did not hold: this check was added three times, and each round found
/// another flag that had been missed. The last one was `github connect --branch`,
/// which sends its value under the key `branch` and so escaped even a grep for
/// `branchName`. `every_branch_and_ref_flag_refuses_an_empty_value` counts the flags
/// and holds the property, so the number lives in that assertion and nowhere else.
///
/// The reason it is worth rejecting at all: an empty value is not dropped, it is
/// sent as an empty field, and what an empty field means is then the server's
/// choice rather than the caller's. Where that choice has actually been measured,
/// `nonempty_target` says so instead.
///
/// Empty values reach the CLI from scripts, not just from typos: `jq -r` prints
/// nothing at all for empty input, and `$GITHUB_HEAD_REF` is empty on every trigger
/// but `pull_request`. (A missing *field* prints `null`, which travels as a literal
/// name and gets a 404 — loud, and the right answer.) The reachable case is not a
/// mistyped flag; it is `data-model dev-mode ""` from a CI gate whose branch
/// variable came out empty, which without this would enter dev mode on the deploy
/// branch and leave every later step of the gate checking production.
///
/// Also used for `create-branch <name>`, which the walk's id filter does not reach —
/// that one guard is a local decision, not a property the test holds.
pub fn nonempty(s: &str) -> Result<String, String> {
    if s.trim().is_empty() {
        return Err("an empty value is still sent, as an empty field — what it \
                    then means is the server's choice rather than yours"
            .to_string());
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
        return Err(
            "an empty value is read as \"not specified\" and falls back to \
                    the branch the server would have picked anyway, so this would \
                    run against code you did not name"
                .to_string(),
        );
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
pub fn branch_or_placeholder(branch: &str) -> String {
    if names_a_branch(branch) {
        branch.to_string()
    } else {
        "<branch>".to_string()
    }
}

/// Whether a payload actually named a branch.
///
/// Trim-aware, like `nonempty` above: a name of blanks is the other spelling of "named
/// nothing", and the only one that an `is_empty` check still reads as a name. That matters
/// most where the answer picks BETWEEN two sentences rather than filling one in — taking
/// the wrong arm prints both a hole mid-sentence and a suggested command that can't work,
/// which is worse than either alone.
pub fn names_a_branch(branch: &str) -> bool {
    !branch.trim().is_empty()
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
    fn blanks_do_not_name_a_branch() {
        assert!(names_a_branch("main"));
        assert!(!names_a_branch(""));
        // The spelling an `is_empty` guard mistakes for a name: it would print
        // "Entered dev mode on     (forked from main)" and suggest `--branch '   '`.
        assert!(!names_a_branch("   "));
        assert!(!names_a_branch("\n"));
        assert_eq!(branch_or_placeholder("   "), "<branch>");
        assert_eq!(branch_or_placeholder("main"), "main");
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

    /// Holds the promise the doc comment makes, which hand-written calls in match
    /// arms could not: every `--branch`/`--ref` in the whole command tree refuses an
    /// empty value. A new branch-taking flag is a copy of a declaration, so this
    /// fails the moment one is declared without the parser.
    ///
    /// It goes through real argument parsing rather than introspecting the parser
    /// (clap keeps `ValueParser::parse_ref` private), filling every other required
    /// argument with a placeholder so that the only thing left to object to is the
    /// empty value.
    #[test]
    fn every_branch_and_ref_flag_refuses_an_empty_value() {
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

        fn walk(cmd: &clap::Command, path: Vec<String>, checked: &mut Vec<String>) {
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
                assert!(
                    err.contains("an empty value"),
                    "{flag} accepts an empty value — add \
                     `value_parser = util::nonempty` to the declaration.\n\
                     Got instead: {err}"
                );
                checked.push(flag);
            }
            for sub in cmd.get_subcommands() {
                let mut path = path.clone();
                path.push(sub.get_name().to_string());
                walk(sub, path, checked);
            }
        }

        let mut checked = Vec::new();
        walk(&crate::Cli::command(), vec!["cube".into()], &mut checked);
        // Exact, not a floor: the filter can only see ids that name a branch or a
        // ref, so a rename out of that family (`branch` → `name`, as
        // `create-branch <name>` already is) or an outright removal would shrink the
        // walk, and a floor would absorb that silently.
        assert_eq!(
            checked.len(),
            20,
            "a branch/ref flag was added, removed or renamed — update the count and \
             check the new one carries `value_parser = util::nonempty`: {checked:#?}"
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
