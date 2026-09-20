//! Tiny CLI flag parsing shared by both binaries.
//!
//! Accepts BOTH `--flag value` and `--flag=value`.
//!
//! This is not cosmetic: the Kubernetes manifests pass the `=` form, and an
//! earlier version of these binaries only matched the space-separated form. The
//! result was a silent fallback to defaults — the bridge launched with
//! `--source=ingest` *ignored* and came up in synthetic mode, and
//! `--hexdump-first=3` was ignored so the spike output never appeared. Silently
//! ignoring a flag in a deployment is worse than rejecting it, so this module
//! also reports unrecognized flags for the caller to surface.

use std::str::FromStr;

/// Value of `--name`, accepting `--name value` and `--name=value`.
///
/// Returns the first match: `--name=a` wins over a later `--name b`.
pub fn flag_value(args: &[String], name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    for (i, arg) in args.iter().enumerate() {
        if arg == name {
            return args.get(i + 1).cloned();
        }
        if let Some(value) = arg.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
    }
    None
}

/// [`flag_value`] parsed into `T`, falling back to `default` when absent or
/// unparseable.
pub fn parse_or<T: FromStr>(args: &[String], name: &str, default: T) -> T {
    flag_value(args, name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `--`-prefixed arguments that are not in `known`, with any `=value` stripped.
///
/// Callers log these: a typo or an unsupported syntax must be visible rather than
/// silently changing behaviour. Values are not reported (they may be a URL, and
/// they are not what is being validated).
pub fn unknown_flags(args: &[String], known: &[&str]) -> Vec<String> {
    let mut unknown: Vec<String> = args
        .iter()
        .filter(|arg| arg.starts_with("--"))
        .map(|arg| arg.split('=').next().unwrap_or(arg).to_string())
        .filter(|key| !known.contains(&key.as_str()))
        .collect();
    unknown.sort();
    unknown.dedup();
    unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn accepts_the_space_separated_form() {
        assert_eq!(
            flag_value(&args(&["bin", "--source", "ingest"]), "--source"),
            Some("ingest".to_string())
        );
    }

    /// The actual regression: manifests pass `--flag=value`, which used to be
    /// ignored, so the bridge came up synthetic in the deployed path.
    #[test]
    fn accepts_the_equals_form() {
        assert_eq!(
            flag_value(&args(&["bin", "--source=ingest"]), "--source"),
            Some("ingest".to_string())
        );
        assert_eq!(
            flag_value(&args(&["bin", "--static=/app/static"]), "--static"),
            Some("/app/static".to_string())
        );
    }

    #[test]
    fn equals_form_handles_values_containing_equals() {
        assert_eq!(
            flag_value(
                &args(&["bin", "--bridge-url=http://host:4780/?a=b"]),
                "--bridge-url"
            ),
            Some("http://host:4780/?a=b".to_string())
        );
    }

    #[test]
    fn missing_flag_is_none_and_parse_or_uses_the_default() {
        assert_eq!(flag_value(&args(&["bin", "--static", "."]), "--source"), None);
        assert_eq!(parse_or(&args(&["bin"]), "--publish-hz", 30.0_f64), 30.0);
        assert_eq!(
            parse_or(&args(&["bin", "--publish-hz=12.5"]), "--publish-hz", 30.0_f64),
            12.5
        );
        assert_eq!(
            parse_or(&args(&["bin", "--publish-hz", "7"]), "--publish-hz", 30.0_f64),
            7.0
        );
    }

    #[test]
    fn first_occurrence_wins() {
        assert_eq!(
            flag_value(&args(&["bin", "--source=ingest", "--source", "synthetic"]), "--source"),
            Some("ingest".to_string())
        );
    }

    #[test]
    fn unknown_flags_are_reported_but_known_ones_are_not() {
        let known = ["--source", "--static", "--center-freq"];
        let argv = args(&[
            "bin",
            "--source=ingest",
            "--static",
            "/app/static",
            "--center-freq=915000000",
            "--typo-flag",
            "--other=1",
            "positional",
        ]);
        assert_eq!(
            unknown_flags(&argv, &known),
            vec!["--other".to_string(), "--typo-flag".to_string()]
        );
        assert!(unknown_flags(&args(&["bin", "--source", "ingest"]), &known).is_empty());
    }
}
