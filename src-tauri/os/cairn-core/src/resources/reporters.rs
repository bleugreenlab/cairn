//! Read-only descriptions of the known system message reporters.

use crate::messages::reporters::{Reporter, ALL};

pub(super) fn collection() -> String {
    let mut out = String::from("# Cairn reporters\n\nKnown machine senders, not the operator.\n\n");
    for reporter in ALL {
        out.push_str(&format!(
            "- [{}]({}) — {}\n",
            reporter.name, reporter.uri, reporter.reports
        ));
    }
    out
}

pub(super) fn member(name: &str) -> Result<String, String> {
    let Some(reporter) = ALL
        .iter()
        .find(|reporter| reporter.uri.rsplit('/').next() == Some(name))
    else {
        return Err(format!(
            "Reporter not found: {name}. Read cairn://reporters for known reporters."
        ));
    };
    Ok(describe(reporter))
}

fn describe(reporter: &Reporter) -> String {
    format!(
        "# {}\n\n{}\n\n{}\n\nUrgency: `{}`. {}\n\n\
         Attribution: a known Cairn system sender, delivered as a `peer` wake referenced by `{}`, never as the operator.\n\n\
         To mute this reporter on your own node or thread:\n\n\
         ```\nwrite cairn:~/wakes {{mute:{{kind:\"peer\",ref:\"{}\"}}}}\n```\n\n\
         A mute keeps the conversation message but creates no wake. A peer mute with no reference also covers this reporter. `steer` does not pierce a mute.\n\n\
         This resource is read-only; it is not a reply destination.\n",
        reporter.name, reporter.uri, reporter.reports, reporter.urgency.as_str(), reporter.urgency_reason, reporter.uri, reporter.uri,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_reporters_are_readable_with_their_exact_mute_scope() {
        for reporter in ALL {
            let resource =
                cairn_uri::parse_uri(reporter.uri).expect("a reporter is a real address");
            assert_eq!(resource.to_uri(), reporter.uri);
            let name = reporter.uri.rsplit('/').next().unwrap();
            let read = member(name).unwrap();
            assert!(collection().contains(reporter.uri));
            assert!(read.contains(&format!("ref:\"{}\"", reporter.uri)));
            assert!(read.contains("`steer`"));
            assert!(read.contains("read-only"));
        }
        let error = member("unknown").expect_err("unknown reporters are failed reads");
        assert!(error.contains("not found"));
        assert!(error.contains("cairn://reporters"));
    }
}
