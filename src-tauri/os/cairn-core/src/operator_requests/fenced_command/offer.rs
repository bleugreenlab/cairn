//! The text a refusal appends when a denied `run` item recovered no path: a
//! `fenced_command` request the agent can file itself, or why that act is not
//! available where the item ran.
//!
//! This only chooses words. It files nothing and authorizes nothing: filing is
//! the agent's own `write`, and only the operator's acceptance runs anything.

use serde_json::json;

/// The offer appended to a no-path denial of `command` on `executor`.
///
/// `floor` is whether that executor can take a `fenced_command` now, with the
/// sentence that says why not. `declared_check` is whether the canonical
/// project configuration declares the command as a check, which already runs
/// unfenced through the check cadences.
pub(crate) fn offer_for(
    command: &str,
    executor: Option<&str>,
    floor: Result<(), String>,
    declared_check: bool,
) -> String {
    let mut offer = String::new();
    if declared_check {
        offer.push_str(
            "This command is a check the project declares, and declared checks already run \
             unfenced as checks: `cairn check run <suite>` runs it that way and records the \
             verdict, with no request needed.\n",
        );
    }
    let executor = match (executor, floor) {
        (Some(executor), Ok(())) => executor,
        (_, Err(unavailable)) => {
            offer.push_str(&format!(
                "Asking the operator to run this exact command once outside the fence (a \
                 `fenced_command` request) is not available here: {unavailable}"
            ));
            return offer;
        }
        (None, Ok(())) => {
            offer.push_str(
                "Asking the operator to run this exact command once outside the fence (a \
                 `fenced_command` request) needs the executor it runs on, and this denial does \
                 not say which executor ran the item.",
            );
            return offer;
        }
    };
    let payload = json!({
        "kind": "fenced_command",
        "target": { "executor": executor, "command": command },
        "rationale": "<why this work must reach outside the checkout and $TMPDIR>",
        "wait": true,
    });
    let request = json!({
        "changes": [{ "target": "cairn://requests", "mode": "append", "payload": payload }],
    });
    offer.push_str(&format!(
        "If this work genuinely needs to run outside the fence, you can ask the operator to run \
         this exact command once on {executor}, outside the fence but still unable to read \
         Cairn's credentials. Filing the request runs nothing and grants nothing: the operator \
         decides whether to accept it, and an acceptance runs the command once. Fill in the \
         rationale, then file it with `write`:\n{request}\n\
         A need that recurs is not a request each time: ask for a declared writable path \
         (cairn/7081) instead."
    ));
    offer
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The payload line of an offer, parsed back as the `write` call it is.
    fn filed(offer: &str) -> serde_json::Value {
        let line = offer
            .lines()
            .find(|line| line.starts_with('{'))
            .unwrap_or_else(|| panic!("no payload in: {offer}"));
        serde_json::from_str(line).unwrap()
    }

    #[test]
    fn an_available_executor_gets_a_ready_to_file_request_for_the_exact_command() {
        let command = "printf '%s\n' \"a b\" > ~/.config/tool/rc && make install";
        let offer = offer_for(command, Some("local"), Ok(()), false);
        let request = filed(&offer);
        let change = &request["changes"][0];
        assert_eq!(change["target"], "cairn://requests");
        assert_eq!(change["mode"], "append");
        let payload = &change["payload"];
        assert_eq!(payload["kind"], "fenced_command");
        assert_eq!(payload["target"]["executor"], "local");
        assert_eq!(payload["target"]["command"], command);
        assert_eq!(payload["wait"], true);
        assert!(payload["rationale"].is_string());
        assert!(offer.contains("operator decides"), "{offer}");
        assert!(offer.contains("runs nothing and grants nothing"), "{offer}");
        assert!(!offer.contains("cairn check run"), "not a check: {offer}");
    }

    #[test]
    fn an_unavailable_floor_explains_itself_and_offers_nothing_to_file() {
        let why = "fenced_command is unavailable on box (linux): its operator-credential floor is not measured.";
        let offer = offer_for("make install", Some("box"), Err(why.into()), false);
        assert!(offer.contains(why), "{offer}");
        assert!(!offer.contains("cairn://requests"), "{offer}");
        assert!(!offer.lines().any(|line| line.starts_with('{')), "{offer}");
    }

    #[test]
    fn an_unknown_executor_offers_nothing_to_file() {
        let offer = offer_for("make install", None, Ok(()), false);
        assert!(offer.contains("does not say which executor"), "{offer}");
        assert!(!offer.contains("cairn://requests"), "{offer}");
    }

    #[test]
    fn a_declared_check_points_at_the_check_route_whatever_the_floor() {
        let available = offer_for("bun run test:rust", Some("local"), Ok(()), true);
        assert!(available.contains("cairn check run <suite>"), "{available}");
        assert_eq!(
            filed(&available)["changes"][0]["payload"]["target"]["command"],
            "bun run test:rust"
        );
        let unavailable = offer_for(
            "bun run test:rust",
            Some("box"),
            Err("no floor".into()),
            true,
        );
        assert!(
            unavailable.contains("cairn check run <suite>"),
            "{unavailable}"
        );
        assert!(unavailable.contains("no floor"), "{unavailable}");
    }
}
