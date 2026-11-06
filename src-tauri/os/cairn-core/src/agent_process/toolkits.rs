pub use cairn_session::agent_process::toolkits::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// A built-in the CLI declares despite `--tools ""` is named in an error
    /// on the tool-surface target, every time it is seen.
    #[test]
    fn a_declared_built_in_trips_the_tool_surface_log_every_time() {
        crate::test_support::capture_log(TOOL_SURFACE_LOG_TARGET);
        let mut declared = cairn_models::wire::claude_stream::Init::from_data(
            &serde_json::from_str(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../cairn-models/tests/fixtures/claude_init_tools_none-2.1.311.json"
            )))
            .unwrap(),
        );
        declared.tools.push("Monitor".to_string());

        let run = "run-tool-surface-tripwire";
        let logged = || {
            crate::test_support::captured_log(TOOL_SURFACE_LOG_TARGET)
                .into_iter()
                .filter(|line| line.contains(run))
                .collect::<Vec<_>>()
        };

        assert_eq!(report_declared_surface(run, &declared), vec!["Monitor"]);
        let first = logged();
        assert_eq!(first.len(), 1, "{first:?}");
        assert!(first[0].contains("Monitor"), "{}", first[0]);
        assert!(!first[0].contains("mcp__cairn__read"), "{}", first[0]);

        // No once-per-process memory: the next session reports it again.
        assert_eq!(report_declared_surface(run, &declared), vec!["Monitor"]);
        assert_eq!(logged().len(), 2);
    }
}
