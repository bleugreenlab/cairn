/// Parse Cairn's compact duration vocabulary into milliseconds.
///
/// Bounds belong to callers because a durable wait and a recurring schedule
/// have different safe operating ranges.
pub(crate) fn parse_duration_ms(value: &str) -> Result<u64, String> {
    let unit_start = value
        .find(|c: char| !c.is_ascii_digit())
        .ok_or("duration needs ms, s, m, h, or d")?;
    let amount: u64 = value[..unit_start]
        .parse()
        .map_err(|_| "invalid duration")?;
    let multiplier = match &value[unit_start..] {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err("duration needs ms, s, m, h, or d".into()),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration too large".into())
}

#[cfg(test)]
mod tests {
    use super::parse_duration_ms;

    #[test]
    fn parses_every_supported_unit() {
        assert_eq!(parse_duration_ms("2ms").unwrap(), 2);
        assert_eq!(parse_duration_ms("3s").unwrap(), 3_000);
        assert_eq!(parse_duration_ms("4m").unwrap(), 240_000);
        assert_eq!(parse_duration_ms("5h").unwrap(), 18_000_000);
        assert_eq!(parse_duration_ms("6d").unwrap(), 518_400_000);
    }

    #[test]
    fn preserves_duration_parse_errors() {
        assert_eq!(
            parse_duration_ms("3").unwrap_err(),
            "duration needs ms, s, m, h, or d"
        );
        assert_eq!(
            parse_duration_ms("3w").unwrap_err(),
            "duration needs ms, s, m, h, or d"
        );
        assert_eq!(parse_duration_ms("ms").unwrap_err(), "invalid duration");
        assert_eq!(
            parse_duration_ms("18446744073709551615d").unwrap_err(),
            "duration too large"
        );
    }
}
