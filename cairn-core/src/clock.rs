use chrono::{DateTime, Offset, Utc};
use chrono_tz::Tz;

#[derive(Clone, Debug)]
pub(crate) struct HostClock {
    timezone_name: String,
    timezone: Tz,
}

impl HostClock {
    pub(crate) fn local() -> Self {
        let timezone_name = iana_time_zone::get_timezone().unwrap_or_else(|_| "UTC".to_string());
        let timezone = timezone_name.parse().unwrap_or(chrono_tz::UTC);
        Self {
            timezone_name,
            timezone,
        }
    }

    #[cfg(test)]
    pub(crate) fn fixed(timezone_name: &str) -> Self {
        Self {
            timezone_name: timezone_name.to_string(),
            timezone: timezone_name.parse().expect("valid test timezone"),
        }
    }

    pub(crate) fn timezone_name(&self) -> &str {
        &self.timezone_name
    }

    pub(crate) fn orientation_line(&self, now: DateTime<Utc>) -> String {
        let local = now.with_timezone(&self.timezone);
        let offset_seconds = local.offset().fix().local_minus_utc();
        format!(
            "Clock: {} {} ({})",
            local.format("%a %Y-%m-%d %H:%M"),
            self.timezone_name,
            format_utc_offset(offset_seconds),
        )
    }

    pub(crate) fn resume_prefix(&self, now: DateTime<Utc>, previous_end: Option<i64>) -> String {
        let local = now.with_timezone(&self.timezone);
        let elapsed = previous_end
            .map(|ended_at| now.timestamp().saturating_sub(ended_at))
            .filter(|seconds| *seconds >= 60);
        match elapsed {
            Some(seconds) => format!(
                "[{} — resumed after {}]",
                local.format("%a %H:%M"),
                format_elapsed(seconds)
            ),
            // Sub-minute gaps are intentionally omitted: second-level precision is
            // noise at a turn boundary and would imply more accuracy than agents need.
            None => format!("[{} — resumed]", local.format("%a %H:%M")),
        }
    }

    pub(crate) fn message_stamp(
        &self,
        timestamp: i64,
        previous_local_date: Option<chrono::NaiveDate>,
    ) -> Option<(String, chrono::NaiveDate)> {
        let local = DateTime::from_timestamp(timestamp, 0)?.with_timezone(&self.timezone);
        let date = local.date_naive();
        let stamp = if previous_local_date.is_some_and(|previous| previous != date) {
            local.format("%Y-%m-%d %H:%M:%S").to_string()
        } else {
            local.format("%H:%M:%S").to_string()
        };
        Some((stamp, date))
    }
}

fn format_utc_offset(offset_seconds: i32) -> String {
    let sign = if offset_seconds < 0 { '-' } else { '+' };
    let total_minutes = offset_seconds.unsigned_abs() / 60;
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;
    if minutes == 0 {
        format!("UTC{sign}{hours}")
    } else {
        format!("UTC{sign}{hours}:{minutes:02}")
    }
}

fn format_elapsed(seconds: i64) -> String {
    let minutes = seconds / 60;
    let days = minutes / (24 * 60);
    let hours = (minutes / 60) % 24;
    let minutes = minutes % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 || parts.is_empty() {
        parts.push(format!("{minutes}m"));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(timestamp: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(timestamp, 0).unwrap()
    }

    #[test]
    fn pins_orientation_and_resume_formats() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let now = utc(1_752_381_600); // 2025-07-12 21:40 PDT
        assert_eq!(
            clock.orientation_line(now),
            "Clock: Sat 2025-07-12 21:40 America/Los_Angeles (UTC-7)"
        );
        assert_eq!(
            clock.resume_prefix(now, Some(now.timestamp() - (3 * 3600 + 12 * 60))),
            "[Sat 21:40 — resumed after 3h 12m]"
        );
        assert_eq!(
            clock.resume_prefix(now, Some(now.timestamp() - 59)),
            "[Sat 21:40 — resumed]"
        );
    }

    #[test]
    fn message_stamp_adds_date_only_on_local_day_rollover() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let first = utc(1_752_387_599); // 2025-07-12 23:19:59 PDT
        let second = utc(1_752_387_601); // same local day
        let next_day = utc(1_752_391_200); // 2025-07-13 00:20:00 PDT
        let (first_stamp, date) = clock.message_stamp(first.timestamp(), None).unwrap();
        let (second_stamp, date) = clock.message_stamp(second.timestamp(), Some(date)).unwrap();
        let (next_stamp, _) = clock
            .message_stamp(next_day.timestamp(), Some(date))
            .unwrap();
        assert_eq!(first_stamp, "23:19:59");
        assert_eq!(second_stamp, "23:20:01");
        assert_eq!(next_stamp, "2025-07-13 00:20:00");
    }
}
