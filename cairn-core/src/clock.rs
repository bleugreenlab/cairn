//! The display layer for absolute times.
//!
//! Storage stays epoch/UTC. This module is the only place that turns an instant
//! into text a person or an agent reads, and it enforces one rule: **one clock
//! per rendered timestamp, and that clock is named.** `2026-08-01 16:47 PDT`,
//! never a bare `16:47` whose zone the reader has to infer.
//!
//! The rule exists because the inference used to be wrong. Transcript turn
//! headers rendered UTC, resume markers rendered host-local, neither said which,
//! and an agent reading both reasoned as if it were near midnight at 4pm local
//! (CAIRN-3428). Every cross-surface comparison paid a "which clock is this"
//! tax, worst during incident forensics where `~/.cairn` logs speak UTC and the
//! operator speaks local.
//!
//! The chosen clock is the host's — the machine the runner, its terminals, and
//! its operator all share — so a rendered time is the same time the operator
//! would read off their own menu bar. The zone abbreviation (`PDT`, `UTC`, or a
//! numeric `+04` for zones that have no abbreviation) rides with it.
//!
//! [`date`] is the one deliberate exemption: a calendar date has no hour to
//! misread, and labelling it would only add noise. It still comes off this same
//! host clock, so it can never disagree by a day with the times printed beside
//! it — which is exactly what a UTC-derived date next to local times used to do.

use chrono::{DateTime, Offset, Utc};
use chrono_tz::Tz;
use std::sync::OnceLock;

/// The process-wide host clock.
///
/// The zone is read from the host once and then held. Daylight saving is still
/// tracked correctly — the abbreviation and offset are derived per instant from
/// the zone's rules — so only a zone *reconfiguration* mid-process is missed,
/// and one stable clock across a process's whole output is worth more than
/// picking that up.
pub(crate) fn host() -> &'static HostClock {
    static HOST: OnceLock<HostClock> = OnceLock::new();
    HOST.get_or_init(HostClock::local)
}

/// `2026-08-01` — the host-local calendar date, deliberately unlabelled.
pub(crate) fn date(timestamp: i64) -> Option<String> {
    host().date(timestamp)
}

/// `2026-08-01 16:47 PDT`.
pub(crate) fn stamp(timestamp: i64) -> Option<String> {
    host().stamp(timestamp)
}

/// `2026-08-01 16:47 PDT` from a millisecond instant.
pub(crate) fn stamp_millis(millis: i64) -> Option<String> {
    host().stamp_millis(millis)
}

/// `2026-08-01 16:47:03 PDT`, for surfaces where second-level precision earns
/// its width (progress logs, message streams, forensic records).
pub(crate) fn stamp_with_seconds(timestamp: i64) -> Option<String> {
    host().stamp_with_seconds(timestamp)
}

/// `16:47 PDT`, or `2026-08-02 00:15 PDT` on the first stamp after the local
/// day turns over. See [`HostClock::turn_stamp`].
pub(crate) fn turn_stamp(
    timestamp: i64,
    previous_local_date: Option<chrono::NaiveDate>,
) -> Option<(String, chrono::NaiveDate)> {
    host().turn_stamp(timestamp, previous_local_date)
}

/// `2026-08-01 16:47:03 PDT` from a millisecond instant.
pub(crate) fn stamp_millis_with_seconds(millis: i64) -> Option<String> {
    host().stamp_millis_with_seconds(millis)
}

/// `3h 12m ago (2026-08-01 16:47 PDT)` — how a *skimmed* surface states an
/// instant.
///
/// Relative age leads because it is what a reader of a corpus actually wants:
/// whether a post is an hour old or a month old should not cost arithmetic on
/// an epoch. The labelled absolute stamp rides in parentheses rather than
/// replacing it, because a relative age alone is a second, unfalsifiable clock
/// — it cannot be lined up against a log line or another surface. Both halves
/// come off the one host clock this module enforces, so the parenthesized
/// anchor reads identically to [`stamp`] printed anywhere else.
///
/// An instant still ahead — a grant's expiry — reads `in 2h 5m (…)`. "ago" on a
/// deadline is worse than the raw epoch was.
///
/// Rendered Markdown only. `format=json` projections keep the epoch, because a
/// machine consumer wants the instant, not a reading of it.
pub(crate) fn age(timestamp: i64) -> String {
    host().age(timestamp, Utc::now().timestamp())
}

/// [`age`] from a millisecond instant, mirroring the [`stamp`]/[`stamp_millis`]
/// pair so a caller never rescales at the call site and guesses wrong.
pub(crate) fn age_millis(millis: i64) -> String {
    host().age_millis(millis, Utc::now().timestamp_millis())
}

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

    fn at(&self, timestamp: i64) -> Option<DateTime<Tz>> {
        Some(DateTime::from_timestamp(timestamp, 0)?.with_timezone(&self.timezone))
    }

    fn at_millis(&self, millis: i64) -> Option<DateTime<Tz>> {
        Some(DateTime::from_timestamp_millis(millis)?.with_timezone(&self.timezone))
    }

    /// The host-local calendar day of an instant, for a caller that needs to
    /// compare days rather than print one — notably to seed a rolling stamp.
    pub(crate) fn local_date(&self, timestamp: i64) -> Option<chrono::NaiveDate> {
        Some(self.at(timestamp)?.date_naive())
    }

    pub(crate) fn date(&self, timestamp: i64) -> Option<String> {
        Some(self.at(timestamp)?.format("%Y-%m-%d").to_string())
    }

    pub(crate) fn stamp(&self, timestamp: i64) -> Option<String> {
        Some(self.at(timestamp)?.format("%Y-%m-%d %H:%M %Z").to_string())
    }

    pub(crate) fn stamp_millis(&self, millis: i64) -> Option<String> {
        Some(
            self.at_millis(millis)?
                .format("%Y-%m-%d %H:%M %Z")
                .to_string(),
        )
    }

    pub(crate) fn stamp_with_seconds(&self, timestamp: i64) -> Option<String> {
        Some(
            self.at(timestamp)?
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
        )
    }

    pub(crate) fn stamp_millis_with_seconds(&self, millis: i64) -> Option<String> {
        Some(
            self.at_millis(millis)?
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
        )
    }

    /// The stamp that opens a fresh session.
    ///
    /// This one is the anchor, so it is the richest form: it names the zone the
    /// way every other stamp does (`PDT`) and then spells that abbreviation out
    /// in full (`America/Los_Angeles, UTC-7`). An agent reading it once knows
    /// what the bare `PDT` on every later turn header means.
    pub(crate) fn initial_turn_prefix(&self, now: DateTime<Utc>) -> String {
        let local = now.with_timezone(&self.timezone);
        let offset_seconds = local.offset().fix().local_minus_utc();
        format!(
            "[{} ({}, {})]",
            local.format("%a %Y-%m-%d %H:%M %Z"),
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
                local.format("%a %H:%M %Z"),
                format_elapsed(seconds)
            ),
            // Sub-minute gaps are intentionally omitted: second-level precision is
            // noise at a turn boundary and would imply more accuracy than agents need.
            None => format!("[{} — resumed]", local.format("%a %H:%M %Z")),
        }
    }

    /// A stamp for a stream of instants rendered in order, which states its date
    /// only when the local calendar day has turned over since the one before it.
    ///
    /// A surface that heads itself with a date and then prints bare times is
    /// making a claim it cannot keep once the stream crosses local midnight: the
    /// reader associates every later time with the heading's day. Carrying the
    /// previous rendered date through the loop is what makes each stamp
    /// absolute, and seeding it from the heading makes the heading honest too.
    ///
    /// The caller feeds back the returned date on the next call. `None` means
    /// there is nothing to differ from, so the stamp renders bare.
    fn rolling_stamp(
        &self,
        timestamp: i64,
        previous_local_date: Option<chrono::NaiveDate>,
        same_day: &str,
        new_day: &str,
    ) -> Option<(String, chrono::NaiveDate)> {
        let local = self.at(timestamp)?;
        let date = local.date_naive();
        let rolled_over = previous_local_date.is_some_and(|previous| previous != date);
        let stamp = local
            .format(if rolled_over { new_day } else { same_day })
            .to_string();
        Some((stamp, date))
    }

    /// Second precision, for message catch-up digests.
    pub(crate) fn message_stamp(
        &self,
        timestamp: i64,
        previous_local_date: Option<chrono::NaiveDate>,
    ) -> Option<(String, chrono::NaiveDate)> {
        self.rolling_stamp(
            timestamp,
            previous_local_date,
            "%H:%M:%S %Z",
            "%Y-%m-%d %H:%M:%S %Z",
        )
    }

    /// See [`age`]. `now` is a parameter rather than a call to the process clock
    /// so the rendered format can be pinned in a test without a clock seam.
    pub(crate) fn age(&self, timestamp: i64, now: i64) -> String {
        match self.stamp(timestamp) {
            Some(stamp) => format!("{} ({stamp})", relative(now.saturating_sub(timestamp))),
            // An instant chrono cannot place is a broken row, not a time. Saying
            // so beats printing the number back, which only looks like data.
            None => UNPLACEABLE_INSTANT.to_string(),
        }
    }

    /// See [`age_millis`]. The relative half still reasons in seconds: the
    /// coarse `1d 3h 12m` vocabulary has no sub-minute term to spend the extra
    /// precision on.
    pub(crate) fn age_millis(&self, millis: i64, now_millis: i64) -> String {
        match self.stamp_millis(millis) {
            Some(stamp) => format!(
                "{} ({stamp})",
                relative(now_millis.saturating_sub(millis) / 1_000)
            ),
            None => UNPLACEABLE_INSTANT.to_string(),
        }
    }

    /// Minute precision, for transcript turn headers.
    pub(crate) fn turn_stamp(
        &self,
        timestamp: i64,
        previous_local_date: Option<chrono::NaiveDate>,
    ) -> Option<(String, chrono::NaiveDate)> {
        self.rolling_stamp(
            timestamp,
            previous_local_date,
            "%H:%M %Z",
            "%Y-%m-%d %H:%M %Z",
        )
    }
}

/// What a renderer says instead of a time when the stored value is not one.
const UNPLACEABLE_INSTANT: &str = "unknown time";

/// The relative half of [`HostClock::age`]: `3h 12m ago`, or `in 3h 12m` when
/// the instant has not arrived yet.
///
/// The span itself goes through [`format_elapsed`] rather than growing a
/// vocabulary of its own, so a post's age and a resume header's gap describe
/// the same duration with the same words. That inherits `format_elapsed`'s
/// floor: anything under a minute reads `0m`, which the absolute anchor beside
/// it disambiguates.
fn relative(elapsed_seconds: i64) -> String {
    if elapsed_seconds < 0 {
        format!("in {}", format_elapsed(elapsed_seconds.saturating_neg()))
    } else {
        format!("{} ago", format_elapsed(elapsed_seconds))
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

/// A span of seconds as the coarse `1d 3h 12m` form every agent-facing surface
/// uses for elapsed time. One formatter, so a resume header and a child's
/// last-activity stamp never disagree about what "3h" looks like.
pub(crate) fn format_elapsed(seconds: i64) -> String {
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
    fn pins_initial_turn_and_resume_formats() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let now = utc(1_752_381_600); // 2025-07-12 21:40 PDT
        assert_eq!(
            clock.initial_turn_prefix(now),
            "[Sat 2025-07-12 21:40 PDT (America/Los_Angeles, UTC-7)]"
        );
        assert_eq!(
            clock.resume_prefix(now, Some(now.timestamp() - (3 * 3600 + 12 * 60))),
            "[Sat 21:40 PDT — resumed after 3h 12m]"
        );
        assert_eq!(
            clock.resume_prefix(now, Some(now.timestamp() - 59)),
            "[Sat 21:40 PDT — resumed]"
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
        assert_eq!(first_stamp, "23:19:59 PDT");
        assert_eq!(second_stamp, "23:20:01 PDT");
        assert_eq!(next_stamp, "2025-07-13 00:20:00 PDT");
    }

    /// Every renderer names its zone, and every renderer names the SAME instant:
    /// a reader comparing a turn header against a resume marker against an issue
    /// header is comparing one clock, not three.
    #[test]
    fn every_renderer_labels_its_zone_and_agrees_on_the_instant() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let at = 1_752_381_600; // 2025-07-12 21:40:00 PDT / 2025-07-13 04:40 UTC
        assert_eq!(clock.stamp(at).unwrap(), "2025-07-12 21:40 PDT");
        assert_eq!(
            clock.stamp_millis(at * 1000).unwrap(),
            "2025-07-12 21:40 PDT"
        );
        assert_eq!(
            clock.stamp_with_seconds(at).unwrap(),
            "2025-07-12 21:40:00 PDT"
        );
        assert_eq!(clock.turn_stamp(at, None).unwrap().0, "21:40 PDT");
        assert_eq!(
            clock.stamp_millis_with_seconds(at * 1000).unwrap(),
            "2025-07-12 21:40:00 PDT"
        );
        // The date is the LOCAL calendar day, not the UTC one it would have been
        // read as before — that day-boundary disagreement is the bug.
        assert_eq!(clock.date(at).unwrap(), "2025-07-12");
    }

    /// A digest that runs past local midnight cannot leave its later turns
    /// leaning on the date in its heading — the first turn on the new day states
    /// that day itself.
    #[test]
    fn turn_stamp_adds_the_date_on_the_first_turn_of_a_new_local_day() {
        let clock = HostClock::fixed("America/Los_Angeles");
        // 2025-07-14 00:00 PDT is 1_752_476_400.
        let before_midnight = 1_752_474_000; // 2025-07-13 23:20 PDT
        let after_midnight = 1_752_479_100; // 2025-07-14 00:45 PDT

        let (first, date) = clock.turn_stamp(before_midnight, None).unwrap();
        let (second, date) = clock.turn_stamp(before_midnight + 60, Some(date)).unwrap();
        let (third, date) = clock.turn_stamp(after_midnight, Some(date)).unwrap();
        let (fourth, _) = clock.turn_stamp(after_midnight + 60, Some(date)).unwrap();

        assert_eq!(first, "23:20 PDT");
        assert_eq!(second, "23:21 PDT");
        assert_eq!(third, "2025-07-14 00:45 PDT");
        assert_eq!(
            fourth, "00:46 PDT",
            "only the turn that crosses says the date"
        );
    }

    /// Latest-first ordering renders the same stream backwards, so the rule is
    /// "differs from the previously rendered stamp", not "is later than".
    #[test]
    fn turn_stamp_marks_the_rollover_when_the_stream_runs_backwards() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let (newest, date) = clock.turn_stamp(1_752_479_100, None).unwrap();
        let (older, _) = clock.turn_stamp(1_752_474_000, Some(date)).unwrap();
        assert_eq!(newest, "00:45 PDT");
        assert_eq!(older, "2025-07-13 23:20 PDT");
    }

    /// The CAIRN-3428 specimen. A transcript turn header and a resume marker
    /// rendered for the same instant used to disagree by seven hours, and
    /// neither named the clock that would have explained the gap. Both come off
    /// this one clock now, so they read the same hour and both say so.
    #[test]
    fn a_turn_header_and_a_resume_marker_read_the_same_hour() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let at = 1_752_381_600;
        let (turn_header, _) = clock.turn_stamp(at, None).unwrap();
        assert_eq!(turn_header, "21:40 PDT");
        assert!(
            clock.resume_prefix(utc(at), None).contains(&turn_header),
            "the resume marker reads the same labelled hour as the turn header"
        );
    }

    /// The CAIRN-4233 specimen. Every posts surface printed `Created: 1786996108`,
    /// so a reader skimming a corpus could not tell an hour-old post from a
    /// month-old one without doing arithmetic. Relative age leads now, and the
    /// labelled stamp stays beside it so the reading is still anchored.
    #[test]
    fn age_leads_with_the_span_and_keeps_the_labelled_anchor() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let at = 1_752_381_600; // 2025-07-12 21:40 PDT
        assert_eq!(
            clock.age(at, at + 3 * 3600 + 12 * 60),
            "3h 12m ago (2025-07-12 21:40 PDT)"
        );
        assert_eq!(
            clock.age(at, at + 40 * 86400),
            "40d ago (2025-07-12 21:40 PDT)"
        );
    }

    /// A grant's expiry is the case that makes "ago" actively wrong, so a future
    /// instant states that it is one.
    #[test]
    fn a_future_instant_reads_forward_rather_than_ago() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let at = 1_752_381_600;
        assert_eq!(
            clock.age(at, at - (2 * 3600 + 5 * 60)),
            "in 2h 5m (2025-07-12 21:40 PDT)"
        );
    }

    /// The millisecond entry point names the same instant as the second one, so
    /// a caller picking the wrong scale is a compile-time choice and not a
    /// silently wrong reading.
    #[test]
    fn age_millis_agrees_with_age_on_the_same_instant() {
        let clock = HostClock::fixed("America/Los_Angeles");
        let at = 1_752_381_600;
        let now = at + 7200;
        assert_eq!(
            clock.age_millis(at * 1000, now * 1000),
            clock.age(at, now),
            "2h ago (2025-07-12 21:40 PDT)"
        );
    }

    /// The one thing this must never do is print the epoch back: a value chrono
    /// cannot place is a broken row, and rendering it as a number would restore
    /// exactly the wart being removed.
    #[test]
    fn an_unplaceable_instant_says_so_instead_of_printing_its_epoch() {
        let clock = HostClock::fixed("America/Los_Angeles");
        assert_eq!(clock.age(i64::MAX, 0), "unknown time");
        assert_eq!(clock.age_millis(i64::MAX, 0), "unknown time");
    }

    #[test]
    fn winter_instants_carry_the_standard_time_abbreviation() {
        let clock = HostClock::fixed("America/Los_Angeles");
        // 2025-01-15 12:12 PST — the operator's own example of a labelled stamp.
        assert_eq!(clock.stamp(1_736_971_920).unwrap(), "2025-01-15 12:12 PST");
    }

    #[test]
    fn a_utc_host_says_utc() {
        let clock = HostClock::fixed("UTC");
        assert_eq!(clock.stamp(1_752_381_600).unwrap(), "2025-07-13 04:40 UTC");
    }

    /// A zone with no abbreviation of its own still gets a label: chrono-tz
    /// falls back to the numeric offset, which is self-describing.
    #[test]
    fn a_zone_without_an_abbreviation_labels_with_its_offset() {
        let clock = HostClock::fixed("Asia/Dubai");
        assert_eq!(clock.stamp(1_752_381_600).unwrap(), "2025-07-13 08:40 +04");
    }

    #[test]
    fn out_of_range_instants_render_nothing_rather_than_a_wrong_time() {
        let clock = HostClock::fixed("America/Los_Angeles");
        assert_eq!(clock.stamp(i64::MAX), None);
        assert_eq!(clock.stamp_millis_with_seconds(i64::MAX), None);
    }
}
