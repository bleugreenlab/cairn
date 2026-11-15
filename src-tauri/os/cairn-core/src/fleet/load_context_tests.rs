use super::*;
use cairn_common::executor_protocol::{HostHealth, IoPressure};
use cairn_common::wire_vocabulary::{Measurement, MeasurementGap};

/// A forecast sees the candidate's advertised storage pressure, and a
/// machine that cannot measure it is unknown there, never quiet.
#[test]
fn a_candidate_load_carries_the_advertised_io_pressure_or_none() {
    let measured = HostHealth {
        io_pressure: Measurement::measured(
            1,
            IoPressure {
                build_device: None,
                full_avg10_millis: 235,
                full_avg60_millis: 190,
                slowest_device: None,
            },
        ),
        ..HostHealth::default()
    };
    assert_eq!(
        load_context(&FleetSnapshot::default(), &measured).io_pressure_full_millis,
        Some(235)
    );

    let unmeasured = HostHealth {
        io_pressure: Measurement::unavailable(1, MeasurementGap::UnsupportedPlatform),
        ..HostHealth::default()
    };
    assert_eq!(
        load_context(&FleetSnapshot::default(), &unmeasured).io_pressure_full_millis,
        None
    );
    assert_eq!(
        load_context(&FleetSnapshot::default(), &HostHealth::default()).io_pressure_full_millis,
        None,
        "an executor too old to report it"
    );
}
