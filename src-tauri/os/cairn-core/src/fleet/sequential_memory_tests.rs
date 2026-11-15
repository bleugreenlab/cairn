use super::*;
use cairn_common::executor_protocol::BuildJobMemoryEvidence;

fn member(largest: u64, typical: u64, whole: u64) -> ResourceReservation {
    ResourceReservation {
        memory_bytes: whole,
        source: ResourceReservationSource::Learned,
        build_job_memory: Some(BuildJobMemoryEvidence {
            largest_bytes: largest,
            typical_bytes: typical,
            samples: 1,
        }),
        ..ResourceReservation::default()
    }
}

#[test]
fn sequential_envelope_uses_a_real_dominating_member_at_every_width() {
    let members = [member(15, 3, 100), member(12, 2, 80), member(10, 1, 50)];
    let group = sequential_memory_envelope(&members, 32);
    assert_eq!(group.build_job_memory, members[0].build_job_memory);
    for width in 1..=32 {
        assert_eq!(
            group.memory_charge_at(Some(width)),
            members
                .iter()
                .map(|member| member.memory_charge_at(Some(width)))
                .max()
                .unwrap()
        );
    }
    assert_eq!(group.test_phase, None);
}

#[test]
fn crossing_or_unknown_members_keep_a_fixed_conservative_envelope() {
    let curves = [member(20, 1, 100), member(10, 3, 100)];
    let group = sequential_memory_envelope(&curves, 20);
    assert_eq!(group.build_job_memory, None);
    assert_eq!(group.memory_bytes, 67);
    for width in 1..=20 {
        assert!(curves
            .iter()
            .all(|member| group.memory_charge_bytes() >= member.memory_charge_at(Some(width))));
    }
    let mixed = [
        member(20, 1, 100),
        ResourceReservation {
            memory_bytes: 50,
            ..ResourceReservation::default()
        },
    ];
    let group = sequential_memory_envelope(&mixed, 20);
    assert_eq!(group.build_job_memory, None);
    assert_eq!(group.source, ResourceReservationSource::Unmeasured);
    assert_eq!(group.memory_charge_bytes(), 50);
    let mut prior = member(20, 3, 30);
    prior.source = ResourceReservationSource::Unmeasured;
    let group = sequential_memory_envelope(
        &[
            prior,
            ResourceReservation {
                memory_bytes: 5,
                ..ResourceReservation::default()
            },
        ],
        20,
    );
    assert_eq!(group.build_job_memory, None);
    assert_eq!(group.memory_charge_bytes(), 77);
    assert_eq!(
        sequential_memory_envelope(&[], 20),
        ResourceReservation::default()
    );
}
