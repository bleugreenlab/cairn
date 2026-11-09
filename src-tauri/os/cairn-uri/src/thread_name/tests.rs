use super::*;

#[test]
fn validates_thread_names() {
    for valid in ["roadmap", "roadmap-2", "2-roadmap"] {
        assert!(validate_thread_name(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "", "Roadmap", "road_map", "123", "-", "-roadmap", "roadmap-",
    ] {
        assert!(validate_thread_name(invalid).is_err(), "{invalid}");
    }
    for reserved in RESERVED_PROJECT_SEGMENTS {
        assert!(validate_thread_name(reserved).is_err(), "{reserved}");
    }
}
