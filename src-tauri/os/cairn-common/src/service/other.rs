//! Fallback service backend for platforms without a supported per-user service
//! manager. The daemon still runs when launched directly; only the OS-service
//! lifecycle is unavailable.

use std::path::PathBuf;

use super::ServiceSpec;

pub fn install(_spec: &ServiceSpec) -> Result<(), String> {
    Err("service install is unsupported on this platform".to_string())
}

pub fn uninstall(_spec: &ServiceSpec) -> Result<(), String> {
    Err("service uninstall is unsupported on this platform".to_string())
}

pub fn restart(_spec: &ServiceSpec) -> Result<(), String> {
    Err("service restart is unsupported on this platform".to_string())
}

pub fn is_installed(_spec: &ServiceSpec) -> bool {
    false
}

pub fn is_loaded(_spec: &ServiceSpec) -> bool {
    false
}

pub fn location(_spec: &ServiceSpec) -> Option<PathBuf> {
    None
}

pub fn runtime(_spec: &ServiceSpec) -> Option<super::ServiceRuntime> {
    None
}

pub fn recent_log(_spec: &ServiceSpec, _lines: usize) -> Option<String> {
    None
}
