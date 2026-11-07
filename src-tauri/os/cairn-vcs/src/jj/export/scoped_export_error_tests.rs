use super::*;
use jj_lib::git::FailedRefExportReason;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
struct WrappedError {
    message: &'static str,
    cause: std::io::Error,
}
impl fmt::Display for WrappedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}
impl Error for WrappedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

#[test]
fn scoped_export_preserves_typed_lock_sources_without_broadening_contention() {
    let locked = FailedRefExportReason::FailedToSet(Box::new(WrappedError {
        message: "The lock for the packed-ref file could not be obtained",
        cause: std::io::Error::new(std::io::ErrorKind::AlreadyExists, "packed-refs.lock exists"),
    }));
    let error = export_error_chain(&locked);
    assert!(error.contains("Failed to set"), "{error}");
    assert!(packed_refs_lock_error(&error), "{error}");
    let denied = FailedRefExportReason::FailedToSet(Box::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "cannot write packed-refs",
    )));
    assert!(!packed_refs_lock_error(&export_error_chain(&denied)));
    assert!(!packed_refs_lock_error(&export_error_chain(
        &FailedRefExportReason::ModifiedInJjDeletedInGit
    )));
}
