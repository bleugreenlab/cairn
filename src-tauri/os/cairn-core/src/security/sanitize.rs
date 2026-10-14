pub use cairn_common::security::sanitize::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// The redaction policy in force for `config_dir`, rebuilt only when the
/// settings file changes.
///
/// Building a policy reads and YAML-parses the whole settings file and
/// re-normalizes every sensitive name. That cost belongs to a settings edit,
/// not to a captured request, so the built policy is cached behind the settings
/// file's modification stamp — which, unlike a save hook, also notices an edit
/// made outside this process. The cache holds a single entry because a process
/// serves exactly one config directory; a second directory (only tests do this)
/// simply rebuilds.
pub fn redaction_policy(config_dir: &Path) -> Arc<RedactionPolicy> {
    let stamp = crate::config::settings::settings_file_stamp(config_dir);
    let mut cache = REDACTION_POLICY
        .lock()
        .expect("redaction policy cache poisoned");
    if let Some(cached) = cache.as_ref() {
        if cached.config_dir == config_dir && cached.stamp == stamp {
            return Arc::clone(&cached.policy);
        }
    }
    let policy = Arc::new(RedactionPolicy::new(
        crate::config::settings::load_browser_network_sensitive_names(config_dir),
    ));
    *cache = Some(CachedRedactionPolicy {
        config_dir: config_dir.to_path_buf(),
        stamp,
        policy: Arc::clone(&policy),
    });
    policy
}

static REDACTION_POLICY: Mutex<Option<CachedRedactionPolicy>> = Mutex::new(None);

struct CachedRedactionPolicy {
    config_dir: PathBuf,
    stamp: Option<(SystemTime, u64)>,
    policy: Arc<RedactionPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redaction_policy_is_reused_until_the_settings_file_changes() {
        let config_dir = tempfile::tempdir().unwrap();
        let settings = config_dir.path().join("settings.yaml");
        std::fs::write(&settings, "browserNetworkSensitiveNames:\n  - x-tenant\n").unwrap();

        let first = redaction_policy(config_dir.path());
        let second = redaction_policy(config_dir.path());
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged settings file must not rebuild the policy"
        );
        assert!(first.is_sensitive("x-tenant"));

        // An edit made OUTSIDE this process is still picked up, because the
        // cache keys on the file's modification stamp rather than on a save
        // hook. The replacement is the same length as the original, so only the
        // modification time can distinguish the two.
        std::fs::write(&settings, "browserNetworkSensitiveNames:\n  - x-region\n").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&settings)
            .unwrap()
            .set_modified(SystemTime::now() + std::time::Duration::from_secs(5))
            .unwrap();

        let rebuilt = redaction_policy(config_dir.path());
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert!(rebuilt.is_sensitive("x-region"));
        assert!(!rebuilt.is_sensitive("x-tenant"));
    }
}
