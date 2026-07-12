use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cairn_common::executor_protocol::{
    BuildSlotRequest, DeltaUploadReceipt, ObjectTransferCoordinate,
};
use sha2::{Digest, Sha256};

const CREDENTIAL_TTL_MS: u64 = 15 * 60 * 1000;
const STAGED_UPLOAD_TTL_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Debug)]
struct ObjectCredentialSession {
    device_id: String,
    runner_device_id: String,
    generation: u64,
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
    previous_token: Option<PreviousObjectCredential>,
}

#[derive(Clone, Debug)]
struct PreviousObjectCredential {
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
}

#[derive(Clone, Debug)]
pub struct AuthenticatedObjectSession {
    pub executor_id: String,
    pub device_id: String,
    pub runner_device_id: String,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub struct StagedDeltaUpload {
    pub receipt: DeltaUploadReceipt,
    pub path: PathBuf,
    staged_at_unix_ms: u64,
}

#[derive(Clone, Default)]
pub struct ObjectPlaneState {
    sessions: Arc<Mutex<HashMap<String, ObjectCredentialSession>>>,
    authorized: Arc<Mutex<HashMap<(String, String), AuthorizedTransfer>>>,
    staged: Arc<Mutex<HashMap<String, StagedDeltaUpload>>>,
}

#[derive(Clone, Debug)]
struct AuthorizedTransfer {
    coordinate: ObjectTransferCoordinate,
    base_commit: String,
}

impl ObjectPlaneState {
    pub fn issue_credential(
        &self,
        executor_id: &str,
        device_id: &str,
        runner_device_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> (String, u64) {
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let expires_at_unix_ms = now_unix_ms.saturating_add(CREDENTIAL_TTL_MS);
        let mut sessions = self.sessions.lock().unwrap();
        let previous_token = sessions.get(executor_id).and_then(|session| {
            (session.generation == generation && session.expires_at_unix_ms >= now_unix_ms)
                .then_some(PreviousObjectCredential {
                    token_hash: session.token_hash,
                    expires_at_unix_ms: session.expires_at_unix_ms,
                })
        });
        sessions.insert(
            executor_id.to_owned(),
            ObjectCredentialSession {
                device_id: device_id.to_owned(),
                runner_device_id: runner_device_id.to_owned(),
                generation,
                token_hash: hash_bytes(token.as_bytes()),
                expires_at_unix_ms,
                previous_token,
            },
        );
        (token, expires_at_unix_ms)
    }

    pub fn revoke(&self, executor_id: &str, generation: u64) {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions
            .get(executor_id)
            .is_some_and(|session| session.generation == generation)
        {
            sessions.remove(executor_id);
            self.authorized.lock().unwrap().retain(|_, transfer| {
                transfer.coordinate.executor_id != executor_id
                    || transfer.coordinate.connection_generation != generation
            });
            let abandoned = {
                let mut staged = self.staged.lock().unwrap();
                let ids = staged
                    .iter()
                    .filter(|(_, upload)| {
                        upload.receipt.coordinate.executor_id == executor_id
                            && upload.receipt.coordinate.connection_generation == generation
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                ids.into_iter()
                    .filter_map(|id| staged.remove(&id))
                    .collect::<Vec<_>>()
            };
            for upload in abandoned {
                let _ = std::fs::remove_file(upload.path);
            }
        }
    }

    pub fn authorize_request(
        &self,
        request: &BuildSlotRequest,
        executor_id: &str,
        generation: u64,
    ) {
        let coordinate = ObjectTransferCoordinate {
            repository: request.repository.identity(),
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            executor_id: executor_id.to_owned(),
            connection_generation: generation,
        };
        self.authorized.lock().unwrap().insert(
            (request.request_id.clone(), request.attempt_id.clone()),
            AuthorizedTransfer {
                coordinate,
                base_commit: request.base_commit.clone(),
            },
        );
    }

    pub fn revoke_request(
        &self,
        request_id: &str,
        attempt_id: &str,
        executor_id: &str,
        generation: u64,
    ) {
        let key = (request_id.to_owned(), attempt_id.to_owned());
        let mut authorized = self.authorized.lock().unwrap();
        if authorized.get(&key).is_some_and(|transfer| {
            transfer.coordinate.executor_id == executor_id
                && transfer.coordinate.connection_generation == generation
        }) {
            authorized.remove(&key);
        }
    }

    pub fn authorizes(&self, coordinate: &ObjectTransferCoordinate, base_commit: &str) -> bool {
        self.authorized
            .lock()
            .unwrap()
            .get(&(coordinate.request_id.clone(), coordinate.attempt_id.clone()))
            .is_some_and(|transfer| {
                transfer.coordinate == *coordinate && transfer.base_commit == base_commit
            })
    }

    pub fn authenticate(
        &self,
        token: &str,
        now_unix_ms: u64,
    ) -> Option<AuthenticatedObjectSession> {
        let candidate = hash_bytes(token.as_bytes());
        let sessions = self.sessions.lock().unwrap();
        let (executor_id, session) = sessions.iter().find(|(_, session)| {
            let current = session.expires_at_unix_ms >= now_unix_ms
                && constant_time_eq(&candidate, &session.token_hash);
            let previous = session.previous_token.as_ref().is_some_and(|previous| {
                previous.expires_at_unix_ms >= now_unix_ms
                    && constant_time_eq(&candidate, &previous.token_hash)
            });
            current || previous
        })?;
        Some(AuthenticatedObjectSession {
            executor_id: executor_id.clone(),
            device_id: session.device_id.clone(),
            runner_device_id: session.runner_device_id.clone(),
            generation: session.generation,
        })
    }

    pub fn stage_delta(
        &self,
        staging_dir: &Path,
        receipt: DeltaUploadReceipt,
        pack: &[u8],
    ) -> Result<StagedDeltaUpload, String> {
        self.cleanup_expired_staged(unix_time_ms());
        let mut staged = self.staged.lock().unwrap();
        if let Some(existing) = staged.get(&receipt.receipt_id) {
            if existing.receipt == receipt {
                return Ok(existing.clone());
            }
            return Err("receipt id is already bound to different content".into());
        }
        std::fs::create_dir_all(staging_dir)
            .map_err(|error| format!("creating object upload staging directory: {error}"))?;
        let final_path = staging_dir.join(format!("{}.pack", receipt.receipt_id));
        let temp_path = staging_dir.join(format!(
            ".{}.{}.tmp",
            receipt.receipt_id,
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&temp_path, pack)
            .map_err(|error| format!("writing staged delta pack: {error}"))?;
        std::fs::rename(&temp_path, &final_path)
            .map_err(|error| format!("publishing staged delta pack: {error}"))?;
        let upload = StagedDeltaUpload {
            receipt,
            path: final_path,
            staged_at_unix_ms: unix_time_ms(),
        };
        staged.insert(upload.receipt.receipt_id.clone(), upload.clone());
        Ok(upload)
    }

    /// Resolve a staged upload only while its executor generation is still the
    /// live authenticated session and every receipt field matches. The locked
    /// commit-barrier consumer uses this instead of trusting executor JSON.
    pub fn staged_delta(&self, receipt: &DeltaUploadReceipt) -> Option<StagedDeltaUpload> {
        let session_is_current = self
            .sessions
            .lock()
            .unwrap()
            .get(&receipt.coordinate.executor_id)
            .is_some_and(|session| {
                session.generation == receipt.coordinate.connection_generation
                    && session.expires_at_unix_ms >= unix_time_ms()
            });
        if !session_is_current {
            return None;
        }
        self.staged
            .lock()
            .unwrap()
            .get(&receipt.receipt_id)
            .filter(|upload| upload.receipt == *receipt)
            .cloned()
    }

    pub fn consume_staged_delta(&self, receipt: &DeltaUploadReceipt) -> Result<(), String> {
        let upload = {
            let mut staged = self.staged.lock().unwrap();
            match staged.get(&receipt.receipt_id) {
                Some(upload) if upload.receipt == *receipt => staged.remove(&receipt.receipt_id),
                Some(_) => return Err("staged delta receipt content changed".into()),
                None => return Ok(()),
            }
        };
        if let Some(upload) = upload {
            match std::fs::remove_file(&upload.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("remove consumed staged delta pack: {error}")),
            }
        }
        Ok(())
    }

    pub fn cleanup_expired_staged(&self, now_unix_ms: u64) {
        let expired = {
            let mut staged = self.staged.lock().unwrap();
            let ids = staged
                .iter()
                .filter(|(_, upload)| {
                    upload
                        .staged_at_unix_ms
                        .saturating_add(STAGED_UPLOAD_TTL_MS)
                        < now_unix_ms
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| staged.remove(&id))
                .collect::<Vec<_>>()
        };
        for upload in expired {
            let _ = std::fs::remove_file(upload.path);
        }
    }
}

pub fn content_sha256(bytes: &[u8]) -> String {
    hex_hash(hash_bytes(bytes))
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex_hash(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{GitObjectFormat, RepositoryIdentity};

    fn coordinate(generation: u64) -> ObjectTransferCoordinate {
        ObjectTransferCoordinate {
            repository: RepositoryIdentity {
                project_id: "project".into(),
                repository_id: "repository".into(),
                object_format: GitObjectFormat::Sha1,
            },
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            executor_id: "executor".into(),
            connection_generation: generation,
        }
    }

    #[test]
    fn reconnect_invalidates_old_credential_and_disconnect_is_generation_guarded() {
        let state = ObjectPlaneState::default();
        let (old, _) = state.issue_credential("executor", "device", "runner", 1, 100);
        let (new, _) = state.issue_credential("executor", "device", "runner", 2, 100);
        assert!(state.authenticate(&old, 101).is_none());
        assert_eq!(state.authenticate(&new, 101).unwrap().generation, 2);
        state.revoke("executor", 1);
        assert!(state.authenticate(&new, 101).is_some());
        state.revoke("executor", 2);
        assert!(state.authenticate(&new, 101).is_none());
    }

    #[test]
    fn request_authorization_binds_every_coordinate_dimension() {
        use cairn_common::executor_protocol::{
            BuildSlotPriority, MutationPolicy, RepositoryLocator,
        };
        let state = ObjectPlaneState::default();
        let request = BuildSlotRequest {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            project_id: "project".into(),
            repository: RepositoryLocator::ManagedObjects {
                project_id: "project".into(),
                repository_id: "repository".into(),
                object_format: GitObjectFormat::Sha1,
            },
            base_commit: "a".repeat(40),
            command: "check".into(),
            cwd: String::new(),
            env: Vec::new(),
            priority: BuildSlotPriority::ReviewCheck,
            deadline_unix_ms: 1,
            timeout_ms: 1,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            constraints: None,
        };
        state.authorize_request(&request, "executor", 1);
        assert!(state.authorizes(&coordinate(1), &request.base_commit));
        assert!(!state.authorizes(&coordinate(2), &request.base_commit));
        assert!(!state.authorizes(&coordinate(1), &"b".repeat(40)));
        state.revoke_request("request", "attempt", "executor", 1);
        assert!(!state.authorizes(&coordinate(1), &request.base_commit));
    }

    #[test]
    fn credential_rotation_keeps_a_live_generation_authenticated_past_the_old_ttl() {
        let state = ObjectPlaneState::default();
        let (old, old_expires) = state.issue_credential("executor", "device", "runner", 1, 100);
        let (rotated, rotated_expires) = state.issue_credential(
            "executor",
            "device",
            "runner",
            1,
            old_expires.saturating_sub(1),
        );
        assert_eq!(
            state
                .authenticate(&old, old_expires.saturating_sub(1))
                .unwrap()
                .generation,
            1,
            "the previous token must remain valid while the update frame is in flight"
        );
        assert!(state
            .authenticate(&old, old_expires.saturating_add(1))
            .is_none());
        assert_eq!(
            state
                .authenticate(&rotated, old_expires.saturating_add(1))
                .unwrap()
                .generation,
            1
        );
        assert!(rotated_expires > old_expires);
    }

    #[test]
    fn staging_is_idempotent_only_for_the_same_receipt() {
        let state = ObjectPlaneState::default();
        let dir = tempfile::tempdir().unwrap();
        let receipt = DeltaUploadReceipt {
            receipt_id: "receipt".into(),
            coordinate: coordinate(1),
            base_commit: "a".repeat(40),
            delta_commit: "b".repeat(40),
            content_hash: content_sha256(b"pack"),
            pack_checksum: "c".repeat(40),
        };
        let first = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        let second = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        assert_eq!(first.path, second.path);
        assert!(state.staged_delta(&receipt).is_none());
        let (_token, _) = state.issue_credential("executor", "device", "runner", 1, unix_time_ms());
        assert_eq!(state.staged_delta(&receipt).unwrap().path, first.path);
        let (_replacement, _) =
            state.issue_credential("executor", "device", "runner", 2, unix_time_ms());
        assert!(state.staged_delta(&receipt).is_none());
        let mut conflicting = receipt;
        conflicting.content_hash = "different".into();
        assert!(state
            .stage_delta(dir.path(), conflicting, b"other")
            .is_err());
    }

    #[test]
    fn consumed_and_expired_staged_uploads_remove_their_files() {
        let state = ObjectPlaneState::default();
        let dir = tempfile::tempdir().unwrap();
        let receipt = DeltaUploadReceipt {
            receipt_id: "consumed".into(),
            coordinate: coordinate(1),
            base_commit: "a".repeat(40),
            delta_commit: "b".repeat(40),
            content_hash: content_sha256(b"pack"),
            pack_checksum: "c".repeat(40),
        };
        let upload = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        assert!(upload.path.is_file());
        state.consume_staged_delta(&receipt).unwrap();
        assert!(!upload.path.exists());
        assert!(state.staged.lock().unwrap().is_empty());

        let mut expired_receipt = receipt;
        expired_receipt.receipt_id = "expired".into();
        let expired = state
            .stage_delta(dir.path(), expired_receipt, b"pack")
            .unwrap();
        state.cleanup_expired_staged(
            expired
                .staged_at_unix_ms
                .saturating_add(STAGED_UPLOAD_TTL_MS)
                .saturating_add(1),
        );
        assert!(!expired.path.exists());
        assert!(state.staged.lock().unwrap().is_empty());
    }
}
