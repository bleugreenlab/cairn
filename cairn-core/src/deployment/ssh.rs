//! SSH command execution via subprocess.

use std::process::Command;

/// Configuration for connecting to a remote host via SSH.
pub struct SshConfig {
    pub host: String,
    pub port: i32,
    pub user: String,
    pub key_path: Option<String>,
}

impl SshConfig {
    /// Build the base SSH command with standard options.
    fn base_command(&self) -> Command {
        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("StrictHostKeyChecking=accept-new")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg("-p")
            .arg(self.port.to_string());

        if let Some(key) = &self.key_path {
            cmd.arg("-i").arg(key);
        }

        cmd.arg(format!("{}@{}", self.user, self.host));
        cmd
    }

    /// Execute a command on the remote host. Returns stdout on success.
    pub fn exec(&self, command: &str) -> Result<String, String> {
        let mut cmd = self.base_command();
        cmd.arg(command);

        let output = cmd.output().map_err(|e| format!("SSH failed: {}", e))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("SSH command failed: {}", stderr))
        }
    }

    /// Execute a command and return output regardless of exit code.
    /// Returns (success, stdout, stderr).
    pub fn exec_raw(&self, command: &str) -> Result<(bool, String, String), String> {
        let mut cmd = self.base_command();
        cmd.arg(command);

        let output = cmd.output().map_err(|e| format!("SSH failed: {}", e))?;

        Ok((
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        ))
    }
}
