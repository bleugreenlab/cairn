//! Docker operations executed over SSH.

use super::ssh::SshConfig;

const DOCKER_IMAGE: &str = "ghcr.io/cairn-dev/cairn-server:latest";

/// Verify Docker is installed and accessible on the remote host.
pub fn check_docker(ssh: &SshConfig) -> Result<(), String> {
    ssh.exec("docker info > /dev/null 2>&1")
        .map(|_| ())
        .map_err(|_| "Docker is not installed or not accessible on the remote host".to_string())
}

/// Pull the cairn-server Docker image.
pub fn pull_image(ssh: &SshConfig) -> Result<(), String> {
    ssh.exec(&format!("docker pull {}", DOCKER_IMAGE))
        .map(|_| ())
}

/// Start a new container. Returns the container ID.
pub fn start_container(
    ssh: &SshConfig,
    container_name: &str,
    api_key: &str,
    server_port: i32,
) -> Result<String, String> {
    let cmd = format!(
        "docker run -d --name {} \
         --init \
         -e CAIRN_API_KEY={} \
         -e CAIRN_HOST=0.0.0.0 \
         -e CAIRN_DATA_DIR=/data \
         -p {}:8080 \
         -v cairn-data-{}:/data \
         -v cairn-projects-{}:/projects \
         --restart unless-stopped \
         {}",
        shell_escape(container_name),
        shell_escape(api_key),
        server_port,
        shell_escape(container_name),
        shell_escape(container_name),
        DOCKER_IMAGE,
    );
    let output = ssh.exec(&cmd)?;
    Ok(output.trim().to_string())
}

/// Stop a running container.
pub fn stop_container(ssh: &SshConfig, container_name: &str) -> Result<(), String> {
    ssh.exec(&format!("docker stop {}", shell_escape(container_name)))
        .map(|_| ())
}

/// Start an existing stopped container.
pub fn start_existing_container(ssh: &SshConfig, container_name: &str) -> Result<(), String> {
    ssh.exec(&format!("docker start {}", shell_escape(container_name)))
        .map(|_| ())
}

/// Restart a container.
pub fn restart_container(ssh: &SshConfig, container_name: &str) -> Result<(), String> {
    ssh.exec(&format!("docker restart {}", shell_escape(container_name)))
        .map(|_| ())
}

/// Remove a container (force stop + remove).
pub fn remove_container(ssh: &SshConfig, container_name: &str) -> Result<(), String> {
    ssh.exec(&format!("docker rm -f {}", shell_escape(container_name)))
        .map(|_| ())
}

/// Get container logs.
pub fn get_logs(ssh: &SshConfig, container_name: &str, tail: u32) -> Result<String, String> {
    ssh.exec(&format!(
        "docker logs --tail {} {}",
        tail,
        shell_escape(container_name)
    ))
}

/// Execute a command inside a running container.
pub fn exec_in_container(
    ssh: &SshConfig,
    container_name: &str,
    command: &str,
) -> Result<String, String> {
    ssh.exec(&format!(
        "docker exec {} {}",
        shell_escape(container_name),
        command,
    ))
}

/// Check if a container is currently running.
pub fn is_container_running(ssh: &SshConfig, container_name: &str) -> Result<bool, String> {
    let (success, stdout, _) = ssh.exec_raw(&format!(
        "docker inspect -f '{{{{.State.Running}}}}' {}",
        shell_escape(container_name)
    ))?;
    Ok(success && stdout.trim() == "true")
}

/// Minimal shell escaping — wraps value in single quotes, escaping internal single quotes.
fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
