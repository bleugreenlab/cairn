use super::tests::{git, git_stdout, init_project, jj_bin};
use super::*;

/// Both application constructors must carry the lockfile policy through a real
/// export and checkout repair, including when the driver crosses a clone seam.
#[test]
#[serial_test::serial(jj)]
fn application_environments_repair_export_with_generated_lockfile_dirt() {
    let Some(bin) = jj_bin() else { return };
    for resolved in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        init_project(project.path());
        let lockfile = project.path().join("src-tauri/Cargo.lock");
        std::fs::create_dir_all(lockfile.parent().unwrap()).unwrap();
        std::fs::write(&lockfile, "committed\n").unwrap();
        git(project.path(), &["add", "src-tauri/Cargo.lock"]);
        git(project.path(), &["commit", "-qm", "lockfile"]);
        let jj = if resolved {
            environment(&bin, home.path())
        } else {
            environment_with_binary(&bin, home.path())
        }
        .clone();
        let store = home.path().join("store");
        ensure_project_store(&jj, &store, project.path()).unwrap();
        jj.run(&store, &["new", "main", "-m", "advance"], "test advance")
            .unwrap();
        jj.run(
            &store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                "@",
                "--ignore-working-copy",
            ],
            "test bookmark",
        )
        .unwrap();
        std::fs::write(&lockfile, "generated churn\n").unwrap();

        export_git_preserving_checkout(&jj, &store, true, "test export").unwrap();

        assert_eq!(
            git_stdout(project.path(), &["symbolic-ref", "HEAD"]),
            "refs/heads/main"
        );
        assert_eq!(std::fs::read_to_string(&lockfile).unwrap(), "committed\n");
        assert_eq!(
            git_stdout(project.path(), &["rev-parse", "HEAD"]),
            bookmark_commit(&jj, &store, "main").unwrap()
        );
    }
}
