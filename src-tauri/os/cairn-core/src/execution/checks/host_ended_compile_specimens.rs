/// Specimen a08e20f0 (fedora, `c5a7e3fc`): rustc for `rmcp` killed by
/// SIGTERM mid-compile, its process line cut at the head by the stored tail.
pub(crate) const KILLED_RMCP_TAIL: &str = "error: could not compile `rmcp` (lib)\n\
        …\n\
        /build-slots/cairn/slot-1109/src-tauri/target/debug/deps/libpin_project_lite-b547d44ab2a3e239.rmeta --extern tokio=/home/dev/.cairn-exec/build-slots/cairn/slot-1109/src-tauri/target/debug/deps/libtokio-c4d896466688d916.rmeta --cap-lints allow` (signal: 15, SIGTERM: termination signal)\n\
        warning: build failed, waiting for other jobs to finish...\n\
        warning: field `job_id` is never read\n\
        \x20 --> os/cairn-core/src/notifications/derive.rs:32:5\n\
        warning: `cairn-core` (lib) generated 4 warnings (run `cargo fix --lib -p cairn-core` to apply 1 suggestion)\n\
        error: command `/home/dev/.rustup/toolchains/1.97.1-x86_64-unknown-linux-gnu/bin/cargo test --no-run --message-format json-render-diagnostics --workspace --features test-utils` exited with code 101\n\
        error: script \"test:rust:full\" exited with code 101";

/// Specimen 6ebdcedb (bgsrv-mac, `ffa890b2`): warnings that point at the
/// source, then rustc's write of an object file refused by a full disk.
pub(crate) const DISK_FULL_TAIL: &str = "warning: field `computed_at` is never read\n\
        \x20  --> os/cairn-core/src/projects/codemap.rs:138:9\n\
        \x20   |\n\
        error: could not write output to /Users/dev/.cairn-exec/build-slots/cairn/slot-705/src-tauri/target/debug/deps/cairn_core-a2de4dd904051819.f391dbsmm5xj2b3xuwpopitus.1cihbaz.rcgu.o: No space left on device\n\
        \n\
        error: could not compile `cairn-core` (lib test) due to 1 previous error; 7 warnings emitted\n\
        error: command `/Users/dev/.rustup/toolchains/1.97.1-aarch64-apple-darwin/bin/cargo test --no-run --message-format json-render-diagnostics --workspace --features test-utils` exited with code 101\n\
        error: script \"test:rust:full\" exited with code 101";

/// cairn/5641's bgsrv-ub specimen (`d092b4cd`): rustc SIGKILLed under
/// memory pressure while compiling the cairn-core lib test binary.
pub(crate) const KILLED_CORE_TEST_TAIL: &str = "   Compiling cairn-core v0.13.1 (/home/dev/.cairn-exec/build-slots/cairn/slot-212/src-tauri/os/cairn-core)\n\
        error: could not compile `cairn-core` (lib test)\n\
        \n\
        Caused by:\n\
        \x20 process didn't exit successfully: `/home/dev/.rustup/toolchains/1.97.1-x86_64-unknown-linux-gnu/bin/rustc --crate-name cairn_core --edition=2021 os/cairn-core/src/lib.rs --test --cfg 'feature=\"test-utils\"'` (signal: 9, SIGKILL: kill)\n\
        error: script \"test:rust\" exited with code 101";

/// Observation ee7c3dbd24184705a40079c9's output tail verbatim (cairn/6279;
/// external-host at `469994a7` on local macos/aarch64): the macOS executor
/// sandbox refused rustc's dep-info writes into the public mirror's target
/// directory under the executor's temp root, which is not a build slot.
pub(crate) const MIRROR_WRITE_DENIED_TAIL: &str = r#"check:external-host: assemble the public mirror in /Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/mirror
check:external-host: the mirror drops an unpublished dev-dependency from cairn-core: cairn-exec = { path = "../../cairn-exec" }
check:external-host: build the mirror's libraries and binaries


stderr:
$ bun run scripts/check-external-host.ts
   Compiling libc v0.2.186
   Compiling proc-macro2 v1.0.104
   Compiling unicode-ident v1.0.22
   Compiling quote v1.0.46
   Compiling serde_core v1.0.228
    Checking cfg-if v1.0.4
   Compiling shlex v1.3.0
   Compiling find-msvc-tools v0.1.9
   Compiling serde v1.0.228
    Checking memchr v2.7.6
   Compiling version_check v0.9.5
   Compiling typenum v1.19.0
    Checking itoa v1.0.17
    Checking scopeguard v1.2.0
error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/unicode_ident-c06507b1980ce130.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/itoa-af600afa55fe7352.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/scopeguard-4d17c248b4b80907.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/cfg_if-05a4a27a6f52301a.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/shlex-e069bb4a1a487a53.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/find_msvc_tools-1ca8c1877fee2a48.d`: Operation not permitted (os error 1)

error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/version_check-188de0cca3df455f.d`: Operation not permitted (os error 1)

error: could not compile `cfg-if` (lib) due to 1 previous error
warning: build failed, waiting for other jobs to finish...
error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/memchr-3eddcbc0de07913c.d`: Operation not permitted (os error 1)

error: could not compile `itoa` (lib) due to 1 previous error
error: could not compile `scopeguard` (lib) due to 1 previous error
error: could not compile `unicode-ident` (lib) due to 1 previous error
error: could not compile `shlex` (lib) due to 1 previous error
error: could not compile `find-msvc-tools` (lib) due to 1 previous error
error: could not compile `version_check` (lib) due to 1 previous error
error: could not compile `memchr` (lib) due to 1 previous error
check:external-host: build the mirror's libraries and binaries failed
error: script "check:external-host" exited with code 101


Exit code: 101"#;

/// The first refused write in [`MIRROR_WRITE_DENIED_TAIL`], which a
/// classification of it must cite.
pub(crate) const MIRROR_WRITE_DENIED_EVIDENCE: &str = "error: error writing dependencies to `/Volumes/Builds/cairn-executor/tmp/cbf981ee2f28/cairn-external-host-5a853c8ef436/target/debug/deps/unicode_ident-c06507b1980ce130.d`: Operation not permitted (os error 1)";

/// A compile the tree failed: a coded diagnostic with its location.
pub(crate) const COMPILE_ERROR_TAIL: &str = "error[E0425]: cannot find value `turn_id` in this scope\n   --> os/cairn-core/src/lib.rs:7:9\nerror: could not compile `cairn-core` (lib) due to 1 previous error";
