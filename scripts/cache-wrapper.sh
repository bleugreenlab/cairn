#!/usr/bin/env bash
# cairn-cache-wrapper-capability: out-dir-gate
#   The line above declares that this wrapper attaches to Cairn's supervised
#   daemon only for output directories CAIRN_BUILD_SERVICE_ROOTS covers. The
#   runner reads it before routing any build to the daemon and withholds
#   routing from a wrapper without it (orchestrator/build_services.rs), so keep
#   it while the gate below exists, and only then.
# rustc-wrapper / CMake compiler launcher with graceful fallback.
# When sccache is on PATH, route compilation through it; otherwise exec the
# compiler directly so builds work unchanged on machines without sccache.
#
# BASH, NOT `/bin/sh`, AND THAT IS LOAD-BEARING. This script stands between
# cargo and EVERY rustc invocation, so it must hand the compiler the exact
# environment cargo built. A POSIX shell does not: dash -- `/bin/sh` on Debian
# and Ubuntu -- discards environment entries whose names are not valid shell
# identifiers as it imports the environment at startup, and never passes them
# on. Cargo sets several such names, because it derives them from cargo TARGET
# names, which may legally contain a hyphen: `CARGO_BIN_EXE_<bin>`, which a
# package's own integration tests read, and the `CARGO_BIN_FILE_*` /
# `CARGO_CDYLIB_FILE_*` / `CARGO_STATICLIB_FILE_*` family for artifact
# dependencies. Under dash, `env!("CARGO_BIN_EXE_cairn-runner")` in
# `cairn-runner/tests/startup.rs` failed to compile and took the whole workspace
# test build with it -- every Rust check lane on every Ubuntu executor at once,
# and only on a COLD build, since a warm target dir never recompiles that test
# (CAIRN-3752). Bash keeps such entries and re-exports them unchanged.
#
# Two ambient signals say "do not start a server here", for two different
# reasons, and either one is decisive:
#
#   CAIRN_BUILD_SERVICE_CLIENT — Cairn has pointed this build at its supervised
#   daemon by injecting that daemon's SCCACHE_* env. The only server that may
#   hold Cairn's port is the one Cairn supervises, so attach when it is up and
#   compile directly when it is not — and attach only for a compile whose
#   output directory the daemon was granted (CAIRN_BUILD_SERVICE_ROOTS).
#
#   CAIRN_SANDBOXED — this spawn is fenced. A server auto-started here inherits
#   the spawning worktree's Seatbelt sandbox (macOS sandboxing is inherited at
#   fork) and is then denied when a cache miss compiles into any *other*
#   worktree's target/ dir — the cross-worktree EPERM bug. Fenced and NOT pointed
#   at Cairn's daemon means there is no cache this build may safely use at all,
#   so it compiles directly.
#
# Compiling uncached is reliable and merely slower; the worst a broken cache may
# cost a build is a cache hit. See docs/worktree-fence.md.

sanitize_sccache_transport() {
  # sccache defaults to TCP server mode. Preserve an explicit short UDS, but
  # drop inherited deep-worktree socket paths before sccache trips SUN_LEN.
  if [ -n "${SCCACHE_SERVER_UDS+x}" ] && [ -n "$SCCACHE_SERVER_UDS" ]; then
    if [ ${#SCCACHE_SERVER_UDS} -gt 100 ]; then
      echo "warning: ignoring SCCACHE_SERVER_UDS longer than 100 chars; using TCP sccache server instead" >&2
      unset SCCACHE_SERVER_UDS
      : "${SCCACHE_SERVER_PORT:=4226}"
      export SCCACHE_SERVER_PORT
    fi
  else
    : "${SCCACHE_SERVER_PORT:=4226}"
    export SCCACHE_SERVER_PORT
  fi
}

sanitize_sccache_tempdir() {
  # Some sccache startup paths create short-lived Unix sockets under TMPDIR even
  # when the server transport is TCP. Keep that rendezvous path safely short and
  # live: long-lived runners can inherit a managed build slot's TMPDIR after the
  # slot and its scratch directory have been released.
  if [ -n "${TMPDIR:-}" ] && { [ ${#TMPDIR} -gt 80 ] || [ ! -d "$TMPDIR" ]; } && [ -d /tmp ]; then
    use_stable_tempdir
  fi
}

use_stable_tempdir() {
  TMPDIR=/tmp
  TMP=/tmp
  TEMP=/tmp
  export TMPDIR TMP TEMP
}

# Whether TMPDIR belongs to a scope that gets reclaimed: an executor build
# cell's scratch directory, or a per-job scratch directory. Both live inside a
# Cairn home, and the glob spans every one of them -- `.cairn`, `.cairn-dev`,
# and a dev instance's `.cairn-dev-<key>` -- because a directory reclaimed under
# one is exactly as fatal to a daemon that outlives it as under another.
sccache_tempdir_is_ephemeral() {
  case "${TMPDIR:-}" in
    */.cairn*/build-slots/*) return 0 ;;
    */.cairn*/scratch/*) return 0 ;;
    *) return 1 ;;
  esac
}

stabilize_daemon_tempdir() {
  # A CLIENT may use an ephemeral temp directory safely: it outlives the compile.
  # A SERVER may not. sccache's server keeps the TMPDIR it was launched with for
  # its entire life and creates a temp directory there for every cache-miss
  # compile it runs, on behalf of every build on the machine. So a server
  # auto-started from within a build cell keeps pointing at that cell's scratch
  # path long after the cell is reclaimed, and then kills every compile routed to
  # it with `Failed to create temp dir` — a failure that surfaces in third-party
  # crates and looks unrelated to whatever change is under test. Called only on
  # the paths where sccache may start a server; the temp directory a client
  # inherits is otherwise left alone.
  if sccache_tempdir_is_ephemeral && [ -d /tmp ]; then
    use_stable_tempdir
  fi
}

# Route a compile through sccache, degrading to a direct compile if sccache
# itself fails fatally. sccache uses exit status 254 for its own startup fatals
# and, in 0.15.0, status 2 when client/server transport fails before the compiler
# response arrives. In either case the wrapper has no compiler verdict, so
# retrying the original compiler command directly is safe and correct. Without this,
# any stray or broken daemon we route to hard-fails the build instead of costing
# it a cache hit, which is exactly the graceful degradation this wrapper exists
# to provide. Every other status is the compiler's own and propagates unchanged.
#
# A fallback says NOTHING on the compile's stderr. Cargo stores a unit's
# compiler output and replays it every time that unit is fresh, so a single
# fallback line once reprinted on 70 units of every build for hours. sccache's
# own lines all begin `sccache: `, and they are dropped from the stream as it
# flows — streamed, never buffered, because cargo reads rustc's stderr
# incrementally for JSON diagnostics and for the metadata notice pipelining
# starts dependents on. rustc never writes a line with that prefix, so its
# diagnostics pass through unchanged. A degraded cache is reported where an
# operator looks (cairn://settings, cairn://executors/local), not here.
#
# Where that guarantee ENDS, stated because the boundary is not obvious and
# guessing at it costs whole builds:
#
# A cache MISS is compiled by the daemon, in the daemon's process, under the
# daemon's sandbox grant and temp directory. When that compile fails for the
# daemon's own reasons — an output write the daemon's grant does not cover, say —
# sccache has already run the compiler, so this is not a 254 fatal. It is exit 1,
# carrying rustc's own error text. Measured against a live daemon whose grant
# missed the requesting build's target dir (CAIRN-3355):
#
#   $ sccache rustc ... --out-dir <ungranted> lib.rs; echo $?
#   error: error writing dependencies to `.../probe3.d`: Operation not permitted (os error 1)
#   1
#
# A genuine type error in that same file also exits 1, so no status can tell
# them apart after the fact. Both causes are therefore settled BEFORE the call:
#
#   - The grant. The supervisor grants the daemon `**/target/**` under every
#     managed root and names the roots it may serve in CAIRN_BUILD_SERVICE_ROOTS;
#     this wrapper attaches only when rustc's output directory lies in a
#     `target/` tree under one of them (out_dir_is_granted). A build writing
#     anywhere else — a CARGO_TARGET_DIR under $TMPDIR, a tool cache — never
#     reaches the daemon, however it came to be pointed at it.
#   - The daemon. The supervisor proves each root once per daemon incarnation
#     with a compile of its own, and sets CAIRN_BUILD_SERVICE_UNFIT on a build
#     whose root the daemon may not serve (see orchestrator/build_services.rs).
#
# This wrapper acts on those facts rather than inferring one it cannot see.
run_sccache() {
  sccache "$@" 2> >(grep -v --line-buffered '^sccache: ' >&2)
  status=$?
  case "$status" in
    2 | 254) exec "$@" ;;
    *) exit "$status" ;;
  esac
}

# The directory rustc writes this compile's outputs to, from `--out-dir X` or
# `--out-dir=X`. Fails when the invocation names none (`rustc -vV`, `--print`).
rustc_out_dir() {
  previous=""
  for argument in "$@"; do
    if [ "$previous" = "--out-dir" ]; then
      printf '%s\n' "$argument"
      return 0
    fi
    case "$argument" in
      --out-dir=*)
        printf '%s\n' "${argument#--out-dir=}"
        return 0
        ;;
    esac
    previous="$argument"
  done
  return 1
}

# A directory as the kernel resolves it, whether or not it exists yet: the
# nearest existing ancestor resolved with `cd -P` (no GNU realpath on macOS),
# then the rest appended. Relative paths are taken from the working directory.
# The service sandbox judges real paths, so the comparison below must too:
# macOS's /tmp and /var are /private/..., and a build volume may be reached
# through a symlink.
canonical_dir() {
  path="$1"
  case "$path" in
    /*) ;;
    *) path="$PWD/$path" ;;
  esac
  path="${path%/}"
  rest=""
  while [ -n "$path" ] && [ ! -d "$path" ]; do
    rest="/${path##*/}$rest"
    path="${path%/*}"
  done
  resolved=$(cd -P "${path:-/}" 2>/dev/null && pwd -P) || return 1
  printf '%s%s\n' "${resolved%/}" "$rest"
}

# Whether the daemon's grant covers where this compile writes: its output
# directory is in a `target/` tree under one of CAIRN_BUILD_SERVICE_ROOTS.
# `case` lets `*` cross `/`, which is exactly the sandbox grant's
# `^<root>/.*/target/.*` (target_grant_glob in config/build_services.rs).
out_dir_is_granted() {
  [ -n "${CAIRN_BUILD_SERVICE_ROOTS:-}" ] || return 1
  out=$(rustc_out_dir "$@") || return 1
  out=$(canonical_dir "$out") || return 1
  roots="$CAIRN_BUILD_SERVICE_ROOTS"
  while [ -n "$roots" ]; do
    root="${roots%%:*}"
    case "$roots" in
      *:*) roots="${roots#*:}" ;;
      *) roots="" ;;
    esac
    [ -n "$root" ] || continue
    case "$out/" in
      "$root"/*/target/*) return 0 ;;
    esac
    # The supervisor names roots canonically; resolve one only if it missed.
    real_root=$(canonical_dir "$root") || continue
    case "$out/" in
      "$real_root"/*/target/*) return 0 ;;
    esac
  done
  return 1
}

# Probe whether a TCP server is listening on 127.0.0.1:$1, through bash's own
# /dev/tcp so the probe needs nothing installed. "Cannot probe" counts as
# unreachable, so we never auto-start a confined server on a port we could not
# check. The redirection runs in a SUBSHELL deliberately: a bare `exec` whose
# only job is a redirection exits a non-interactive bash when that redirection
# fails, which is precisely the outcome this is trying to observe.
sccache_daemon_reachable() {
  port="$1"
  [ -n "$port" ] || return 1
  (exec 3<>"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1
}

if command -v sccache >/dev/null 2>&1; then
  if [ -z "${SCCACHE_DIR+x}" ]; then
    # Cache home is where Cairn's build caches live (scripts/lib/cache-home.ts).
    # One that is not a directory is a build volume that is not mounted, and the
    # cache must not quietly land on the internal disk in its place.
    if [ -n "${CAIRN_CACHE_HOME:-}" ] && [ ! -d "$CAIRN_CACHE_HOME" ]; then
      echo "build volume absent: $CAIRN_CACHE_HOME" >&2
      exit 1
    fi
    SCCACHE_DIR=${CAIRN_CACHE_HOME:-$HOME}/.cache/sccache
  fi
  # SCCACHE_BASEDIRS is deliberately NOT set here. sccache consults basedirs only
  # when normalizing C/C++ preprocessor output (its ccache-CCACHE_BASEDIR
  # analogue); compiler/rust.rs never reads them, and hashes the compile's working
  # directory unconditionally, so two checkouts at different paths never share a
  # workspace-crate object at any setting.
  #
  # It earned nothing on the C/C++ side either, because the two conditions it
  # needs are mutually exclusive here. To configure anything it must reach a
  # SERVER, and sccache loads config in the server process -- so it only counts on
  # the auto-start path below, where the client starts the server and the server
  # inherits this environment. A build Cairn pointed at its supervised daemon just
  # ATTACHES, and never configures it. But only builds OUTSIDE the managed roots
  # reach auto-start, and the value this used to set named {home}/.cairn/worktrees
  # -- itself a managed root, so no auto-starting build ever compiles under it. It
  # would have matched nothing.
  #
  # That reasoning matters because this wrapper is also installed as
  # CMAKE_{C,CXX}_COMPILER_LAUNCHER (scripts/rust-cache-env.ts), so the C/C++ path
  # is live in principle. In this tree it carries no traffic: a fully built
  # target/ contains no CMakeCache.txt, the native deps (aws-lc-sys, ring,
  # zstd-sys, tree-sitter) compile through cc-rs, which consults neither launcher
  # nor RUSTC_WRAPPER, and the daemon has never recorded a non-Rust compile.
  #
  # What genuinely travels between checkouts is registry dependencies, whose
  # compile cwd is the one shared ~/.cargo/registry source dir -- needing no
  # configuration at all. See docs/dev-instances.md.
  export SCCACHE_DIR
  sanitize_sccache_transport
  sanitize_sccache_tempdir

  # sccache REFUSES the call outright when CARGO_INCREMENTAL names anything but
  # 0 -- "incremental compilation is prohibited: Unset CARGO_INCREMENTAL to
  # continue" -- and it does so with status 1, not its fatal 254. Because cargo's
  # very first probe is `rustc -vV` through this wrapper, a single
  # CARGO_INCREMENTAL=1 anywhere in the environment kills every build on the
  # machine before a compiler ever runs.
  #
  # This is the one exit-1 class the wrapper can act on, and it does not weaken
  # the argument above. That argument is about telling a daemon's failure from a
  # compiler's AFTER the compile, where an exit status is all there is to go on.
  # Here the deciding evidence sits in the environment BEFORE the call, so
  # nothing has to be inferred from a status at all -- the same shape as
  # CAIRN_BUILD_SERVICE_UNFIT below, decided locally rather than by the
  # supervisor.
  #
  # Nothing is lost by stepping aside: an incremental compile is one sccache
  # would refuse to cache anyway, so compile directly and keep the incremental
  # state the caller asked for.
  case "${CARGO_INCREMENTAL:-}" in
    "" | 0) ;;
    *) exec "$@" ;;
  esac

  if [ -n "${CAIRN_BUILD_SERVICE_CLIENT:-}" ]; then
    # Cairn has observed the daemon it named failing the compiles it accepts, so
    # routing this compile there would not cost a cache hit — it would fail the
    # build with a diagnostic naming some unrelated crate. Compile directly.
    if [ -n "${CAIRN_BUILD_SERVICE_UNFIT:-}" ]; then
      exec "$@"
    fi
    # The daemon compiles a cache miss itself, under its own sandbox, so it can
    # only serve an output directory its grant covers. Anything else compiles
    # directly, before the daemon is asked to fail it.
    if ! out_dir_is_granted "$@"; then
      exec "$@"
    fi
    # Pointed at Cairn's supervised daemon: use it only if it is already up, and
    # never auto-start one over it. No server can be started here, so this path
    # keeps whatever temp directory it was given.
    if sccache_daemon_reachable "${SCCACHE_SERVER_PORT:-4226}"; then
      run_sccache "$@"
    fi
    exec "$@"
  fi

  if [ -n "${CAIRN_SANDBOXED:-}" ]; then
    # Fenced, and Cairn did not point this build at its daemon — its build
    # directory is one the daemon may not write. Starting a server would confine
    # it; attaching to whichever unconfined server holds sccache's default port
    # would compile through a process the fence does not cover. Compile directly.
    exec "$@"
  fi

  # Neither: the developer's own checkout, or CI. Preserve the original behavior
  # — sccache may auto-start its own server here, which is safe because nothing
  # confines it and nothing supervises the port it uses.
  stabilize_daemon_tempdir
  run_sccache "$@"
fi

exec "$@"
