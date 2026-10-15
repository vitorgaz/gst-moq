//! End-to-end checks against the built plugin library.
//!
//! `smoke` runs tests/smoke.sh: the plugin loads, both elements inspect
//! cleanly, a CMAF publish pipeline runs against an unreachable relay without
//! crashing, and a moqsink state cycle neither fails nor hangs. No relay is
//! reached. `smoke_guards` runs the socket-free controls for that script.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Build the plugin cdylib and return its path. Integration tests only link
/// the rlib, so without this the smoke run could load a stale library.
fn built_plugin() -> PathBuf {
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["build", "--lib", "--message-format=json-render-diagnostics"])
        .current_dir(repo());
    if !cfg!(debug_assertions) {
        cmd.arg("--release");
    }
    let out = cmd.output().expect("run cargo build");
    assert!(
        out.status.success(),
        "cargo build --lib failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let suffix = std::env::consts::DLL_SUFFIX;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| {
            l.contains("\"reason\":\"compiler-artifact\"") && l.contains("\"name\":\"gstmoq\"")
        })
        .flat_map(|l| l.split('"').map(str::to_owned).collect::<Vec<_>>())
        .find(|s| s.ends_with(suffix) && s.contains("gstmoq"))
        .map(PathBuf::from)
        .expect("cargo reported no gstmoq cdylib")
}

#[test]
fn smoke() {
    // A directory holding only this plugin, and a private registry, so the
    // run neither picks up an installed gst-moq nor touches the user cache.
    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR")).join("smoke");
    let plugins = tmp.join("plugins");
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&plugins).unwrap();
    let lib = built_plugin();
    std::fs::copy(&lib, plugins.join(lib.file_name().unwrap())).unwrap();

    let out = Command::new("bash")
        .arg(repo().join("tests/smoke.sh"))
        .arg(&plugins)
        .env("GST_REGISTRY", tmp.join("registry.bin"))
        .output()
        .expect("run tests/smoke.sh");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("smoke OK"),
        "smoke.sh failed ({}):\n{}\n{}",
        out.status,
        stdout,
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn smoke_guards() {
    let out = Command::new("python3")
        .arg("-I")
        .arg(repo().join("tests/test_smoke_guards.py"))
        .output()
        .expect("python3 is required for the smoke guard tests");
    assert!(
        out.status.success(),
        "test_smoke_guards.py failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
