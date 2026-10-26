//! Workflow rollback against a real `celld dev` node.
//!
//! The behaviour test runs in any build. The crash and storage-fault tests
//! need the failpoint build, which turns the harness's durability
//! checkpoints into calls that abort the node or fail its commit:
//!
//! ```sh
//! RUSTFLAGS="--cfg celld_workflow_failpoints" \
//!   CARGO_TARGET_DIR=target/failpoints \
//!   cargo test -p celld --test workflow_rollback
//! ```
//!
//! A crash test arms one checkpoint, lets the node abort there, starts a new
//! node on the same local state, and asserts that the rollback still settles
//! with every handler run at least once and in order.

// The test builds a throwaway project directory and reads the failpoint
// file; neither is node storage, which the disallowed fs methods guard.
#![allow(clippy::disallowed_methods)]

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/workflow_rollback");

struct Node {
    _dir: tempfile::TempDir,
    project: PathBuf,
    failpoints: PathBuf,
    port: u16,
    supervisor: Option<Child>,
}

impl Node {
    fn start() -> Self {
        let dir = tempfile::tempdir().expect("create the project directory");
        let project = dir.path().join("project");
        std::fs::create_dir(&project).expect("create the project directory");
        for file in ["wrangler.jsonc", "index.js"] {
            std::fs::copy(Path::new(FIXTURE).join(file), project.join(file))
                .expect("copy the fixture");
        }
        let failpoints = dir.path().join("failpoints");
        std::fs::write(&failpoints, "").expect("create the failpoint file");
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("pick a free port")
            .port();
        let mut node = Self {
            _dir: dir,
            project,
            failpoints,
            port,
            supervisor: None,
        };
        node.spawn(true);
        node
    }

    fn spawn(&mut self, clean: bool) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.project.join("node.log"))
            .expect("open the node log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        command
            .arg("dev")
            .arg(&self.project)
            .args(["--port", &self.port.to_string(), "--no-watch"])
            .env("CELLD_TEST_WORKFLOW_FAILPOINTS", &self.failpoints)
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("clone the node log"))
            .stderr(log);
        if clean {
            command.arg("--clean");
        }
        self.supervisor = Some(command.spawn().expect("start celld dev"));
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if self.try_get("/ready").is_some_and(|body| body == "not found") {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("celld dev did not start; see {}", self.project.join("node.log").display());
    }

    /// Stop the supervisor and the node it started. The node runs in its own
    /// process group, and its command line names the listener port.
    fn kill(&mut self) {
        if let Some(mut supervisor) = self.supervisor.take() {
            let _ = supervisor.kill();
            let _ = supervisor.wait();
        }
        let _ = Command::new("pkill")
            .args(["-9", "-f", &format!("listen 127.0.0.1:{} ", self.port)])
            .status();
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.try_get("/ready").is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    #[cfg(celld_workflow_failpoints)]
    fn arm(&self, lines: &[&str]) {
        let text: String = lines.iter().map(|line| format!("{line}\n")).collect();
        std::fs::write(&self.failpoints, text).expect("arm the failpoints");
    }

    /// Wait for an armed crash to take the node down, then start a new node
    /// on the same local state.
    #[cfg(celld_workflow_failpoints)]
    fn restart_after_crash(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.try_get("/ready").is_some() {
            assert!(Instant::now() < deadline, "the armed failpoint did not crash the node");
            std::thread::sleep(Duration::from_millis(100));
        }
        let armed = std::fs::read_to_string(&self.failpoints).expect("read the failpoints");
        assert!(armed.trim().is_empty(), "failpoints still armed after the crash: {armed}");
        self.kill();
        self.spawn(false);
    }

    fn try_get(&self, path: &str) -> Option<String> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(60))).ok()?;
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )
        .ok()?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).ok()?;
        let response = String::from_utf8(response).ok()?;
        let (head, body) = response.split_once("\r\n\r\n")?;
        if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
            Some(dechunk(body))
        } else {
            Some(body.to_owned())
        }
    }

    fn get(&self, path: &str) -> Value {
        let body = self
            .try_get(path)
            .unwrap_or_else(|| panic!("GET {path} failed"));
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("GET {path} answered {body}"))
    }

    fn create(&self, id: &str, params: Value) {
        let params = percent_encoding::utf8_percent_encode(
            &params.to_string(),
            percent_encoding::NON_ALPHANUMERIC,
        )
        .to_string();
        let created = self.get(&format!("/create?id={id}&params={params}"));
        assert_eq!(created, serde_json::json!({ "ok": true }), "create {id}");
    }

    fn call(&self, path: &str, id: &str) -> Value {
        let separator = if path.contains('?') { '&' } else { '?' };
        self.get(&format!("{path}{separator}id={id}"))
    }

    fn status(&self, id: &str) -> Value {
        self.call("/status", id)
    }

    /// Wait until the instance stops running (or rolling back).
    fn settled(&self, id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let status = self.status(id);
            let state = status["status"].as_str().unwrap_or_default();
            if !["queued", "running", "waiting"].contains(&state) {
                return status;
            }
            assert!(Instant::now() < deadline, "{id} did not settle: {status}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn wait_for_status(&self, id: &str, wanted: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.status(id)["status"] != wanted {
            assert!(Instant::now() < deadline, "{id} never reached {wanted}");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn log(&self, id: &str) -> Vec<Value> {
        self.call("/log", id).as_array().cloned().expect("log array")
    }

    /// Rollback handler runs in order, as (step, output).
    fn undone(&self, id: &str) -> Vec<(String, Value)> {
        self.log(id)
            .into_iter()
            .filter(|entry| !entry["step"].as_str().unwrap().starts_with("fwd:"))
            .map(|entry| (entry["step"].as_str().unwrap().to_owned(), entry["output"].clone()))
            .collect()
    }

    fn forward_runs(&self, id: &str, step: &str) -> usize {
        let wanted = format!("fwd:{step}");
        self.log(id).iter().filter(|entry| entry["step"] == wanted).count()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        out.push_str(&tail[..size]);
        rest = &tail[size + 2..];
    }
    out
}

fn steps(undone: &[(String, Value)]) -> Vec<&str> {
    undone.iter().map(|(step, _)| step.as_str()).collect()
}

fn rollback(outcome: &str) -> Value {
    serde_json::json!({ "outcome": outcome, "error": null })
}

#[test]
fn rollback_behaviour() {
    let node = Node::start();
    let scenario = |id: &str, name: &str| node.create(id, serde_json::json!({ "scenario": name }));
    for (id, name) in [
        ("chain", "chain"),
        ("repeat", "repeat"),
        ("parallel", "parallel"),
        ("caught", "caught"),
        ("failed-step", "failed-step"),
        ("transient", "transient"),
        ("halt", "halt"),
        ("sibling", "sibling"),
        ("branches", "branches"),
        ("restart-from", "restart-from"),
        ("bad-options", "bad-options"),
        ("bad-config", "bad-config"),
        ("terminate", "sleeper"),
        ("terminate-paused", "sleeper"),
        ("terminate-plain", "sleeper"),
        ("terminate-null", "sleeper"),
        ("in-flight", "in-flight"),
        ("rpc-fence", "slow-undo"),
        ("delete-mid", "slow-undo"),
    ] {
        scenario(id, name);
    }

    // Forward failure: reverse start order, saved output, original context.
    let status = node.settled("chain");
    assert_eq!(status["status"], "errored");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(status["error"]["message"], "boom");
    assert_eq!(steps(&node.undone("chain")), ["b", "a"]);
    let first = &node.log("chain").into_iter().find(|e| e["step"] == "b").unwrap();
    assert_eq!(first["output"], "out-b");
    assert_eq!(first["error"], "NonRetryableError:boom");
    assert_eq!(first["ctx"], serde_json::json!(["b", 1, 1]));

    node.settled("repeat");
    let undone = node.undone("repeat");
    let outputs: Vec<&Value> = undone.iter().map(|(_, output)| output).collect();
    assert_eq!(outputs, ["same-2", "same-1", "same-0"]);

    // Reverse start order, not finish order.
    node.settled("parallel");
    assert_eq!(steps(&node.undone("parallel")), ["second", "first"]);

    assert_eq!(node.settled("caught")["rollback"], Value::Null);
    assert!(node.undone("caught").is_empty());

    // A permanently failed step is compensated, without an output.
    node.settled("failed-step");
    assert_eq!(
        node.undone("failed-step"),
        [("bad".to_owned(), Value::Null), ("a".to_owned(), "out-a".into())]
    );

    // A handler retries under its own policy.
    assert_eq!(node.settled("transient")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("transient")), ["a", "a"]);

    // An exhausted handler halts the chain; retryRollback() finishes it.
    let status = node.settled("halt");
    assert_eq!(status["rollback"]["outcome"], "failed");
    assert_eq!(status["rollback"]["error"]["message"], "undo failed for b");
    assert_eq!(steps(&node.undone("halt")), ["b", "b"]);
    assert!(node.call("/retry-rollback", "caught")["error"]
        .as_str()
        .unwrap()
        .contains("needs an instance whose rollback failed"));
    node.call("/allow", "halt");
    assert_eq!(node.call("/retry-rollback", "halt"), serde_json::json!({ "ok": true }));
    assert_eq!(node.settled("halt")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("halt")), ["b", "b", "b", "a"]);

    // run() rejecting beside a running sibling waits for it, so the
    // compensator sees its real output.
    node.settled("sibling");
    assert_eq!(node.undone("sibling"), [("reserve".to_owned(), "out-reserve".into())]);

    // A parallel branch still registers after a sibling rejects the replay.
    assert_eq!(node.settled("branches")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("branches")), ["a2"]);

    // restart({from}) re-runs an already compensated step instead of
    // replaying its output, and compensates the new effect once.
    node.settled("restart-from");
    assert_eq!(node.call("/restart?from=b", "restart-from"), serde_json::json!({ "ok": true }));
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.undone("restart-from").len() < 2 {
        assert!(Instant::now() < deadline, "restart-from never compensated again");
        std::thread::sleep(Duration::from_millis(100));
    }
    node.settled("restart-from");
    assert_eq!(node.forward_runs("restart-from", "a"), 2);
    assert_eq!(steps(&node.undone("restart-from")), ["a", "a"]);

    let status = node.settled("bad-options");
    assert!(status["error"]["message"].as_str().unwrap().contains("rollback must be a function"));
    let status = node.settled("bad-config");
    assert!(status["error"]["message"].as_str().unwrap().contains("unknown option \"sensitive\""));

    // terminate({rollback: true}) on a waiting and on a paused instance.
    for id in ["terminate", "terminate-paused", "terminate-plain", "terminate-null"] {
        node.wait_for_status(id, "waiting");
    }
    node.call("/pause", "terminate-paused");
    node.wait_for_status("terminate-paused", "paused");
    for id in ["terminate", "terminate-paused"] {
        let done = node.call("/terminate?options=%7B%22rollback%22%3Atrue%7D", id);
        assert_eq!(done, serde_json::json!({ "ok": true }));
        let status = node.status(id);
        assert_eq!(status["status"], "terminated", "{id}");
        assert_eq!(status["rollback"], rollback("complete"), "{id}");
        let entry = node.log(id).into_iter().find(|e| e["step"] == "a").unwrap();
        assert_eq!(entry["error"], "Terminated:Instance terminated during rollback");
    }
    node.call("/terminate", "terminate-plain");
    assert_eq!(node.status("terminate-plain")["rollback"], Value::Null);
    assert!(node.undone("terminate-plain").is_empty());
    node.call("/terminate?options=null", "terminate-null");
    assert_eq!(node.status("terminate-null")["status"], "terminated");

    // A terminate under a running step waits for it; its late result is
    // fenced, so its compensator receives undefined.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.forward_runs("in-flight", "a") == 0 {
        assert!(Instant::now() < deadline, "in-flight never started");
        std::thread::sleep(Duration::from_millis(50));
    }
    std::thread::sleep(Duration::from_millis(300));
    node.call("/terminate?options=%7B%22rollback%22%3Atrue%7D", "in-flight");
    assert_eq!(node.settled("in-flight")["rollback"], rollback("complete"));
    assert_eq!(
        node.undone("in-flight"),
        [("slow".to_owned(), Value::Null), ("a".to_owned(), "out-a".into())]
    );

    // While compensating: running status, fenced RPCs, and delete stops it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.undone("rpc-fence").is_empty() || node.undone("delete-mid").is_empty() {
        assert!(Instant::now() < deadline, "slow-undo never started compensating");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        node.status("rpc-fence"),
        serde_json::json!({ "status": "running", "rollback": null })
    );
    for (path, action) in [
        ("/event", "send an event to"),
        ("/pause", "pause"),
        ("/restart", "restart"),
        ("/terminate?options=%7B%22rollback%22%3Atrue%7D", "terminate"),
    ] {
        let refused = node.call(path, "rpc-fence");
        assert_eq!(
            refused["error"],
            format!("WORKFLOW_ERROR: cannot {action} an instance that is rolling back"),
            "{path}"
        );
    }
    assert_eq!(node.call("/delete", "delete-mid"), serde_json::json!({ "ok": true }));
    assert_eq!(node.settled("rpc-fence")["rollback"], rollback("complete"));
    std::thread::sleep(Duration::from_millis(4000));
    assert_eq!(steps(&node.undone("delete-mid")), ["b"]);
    assert!(node.status("delete-mid")["error"]
        .as_str()
        .unwrap()
        .contains("instance does not exist"));
}

/// Crash at one checkpoint of the "chain" scenario (a, b with handlers, then
/// a failure) and return the node after the rollback settled on restart.
#[cfg(celld_workflow_failpoints)]
fn chain_crashing_at(failpoints: &[&str], params: Value) -> (Node, Value) {
    let mut node = Node::start();
    node.arm(failpoints);
    node.create("chain", params);
    node.restart_after_crash();
    let status = node.settled("chain");
    (node, status)
}

#[cfg(celld_workflow_failpoints)]
fn chain() -> Value {
    serde_json::json!({ "scenario": "chain" })
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_after_forward_callback_reruns_the_step() {
    let (node, status) = chain_crashing_at(&["forward.after_callback crash"], chain());
    assert_eq!(status["rollback"], rollback("complete"));
    // The effect happened, its output did not commit: the step runs again.
    assert_eq!(node.forward_runs("chain", "a"), 2);
    assert_eq!(steps(&node.undone("chain")), ["b", "a"]);
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_after_rollback_request_resumes_it() {
    let (node, status) = chain_crashing_at(&["rollback.requested crash"], chain());
    assert_eq!(status["status"], "errored");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("chain")), ["b", "a"]);
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_before_a_handler_runs_it_after_restart() {
    let (node, status) = chain_crashing_at(&["rollback.before_handler crash"], chain());
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("chain")), ["b", "a"]);
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_after_a_handler_runs_it_again() {
    // The handler's effect landed but its completion did not commit, so it
    // runs a second time: compensation is at least once.
    let (node, status) = chain_crashing_at(&["rollback.after_handler crash"], chain());
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("chain")), ["b", "b", "a"]);
}

/// A commit is durable only once the output gate proves it, so a crash right
/// after one may lose it and run that handler again. Assert what holds
/// either way: every handler ran, in order, and none ran more than twice.
#[cfg(celld_workflow_failpoints)]
fn assert_compensated_at_least_once(undone: &[(String, Value)]) {
    let order = steps(undone);
    let b = order.iter().filter(|step| **step == "b").count();
    let a = order.iter().filter(|step| **step == "a").count();
    assert!((1..=2).contains(&b) && (1..=2).contains(&a), "{order:?}");
    assert!(order[..b].iter().all(|step| *step == "b"), "{order:?}");
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_between_steps_resumes_at_the_next_handler() {
    let (node, status) = chain_crashing_at(&["rollback.after_step crash"], chain());
    assert_eq!(status["rollback"], rollback("complete"));
    let undone = node.undone("chain");
    assert_compensated_at_least_once(&undone);
    assert_eq!(steps(&undone).iter().filter(|step| **step == "a").count(), 1);
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn crash_before_finish_settles_on_restart() {
    let (node, status) = chain_crashing_at(&["rollback.before_finish crash"], chain());
    assert_eq!(status["status"], "errored");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_compensated_at_least_once(&node.undone("chain"));
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn handler_that_crashes_every_attempt_exhausts_its_budget() {
    // One retry allowed: two attempts that never return end the rollback.
    let params = serde_json::json!({
        "scenario": "chain",
        "bConfig": { "retries": { "limit": 1, "delay": 0 } },
    });
    let mut node = Node::start();
    node.arm(&["rollback.before_handler crash"]);
    node.create("chain", params);
    node.restart_after_crash();
    node.arm(&["rollback.before_handler crash"]);
    node.restart_after_crash();
    let status = node.settled("chain");
    assert_eq!(status["rollback"]["outcome"], "failed");
    assert!(status["rollback"]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("did not return in 2 attempts"));
    assert!(node.undone("chain").is_empty());
}

#[cfg(celld_workflow_failpoints)]
#[test]
fn storage_fault_before_a_handler_does_not_spend_its_retries() {
    // No retries allowed: a storage failure counted as a handler failure
    // would fail the rollback. It must retry the drive instead.
    let params = serde_json::json!({
        "scenario": "chain",
        "bConfig": { "retries": { "limit": 0, "delay": 0 } },
    });
    let node = Node::start();
    node.arm(&["rollback.running_commit storage"]);
    node.create("chain", params);
    let status = node.settled("chain");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("chain")), ["b", "a"]);
    assert!(std::fs::read_to_string(&node.failpoints).unwrap().trim().is_empty());
}
