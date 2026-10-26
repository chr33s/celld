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
//!
//! The fleet test is ignored by default because it needs Docker. It runs a
//! MinIO bucket and two `celld` nodes, kills the node that owns the Workflow
//! cell in the middle of a rollback handler, and asserts that the other node
//! takes the cell over and finishes the rollback:
//!
//! ```sh
//! cargo test -p celld --test workflow_rollback -- --ignored fleet
//! ```

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
    token: String,
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
        let token = dir
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a temporary directory name")
            .to_owned();
        std::fs::write(project.join(".dev.vars"), format!("NODE_TOKEN={token}\n"))
            .expect("write the node token");
        let port = free_port();
        let mut node = Self {
            _dir: dir,
            project,
            failpoints,
            token,
            port,
            supervisor: None,
        };
        node.spawn(true);
        node
    }

    /// Start `celld dev` and wait until this node, identified by its token,
    /// answers. Another test can take a port this node released, so a start
    /// whose supervisor exits early retries on a fresh port; the local state
    /// lives in the project, not behind the port.
    fn spawn(&mut self, clean: bool) {
        for _ in 0..3 {
            if self.try_spawn(clean) {
                return;
            }
            self.port = free_port();
        }
        panic!("celld dev did not start\n{}", self.log_tail());
    }

    fn try_spawn(&mut self, clean: bool) -> bool {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.project.join("node.log"))
            .expect("open the node log");
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        command
            .arg("dev")
            .arg(&self.project)
            .args(["--port", &self.port.to_string(), "--no-watch", "--logs"])
            .env("CELLD_TEST_WORKFLOW_FAILPOINTS", &self.failpoints)
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("clone the node log"))
            .stderr(log);
        if clean {
            command.arg("--clean");
        }
        let mut supervisor = command.spawn().expect("start celld dev");
        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if self.try_get("/whoami").as_deref() == Some(self.token.as_str()) {
                self.supervisor = Some(supervisor);
                return true;
            }
            if supervisor.try_wait().expect("poll the supervisor").is_some() {
                return false;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = supervisor.kill();
        let _ = supervisor.wait();
        false
    }

    fn log_tail(&self) -> String {
        let log = std::fs::read_to_string(self.project.join("node.log")).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    /// Stop the supervisor and the node it started. The node runs in its own
    /// process group, so it is found as the supervisor's child and killed by
    /// pid; matching its command line could hit another test's node that
    /// took over a port this one released.
    fn kill(&mut self) {
        if let Some(mut supervisor) = self.supervisor.take() {
            let children = Command::new("pgrep")
                .args(["-P", &supervisor.id().to_string()])
                .output()
                .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
                .unwrap_or_default();
            for pid in children.split_whitespace() {
                let _ = Command::new("kill").args(["-9", pid]).status();
            }
            let _ = supervisor.kill();
            let _ = supervisor.wait();
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.answers() && Instant::now() < deadline {
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
        while self.answers() {
            assert!(Instant::now() < deadline, "the armed failpoint did not crash the node");
            std::thread::sleep(Duration::from_millis(100));
        }
        let armed = std::fs::read_to_string(&self.failpoints).expect("read the failpoints");
        assert!(armed.trim().is_empty(), "failpoints still armed after the crash: {armed}");
        self.kill();
        self.spawn(false);
    }

    /// Restart on the same local state, as a deploy of new code would.
    fn restart(&mut self) {
        self.kill();
        self.spawn(false);
    }

    /// Rewrite the fixture's `DRIFT` mode for the next start.
    fn set_drift(&self, mode: &str) {
        let path = self.project.join("index.js");
        let source = std::fs::read_to_string(&path).expect("read the fixture");
        let line = source
            .lines()
            .find(|line| line.starts_with("const DRIFT = "))
            .expect("the fixture declares DRIFT")
            .to_owned();
        let source = source.replace(&line, &format!("const DRIFT = {mode:?};"));
        std::fs::write(&path, source).expect("write the fixture");
    }

    /// The rollback lifecycle events the node logged for one instance, as
    /// `kind` or `kind:step`.
    fn events(&self, id: &str) -> Vec<String> {
        let log = std::fs::read_to_string(self.project.join("node.log")).expect("read the node log");
        rollback_events(&log, id)
    }

    fn try_get(&self, path: &str) -> Option<String> {
        http_get(self.port, path)
    }

    /// Whether this node, not merely some process on its port, answers.
    fn answers(&self) -> bool {
        self.try_get("/whoami").as_deref() == Some(self.token.as_str())
    }

    fn node_log(&self) -> String {
        std::fs::read_to_string(self.project.join("node.log")).expect("read the node log")
    }

    /// The operator listener of the running node, from its startup line.
    fn internal_port(&self) -> u16 {
        let log = self.node_log();
        let line = log
            .lines()
            .rev()
            .find(|line| line.starts_with("celld internal listening on 127.0.0.1:"))
            .expect("the node announced its internal listener");
        line["celld internal listening on 127.0.0.1:".len()..]
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|port| port.parse().ok())
            .expect("an internal port")
    }

    fn state(&self) -> Value {
        let body = http_get(self.internal_port(), "/state").expect("read the node state");
        serde_json::from_str(&body).expect("node state JSON")
    }

    /// Send a request and hang up before it answers, as a client that gives
    /// up or a cancelled fetch does.
    fn cancel_get(&self, path: &str, after: Duration) {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).expect("connect");
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        )
        .expect("send the request");
        std::thread::sleep(after);
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }

    /// Ask the supervisor to stop, as Ctrl-C does, and return how long the
    /// node took to drain and exit.
    fn stop_gracefully(&mut self) -> Duration {
        let mut supervisor = self.supervisor.take().expect("a running supervisor");
        let started = Instant::now();
        let _ = Command::new("kill")
            .args(["-INT", &supervisor.id().to_string()])
            .status();
        let deadline = started + Duration::from_secs(60);
        while supervisor.try_wait().expect("poll the supervisor").is_none() {
            assert!(Instant::now() < deadline, "celld dev did not stop");
            std::thread::sleep(Duration::from_millis(100));
        }
        started.elapsed()
    }
}

/// Parse `2026-10-26T04:22:41.416625Z` into milliseconds within the day,
/// enough to measure the gaps between attempts of one test run.
fn log_millis(line: &str) -> Option<f64> {
    let time = line.split('T').nth(1)?.split('Z').next()?;
    let mut parts = time.split(':');
    let hours: f64 = parts.next()?.parse().ok()?;
    let minutes: f64 = parts.next()?.parse().ok()?;
    let seconds: f64 = parts.next()?.parse().ok()?;
    Some(((hours * 60.0 + minutes) * 60.0 + seconds) * 1000.0)
}

fn rollback_events(log: &str, id: &str) -> Vec<String> {
    let field = |line: &str, name: &str| {
        line.split_once(&format!(" {name}="))
            .map(|(_, rest)| rest.split(' ').next().unwrap_or_default().to_owned())
            .unwrap_or_default()
    };
    log.lines()
        .filter(|line| line.contains("event=\"workflow_rollback\""))
        .filter(|line| field(line, "instance") == id)
        .map(|line| match field(line, "step") {
            step if step.is_empty() => field(line, "kind"),
            step => format!("{}:{step}", field(line, "kind")),
        })
        .collect()
}

fn http_get(port: u16, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("pick a free port")
        .port()
}

impl Node {
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
            assert!(
                Instant::now() < deadline,
                "{id} did not settle: {status}\n{}",
                self.log_tail()
            );
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
        ("rpc-fence", "held-undo"),
        ("delete-mid", "held-undo"),
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
    assert_eq!(
        node.events("chain"),
        [
            "started",
            "step_started:b",
            "attempt_started:b",
            "attempt_completed:b",
            "step_completed:b",
            "step_started:a",
            "attempt_started:a",
            "attempt_completed:a",
            "step_completed:a",
            "completed",
        ]
    );

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
    let events = node.events("halt");
    let halted = [
        "attempt_started:b",
        "attempt_errored:b",
        "attempt_started:b",
        "attempt_errored:b",
        "step_errored:b",
        "errored",
        "retried:b",
        "started",
        "step_started:b",
    ];
    assert!(
        events.windows(halted.len()).any(|window| window == halted),
        "{events:?}"
    );
    assert_eq!(events.last().map(String::as_str), Some("completed"));

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
    // fenced, so its compensator receives undefined. The step is held until
    // the terminate has landed.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.forward_runs("in-flight", "slow") == 0 {
        assert!(Instant::now() < deadline, "in-flight never started");
        std::thread::sleep(Duration::from_millis(50));
    }
    let port = node.port;
    let terminating = std::thread::spawn(move || {
        http_get(port, "/terminate?options=%7B%22rollback%22%3Atrue%7D&id=in-flight")
    });
    std::thread::sleep(Duration::from_millis(500));
    node.call("/allow", "in-flight");
    terminating.join().expect("the terminate call");
    assert_eq!(node.settled("in-flight")["rollback"], rollback("complete"));
    assert_eq!(
        node.undone("in-flight"),
        [("slow".to_owned(), Value::Null), ("a".to_owned(), "out-a".into())]
    );

    // While compensating: running status, fenced RPCs, and delete stops it.
    // Both handlers for b are held until allowed.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.undone("rpc-fence").is_empty() || node.undone("delete-mid").is_empty() {
        assert!(Instant::now() < deadline, "held-undo never started compensating");
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
    node.call("/allow", "rpc-fence");
    node.call("/allow", "delete-mid");
    assert_eq!(node.settled("rpc-fence")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("rpc-fence")), ["b", "a"]);
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(steps(&node.undone("delete-mid")), ["b"]);
    assert!(node.status("delete-mid")["error"]
        .as_str()
        .unwrap()
        .contains("instance does not exist"));
}

#[test]
fn code_drift_fails_closed() {
    let mut node = Node::start();
    for id in ["drift-missing", "drift-config"] {
        node.create(id, serde_json::json!({ "scenario": "drift" }));
        node.wait_for_status(id, "waiting");
    }
    let terminate = "/terminate?options=%7B%22rollback%22%3Atrue%7D";

    // A deploy that drops the handler fails the rollback instead of
    // skipping the step.
    node.set_drift("none");
    node.restart();
    node.call(terminate, "drift-missing");
    let status = node.settled("drift-missing");
    assert_eq!(status["rollback"]["outcome"], "failed");
    assert!(status["rollback"]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("did not register its handler"));
    assert!(node.undone("drift-missing").is_empty());

    // A changed rollbackConfig fails closed too, until retryRollback()
    // accepts the deployed config.
    node.set_drift("config");
    node.restart();
    node.call(terminate, "drift-config");
    let status = node.settled("drift-config");
    assert_eq!(status["rollback"]["outcome"], "failed");
    assert!(status["rollback"]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("rollbackConfig of step \"a\" changed"));
    assert!(node.undone("drift-config").is_empty());
    node.call("/retry-rollback", "drift-config");
    assert_eq!(node.settled("drift-config")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("drift-config")), ["a"]);

    // Restoring the handler lets retryRollback() finish the other one.
    node.set_drift("handler");
    node.restart();
    node.call("/retry-rollback", "drift-missing");
    assert_eq!(node.settled("drift-missing")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("drift-missing")), ["a"]);
}

/// LOAD-01: many instances waiting out a rollback backoff at once. Each
/// handler fails until allowed, with a 6-second backoff. The persisted
/// deadlines must hold under load (no hot retry loop), the cells must share
/// a bounded set of isolates, and every rollback must still finish.
#[test]
fn many_rollbacks_backing_off_stay_bounded() {
    const INSTANCES: usize = 50;
    const BACKOFF_MS: f64 = 6000.0;
    let node = Node::start();
    let ids: Vec<String> = (0..INSTANCES).map(|index| format!("load-{index}")).collect();
    for id in &ids {
        node.create(id, serde_json::json!({ "scenario": "backoff" }));
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while ids
        .iter()
        .any(|id| !node.events(id).iter().any(|event| event == "attempt_errored:a"))
    {
        assert!(Instant::now() < deadline, "not every rollback reached its backoff");
        std::thread::sleep(Duration::from_millis(200));
    }

    // Every cell is owned, but the isolates that run them stay few.
    let state = node.state();
    let cells = state["deployment"]["cells"].as_object().unwrap();
    let workflows = cells.keys().filter(|cell| cell.starts_with("__Workflow.")).count();
    assert_eq!(workflows, INSTANCES);
    let live = state["deployment"]["isolates"]["cells"]["workflow-rollback"]["live"]
        .as_u64()
        .unwrap();
    assert!(live <= 8, "{live} live isolates for {INSTANCES} instances");

    // Let one more round of retries fail, then allow the handlers.
    std::thread::sleep(Duration::from_millis(BACKOFF_MS as u64 + 1000));
    for id in &ids {
        node.call("/allow", id);
    }
    for id in &ids {
        assert_eq!(node.settled(id)["rollback"], rollback("complete"), "{id}");
    }

    // No attempt started before its persisted backoff elapsed.
    let log = node.node_log();
    for id in &ids {
        let starts: Vec<f64> = log
            .lines()
            .filter(|line| line.contains("kind=attempt_started"))
            .filter(|line| line.contains(&format!(" instance={id} ")))
            .filter_map(log_millis)
            .collect();
        assert!(starts.len() >= 2, "{id} attempts: {starts:?}");
        for pair in starts.windows(2) {
            assert!(
                pair[1] - pair[0] >= BACKOFF_MS - 50.0,
                "{id} retried after {} ms",
                pair[1] - pair[0]
            );
        }
    }
}

/// LIFE-01 (celld #289, #294): clients that hang up on terminate({rollback})
/// and retryRollback() while they wait. The rollback must finish on its own,
/// once, leave no stuck work on the instance, and let the node drain fast,
/// also with a waiting call still open when it stops.
#[test]
fn cancelled_rollback_calls_leave_nothing_stuck() {
    let mut node = Node::start();
    for id in ["cancel-terminate", "drain-terminate"] {
        node.create(id, serde_json::json!({ "scenario": "slow-sleeper" }));
    }
    node.create("cancel-retry", serde_json::json!({ "scenario": "halt" }));
    for id in ["cancel-terminate", "drain-terminate"] {
        node.wait_for_status(id, "waiting");
    }
    let terminate = "/terminate?options=%7B%22rollback%22%3Atrue%7D";

    // Hang up while terminate({rollback: true}) waits on a 3-second handler.
    node.cancel_get(&format!("{terminate}&id=cancel-terminate"), Duration::from_millis(300));
    let status = node.settled("cancel-terminate");
    assert_eq!(status["status"], "terminated");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("cancel-terminate")), ["a"]);

    // Hang up while retryRollback() waits.
    assert_eq!(node.settled("cancel-retry")["rollback"]["outcome"], "failed");
    node.call("/allow", "cancel-retry");
    node.cancel_get("/retry-rollback?id=cancel-retry", Duration::from_millis(50));
    assert_eq!(node.settled("cancel-retry")["rollback"], rollback("complete"));
    assert_eq!(steps(&node.undone("cancel-retry")), ["b", "b", "b", "a"]);

    // Nothing is left pending on either instance: each answers at once,
    // and the cancelled callers left no duplicate handler run behind.
    std::thread::sleep(Duration::from_millis(3500));
    for id in ["cancel-terminate", "cancel-retry"] {
        let started = Instant::now();
        node.status(id);
        assert!(started.elapsed() < Duration::from_secs(2), "{id} status was slow");
    }
    assert_eq!(steps(&node.undone("cancel-terminate")), ["a"]);
    assert_eq!(steps(&node.undone("cancel-retry")), ["b", "b", "b", "a"]);

    // A terminate({rollback}) still waiting when the node stops cannot hold
    // the drain to its deadline.
    let port = node.port;
    let waiting = std::thread::spawn(move || http_get(port, &format!("{terminate}&id=drain-terminate")));
    std::thread::sleep(Duration::from_millis(300));
    let stopped = node.stop_gracefully();
    let _ = waiting.join();
    assert!(stopped < Duration::from_secs(20), "the node took {stopped:?} to stop");
    assert!(
        !node.node_log().contains("drain reached its"),
        "the drain hit its deadline"
    );

    // The interrupted rollback finishes after a restart.
    node.spawn(false);
    let status = node.settled("drain-terminate");
    assert_eq!(status["status"], "terminated");
    assert_eq!(status["rollback"], rollback("complete"));
    assert!(!node.undone("drain-terminate").is_empty());
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

/// MinIO, pinned by digest, as the fleet bucket. Docker Hub no longer
/// serves the upstream image; Chainguard publishes this build.
const MINIO_IMAGE: &str = "cgr.dev/chainguard/minio@sha256:268d68a2a3b6fc65f905a61f84d7eab6345ce6bff14f551e2b028ec31e6a1341";
const BUCKET_USER: &str = "celld";
const BUCKET_SECRET: &str = "celld-secret";

/// A two-node fleet on a MinIO bucket. Each node keeps its own local state,
/// so killing one is a machine failure: its cells move to the other node
/// once its lease expires.
struct Fleet {
    _dir: tempfile::TempDir,
    container: String,
    endpoint: String,
    nodes: Vec<FleetNode>,
}

struct FleetNode {
    port: u16,
    internal: u16,
    child: Option<Child>,
}

impl Fleet {
    fn start() -> Self {
        let dir = tempfile::tempdir().expect("create the fleet directory");
        let s3 = free_port();
        let container = format!("celld-rollback-{s3}");
        let started = Command::new("docker")
            .args(["run", "-d", "--rm", "--name", &container, "-p"])
            .arg(format!("127.0.0.1:{s3}:9000"))
            .args(["-e", &format!("MINIO_ROOT_USER={BUCKET_USER}")])
            .args(["-e", &format!("MINIO_ROOT_PASSWORD={BUCKET_SECRET}")])
            .args([MINIO_IMAGE, "server", "/data"])
            .stdout(Stdio::null())
            .status()
            .expect("run docker");
        assert!(started.success(), "could not start MinIO in Docker");
        let mut fleet = Self {
            _dir: dir,
            container,
            endpoint: format!("http://127.0.0.1:{s3}"),
            nodes: Vec::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while http_get(s3, "/minio/health/live").is_none() {
            assert!(Instant::now() < deadline, "MinIO did not start");
            std::thread::sleep(Duration::from_millis(200));
        }
        // The health probe can answer before the S3 API accepts requests.
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let created = Command::new("curl")
                .args(["-sf", "-X", "PUT", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user"])
                .arg(format!("{BUCKET_USER}:{BUCKET_SECRET}"))
                .arg(format!("{}/celld-test", fleet.endpoint))
                .stdout(Stdio::null())
                .status()
                .expect("run curl");
            if created.success() {
                break;
            }
            assert!(Instant::now() < deadline, "could not create the bucket");
            std::thread::sleep(Duration::from_millis(250));
        }

        let project = fleet._dir.path().join("project");
        std::fs::create_dir(&project).expect("create the project directory");
        for file in ["wrangler.jsonc", "index.js"] {
            std::fs::copy(Path::new(FIXTURE).join(file), project.join(file))
                .expect("copy the fixture");
        }
        let deployed = fleet
            .celld(&["deploy", project.to_str().expect("a UTF-8 project path")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run celld deploy");
        assert!(deployed.success(), "celld deploy failed");

        for name in ["a", "b"] {
            let port = free_port();
            let internal = free_port();
            let child = fleet
                .celld(&[])
                .args(["--listen", &format!("127.0.0.1:{port}")])
                .args(["--internal-listen", &format!("127.0.0.1:{internal}")])
                .args(["--advertise", &format!("127.0.0.1:{internal}")])
                .env("CELLD_WATCH", fleet._dir.path().join(name))
                .env("CELLD_NODE", format!("node-{name}"))
                .env("CELLD_READY_FLEET_GATE_MS", "0")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start a fleet node");
            fleet.nodes.push(FleetNode {
                port,
                internal,
                child: Some(child),
            });
        }
        for node in &fleet.nodes {
            let deadline = Instant::now() + Duration::from_secs(120);
            while http_get(node.port, "/ready").as_deref() != Some("not found") {
                assert!(Instant::now() < deadline, "a fleet node did not start");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        fleet
    }

    /// `celld [LEADING...] --bucket ... --endpoint ...`; a subcommand and its
    /// operands must come first.
    fn celld(&self, leading: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        command
            .args(leading)
            .args(["--bucket", "s3://celld-test", "--endpoint", &self.endpoint])
            .env("AWS_ACCESS_KEY_ID", BUCKET_USER)
            .env("AWS_SECRET_ACCESS_KEY", BUCKET_SECRET)
            .env("AWS_REGION", "us-east-1")
            .stdin(Stdio::null());
        command
    }

    fn get(&self, node: usize, path: &str) -> Value {
        let body = http_get(self.nodes[node].port, path)
            .unwrap_or_else(|| panic!("GET {path} failed"));
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("GET {path} answered {body}"))
    }

    /// The node whose `/state` lists the Workflow cell among its cells.
    fn workflow_owner(&self) -> Option<usize> {
        self.nodes.iter().position(|node| {
            http_get(node.internal, "/state")
                .and_then(|state| serde_json::from_str::<Value>(&state).ok())
                .and_then(|state| state["deployment"]["cells"].as_object().cloned())
                .is_some_and(|cells| cells.keys().any(|cell| cell.starts_with("__Workflow.")))
        })
    }

    fn kill(&mut self, node: usize) {
        if let Some(mut child) = self.nodes[node].child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for node in 0..self.nodes.len() {
            self.kill(node);
        }
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[test]
#[ignore = "needs Docker; run with --ignored"]
fn fleet_failover_finishes_the_rollback() {
    let mut fleet = Fleet::start();
    let params = percent_encoding::utf8_percent_encode(
        r#"{"scenario":"held-undo"}"#,
        percent_encoding::NON_ALPHANUMERIC,
    )
    .to_string();
    assert_eq!(
        fleet.get(0, &format!("/create?id=failover&params={params}")),
        serde_json::json!({ "ok": true })
    );

    // Kill the owner while the handler of step b is held.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !fleet.get(0, "/log?id=failover").to_string().contains(r#""step":"b""#) {
        assert!(Instant::now() < deadline, "the rollback never started");
        std::thread::sleep(Duration::from_millis(50));
    }
    let owner = fleet.workflow_owner().expect("one node owns the Workflow cell");
    fleet.kill(owner);
    let survivor = 1 - owner;
    // The Log object may have lived on the killed node too; it answers once
    // its cell has moved.
    let deadline = Instant::now() + Duration::from_secs(90);
    while fleet.get(survivor, "/allow?id=failover") != serde_json::json!({ "ok": true }) {
        assert!(Instant::now() < deadline, "the Log cell never moved to the survivor");
        std::thread::sleep(Duration::from_millis(250));
    }

    let deadline = Instant::now() + Duration::from_secs(90);
    let status = loop {
        let body = http_get(fleet.nodes[survivor].port, "/status?id=failover");
        if let Some(status) = body.and_then(|body| serde_json::from_str::<Value>(&body).ok()) {
            if status["rollback"].is_object() {
                break status;
            }
        }
        assert!(Instant::now() < deadline, "the survivor never finished the rollback");
        std::thread::sleep(Duration::from_millis(250));
    };
    assert_eq!(status["status"], "errored");
    assert_eq!(status["rollback"], rollback("complete"));
    assert_eq!(fleet.workflow_owner(), Some(survivor));
    // The interrupted handler runs again on the survivor, then the chain
    // continues: at least once, in order.
    let log = fleet.get(survivor, "/log?id=failover");
    let undone: Vec<&str> = log
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["step"].as_str())
        .filter(|step| !step.starts_with("fwd:"))
        .collect();
    assert_eq!(undone, ["b", "b", "a"]);
}
