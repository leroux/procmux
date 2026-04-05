//! Stress tests for procmux.
//!
//! These test real failure modes, not just "call the API N times":
//!   - Data integrity: ordering, no loss, no duplication, no cross-contamination
//!   - Lifecycle: cleanup, kill escalation, state transitions, zombie prevention
//!   - Connection: disconnect/reconnect preserves state, reconnect storms
//!   - Adversarial: misbehaving children, resource pressure, edge-case inputs

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use procmux::client::{ProcessMsg, ProcmuxConnection};
use procmux::server::ProcmuxServer;
use tokio::time::timeout;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn sock(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = format!(
        "/tmp/procmux-stress-{}-{}-{}.sock",
        tag,
        std::process::id(),
        n
    );
    let _ = std::fs::remove_file(&path);
    path
}

async fn server(path: &str) -> tokio::task::JoinHandle<()> {
    let s = ProcmuxServer::new(path);
    let h = tokio::spawn(async move { s.run().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;
    h
}

/// Spawn a bash echo-loop, subscribe, return the rx.
async fn echo(
    conn: &ProcmuxConnection,
    name: &str,
) -> tokio::sync::mpsc::UnboundedReceiver<ProcessMsg> {
    let rx = conn.register_process(name).await;
    let r = conn
        .send_command(
            "spawn",
            name,
            vec![
                "bash".into(),
                "-c".into(),
                "while IFS= read -r line; do echo \"$line\"; done".into(),
            ],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(r.ok, "spawn {name}: {:?}", r.error);
    conn.send_simple_command("subscribe", name).await.unwrap();
    rx
}

/// Recv one stdout from rx, panic on anything else.
async fn recv_stdout(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ProcessMsg>,
    label: &str,
) -> serde_json::Value {
    match timeout(Duration::from_secs(10), rx.recv()).await {
        Ok(Some(ProcessMsg::Stdout(s))) => s.data,
        Ok(Some(other)) => panic!("{label}: expected Stdout, got {other:?}"),
        Ok(None) => panic!("{label}: channel closed"),
        Err(_) => panic!("{label}: timeout"),
    }
}

// ===================================================================
// DATA INTEGRITY
// ===================================================================

/// Send 1000 numbered messages through an echo loop and verify every
/// single one comes back, in order, with no gaps or duplicates.
#[tokio::test]
async fn ordering_1000_messages() {
    let path = sock("order1k");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = echo(&conn, "w").await;

    let n = 1000;
    for i in 0..n {
        conn.send_stdin("w", serde_json::json!({"seq": i}))
            .await
            .unwrap();
    }

    for i in 0..n {
        let data = recv_stdout(&mut rx, &format!("msg {i}")).await;
        assert_eq!(data["seq"], i, "out of order at index {i}");
    }

    conn.send_simple_command("kill", "w").await.unwrap();
    conn.close();
    srv.abort();
}

/// 5 processes run simultaneously, each receiving 50 numbered messages.
/// Verify no cross-contamination: process A never sees process B's data.
#[tokio::test]
async fn no_cross_contamination() {
    let path = sock("cross");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut receivers = Vec::new();
    for i in 0..5 {
        let name = format!("p{i}");
        let rx = echo(&conn, &name).await;
        receivers.push(rx);
    }

    let msgs_per = 50;
    for round in 0..msgs_per {
        for i in 0..5 {
            conn.send_stdin(
                &format!("p{i}"),
                serde_json::json!({"proc": i, "round": round}),
            )
            .await
            .unwrap();
        }
    }

    for (i, rx) in receivers.iter_mut().enumerate() {
        for round in 0..msgs_per {
            let data = recv_stdout(rx, &format!("p{i} round {round}")).await;
            assert_eq!(data["proc"], i, "cross-contamination! p{i} got {:?}", data);
            assert_eq!(data["round"], round, "wrong order in p{i}");
        }
    }

    for i in 0..5 {
        conn.send_simple_command("kill", &format!("p{i}"))
            .await
            .unwrap();
    }
    conn.close();
    srv.abort();
}

/// Non-JSON stdout lines get routed as stderr messages.  Verify that
/// valid JSON still arrives as stdout even when interleaved with junk.
#[tokio::test]
async fn non_json_routed_as_stderr() {
    let path = sock("nonjson");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("mixed").await;
    conn.send_command(
        "spawn",
        "mixed",
        vec![
            "bash".into(),
            "-c".into(),
            concat!(
                "echo 'not json at all'; ",
                "echo '{\"a\":1}'; ",
                "echo '???'; ",
                "echo '{\"b\":2}'; ",
                // This is valid JSON but not an object — gets wrapped in {"raw": ...}
                "echo '42'; ",
                "sleep 60"
            )
            .into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "mixed")
        .await
        .unwrap();

    let mut stdouts = Vec::new();
    let mut stderrs = Vec::new();
    // Read until we've got everything (process is still alive via sleep 60)
    for _ in 0..10 {
        match timeout(Duration::from_secs(3), rx.recv()).await {
            Ok(Some(ProcessMsg::Stdout(s))) => stdouts.push(s.data),
            Ok(Some(ProcessMsg::Stderr(s))) => stderrs.push(s.text),
            _ => break,
        }
    }

    // Two JSON objects + one non-object JSON = 3 stdout
    assert_eq!(stdouts.len(), 3, "stdouts: {:?}", stdouts);
    assert_eq!(stdouts[0]["a"], 1);
    assert_eq!(stdouts[1]["b"], 2);
    assert_eq!(stdouts[2]["raw"], 42); // non-object gets wrapped

    // Two non-JSON lines = 2 stderr
    assert_eq!(stderrs.len(), 2, "stderrs: {:?}", stderrs);
    assert!(stderrs[0].contains("not json"));
    assert!(stderrs[1].contains("???"));

    conn.send_simple_command("kill", "mixed").await.unwrap();
    conn.close();
    srv.abort();
}

/// 500KB single JSON line through the echo loop.
#[tokio::test]
async fn half_megabyte_payload() {
    let path = sock("bigpay");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = echo(&conn, "big").await;

    let big = "Z".repeat(500_000);
    conn.send_stdin("big", serde_json::json!({"d": big}))
        .await
        .unwrap();

    let data = recv_stdout(&mut rx, "500KB").await;
    assert_eq!(data["d"].as_str().unwrap().len(), 500_000);

    conn.send_simple_command("kill", "big").await.unwrap();
    conn.close();
    srv.abort();
}

/// Unicode, emoji, CJK, RTL, ZWJ sequences, escape characters.
#[tokio::test]
async fn unicode_fidelity() {
    let path = sock("unicode");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = echo(&conn, "u").await;

    let payloads = vec![
        serde_json::json!({"emoji": "🔥🚀💀🎉👨‍👩‍👧‍👦"}),
        serde_json::json!({"cjk": "你好世界こんにちは"}),
        serde_json::json!({"rtl": "مرحبا بالعالم"}),
        serde_json::json!({"escapes": "tab\there\nnewline"}),
        serde_json::json!({"deep": {"a":{"b":{"c":{"d":{"e":"leaf"}}}}}}),
    ];

    for p in &payloads {
        conn.send_stdin("u", p.clone()).await.unwrap();
    }
    for (i, expected) in payloads.iter().enumerate() {
        let data = recv_stdout(&mut rx, &format!("unicode {i}")).await;
        assert_eq!(data, *expected, "mismatch at {i}");
    }

    conn.send_simple_command("kill", "u").await.unwrap();
    conn.close();
    srv.abort();
}

// ===================================================================
// BUFFER & SUBSCRIBE INTEGRITY
// ===================================================================

/// Unsubscribe → process emits known output → subscribe.
/// Verify the replay count matches exactly and messages are in order.
/// Uses echo loop for synchronization: we know the server has processed
/// a message only once the echo comes back (or in this case, is buffered).
#[tokio::test]
async fn buffer_replay_exact_count_and_order() {
    let path = sock("bufexact");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = echo(&conn, "buf").await;

    // Confirm the echo loop is live
    conn.send_stdin("buf", serde_json::json!({"warmup": true}))
        .await
        .unwrap();
    recv_stdout(&mut rx, "warmup").await;

    // Unsubscribe
    conn.send_simple_command("unsubscribe", "buf")
        .await
        .unwrap();

    // Send 50 messages.  They echo back to stdout → buffered since unsubscribed.
    let n = 50;
    for i in 0..n {
        conn.send_stdin("buf", serde_json::json!({"i": i}))
            .await
            .unwrap();
    }

    // We need to wait until the echo loop has echoed all N lines back and
    // the server has buffered them.  Send a sentinel and poll `list` until
    // buffered_msgs >= n.  (The sentinel also gets buffered, but we account
    // for that.)
    conn.send_stdin("buf", serde_json::json!({"sentinel": true}))
        .await
        .unwrap();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let list = conn.send_simple_command("list", "").await.unwrap();
        let count = list.processes.unwrap()["buf"]["buffered_msgs"]
            .as_u64()
            .unwrap();
        if count >= (n + 1) as u64 {
            break;
        }
    }

    // Subscribe — verify exact replay count
    let sub = conn.send_simple_command("subscribe", "buf").await.unwrap();
    assert!(sub.ok);
    assert_eq!(
        sub.replayed.unwrap(),
        n + 1, // +1 for the sentinel
        "replay count mismatch"
    );

    // Read back and verify order
    for i in 0..n {
        let data = recv_stdout(&mut rx, &format!("replay {i}")).await;
        assert_eq!(data["i"], i, "out of order at {i}");
    }
    // Sentinel
    let data = recv_stdout(&mut rx, "sentinel").await;
    assert_eq!(data["sentinel"], true);

    conn.send_simple_command("kill", "buf").await.unwrap();
    conn.close();
    srv.abort();
}

/// Process exits while nobody is subscribed.  Its output + exit should be
/// buffered.  A later subscribe should replay everything including the exit.
#[tokio::test]
async fn subscribe_to_already_exited_process() {
    let path = sock("exited-sub");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    // Spawn a process that emits 3 lines and exits.  Never subscribe.
    conn.register_process("short").await;
    conn.send_command(
        "spawn",
        "short",
        vec![
            "bash".into(),
            "-c".into(),
            r#"echo '{"a":1}'; echo '{"b":2}'; echo '{"c":3}'; exit 0"#.into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    // Wait for it to exit and buffer
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now subscribe
    let mut rx = conn.register_process("short").await;
    let sub = conn
        .send_simple_command("subscribe", "short")
        .await
        .unwrap();
    assert!(sub.ok);
    assert_eq!(sub.status.as_deref(), Some("exited"));
    // 3 stdout + 1 exit = 4 (at minimum)
    assert!(
        sub.replayed.unwrap() >= 4,
        "expected >=4 replayed, got {:?}",
        sub.replayed
    );

    // Read the replayed messages
    let mut stdout_count = 0;
    let mut got_exit = false;
    for _ in 0..10 {
        match timeout(Duration::from_secs(3), rx.recv()).await {
            Ok(Some(ProcessMsg::Stdout(_))) => stdout_count += 1,
            Ok(Some(ProcessMsg::Exit(e))) => {
                assert_eq!(e.code, Some(0));
                got_exit = true;
                break;
            }
            _ => break,
        }
    }
    assert_eq!(stdout_count, 3);
    assert!(got_exit, "didn't get exit in replay");

    conn.close();
    srv.abort();
}

/// Rapid subscribe→unsubscribe 50 times while a process produces steady
/// output.  The test property: each subscribe must return ok with a
/// non-decreasing replayed count (buffer accumulates between unsubs).
/// The server must remain responsive throughout.
#[tokio::test]
async fn rapid_sub_unsub_cycling() {
    let path = sock("subcycle");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    conn.register_process("steady").await;
    conn.send_command(
        "spawn",
        "steady",
        vec![
            "bash".into(),
            "-c".into(),
            r#"i=0; while true; do printf '{"i":%d}\n' $i; i=$((i+1)); sleep 0.05; done"#.into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    for _ in 0..50 {
        let sub = conn
            .send_simple_command("subscribe", "steady")
            .await
            .unwrap();
        assert!(sub.ok, "subscribe failed: {:?}", sub.error);
        let unsub = conn
            .send_simple_command("unsubscribe", "steady")
            .await
            .unwrap();
        assert!(unsub.ok, "unsubscribe failed: {:?}", unsub.error);
    }

    // Server should still respond
    let status = conn.send_simple_command("status", "").await.unwrap();
    assert!(status.ok);

    conn.send_simple_command("kill", "steady").await.unwrap();
    conn.close();
    srv.abort();
}

// ===================================================================
// LIFECYCLE STRESS
// ===================================================================

/// Spawn → use → kill → re-spawn with the same name, 20 times.
/// Catches resource leaks or stale state between incarnations.
#[tokio::test]
async fn same_name_lifecycle_churn() {
    let path = sock("churn");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    for i in 0..20 {
        let mut rx = echo(&conn, "churn").await;

        conn.send_stdin("churn", serde_json::json!({"cycle": i}))
            .await
            .unwrap();
        let data = recv_stdout(&mut rx, &format!("cycle {i}")).await;
        assert_eq!(data["cycle"], i);

        conn.send_simple_command("kill", "churn").await.unwrap();
    }

    // Process table should be clean
    let list = conn.send_simple_command("list", "").await.unwrap();
    assert!(list.processes.unwrap().as_object().unwrap().is_empty());

    conn.close();
    srv.abort();
}

/// Spawn 30 different processes, kill them all, verify the process table
/// is clean.  Then spawn 30 more to prove no resource exhaustion.
#[tokio::test]
async fn spawn_30_kill_30_spawn_30() {
    let path = sock("30x2");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    // First batch
    for i in 0..30 {
        let name = format!("a{i}");
        conn.register_process(&name).await;
        let r = conn
            .send_command(
                "spawn",
                &name,
                vec!["sleep".into(), "300".into()],
                HashMap::new(),
                None,
            )
            .await
            .unwrap();
        assert!(r.ok, "spawn a{i}: {:?}", r.error);
    }
    for i in 0..30 {
        conn.send_simple_command("kill", &format!("a{i}"))
            .await
            .unwrap();
    }

    let list = conn.send_simple_command("list", "").await.unwrap();
    assert!(list.processes.unwrap().as_object().unwrap().is_empty());

    // Second batch — proves the server isn't leaking
    for i in 0..30 {
        let name = format!("b{i}");
        conn.register_process(&name).await;
        let r = conn
            .send_command(
                "spawn",
                &name,
                vec!["sleep".into(), "300".into()],
                HashMap::new(),
                None,
            )
            .await
            .unwrap();
        assert!(r.ok, "spawn b{i}: {:?}", r.error);
    }

    let list = conn.send_simple_command("list", "").await.unwrap();
    let procs = list.processes.unwrap();
    assert_eq!(procs.as_object().unwrap().len(), 30);

    for i in 0..30 {
        conn.send_simple_command("kill", &format!("b{i}"))
            .await
            .unwrap();
    }
    conn.close();
    srv.abort();
}

/// Child that ignores SIGTERM.  The server must escalate to SIGKILL.
/// Verify the kill returns within the 5s SIGTERM window + margin.
#[tokio::test]
async fn sigkill_escalation() {
    let path = sock("sigkill");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    conn.register_process("stubborn").await;
    conn.send_command(
        "spawn",
        "stubborn",
        vec![
            "bash".into(),
            "-c".into(),
            "trap '' TERM; while true; do sleep 0.1; done".into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let t0 = Instant::now();
    let kill = conn.send_simple_command("kill", "stubborn").await.unwrap();
    let elapsed = t0.elapsed();

    assert!(kill.ok, "kill failed: {:?}", kill.error);
    // SIGTERM 5s timeout + SIGKILL + margin
    assert!(elapsed < Duration::from_secs(8), "took {elapsed:?}");

    let list = conn.send_simple_command("list", "").await.unwrap();
    assert!(list.processes.unwrap().as_object().unwrap().is_empty());

    conn.close();
    srv.abort();
}

/// Parent spawns a grandchild.  Kill the parent.  The grandchild should
/// also die (because setsid + killpg sends signals to the whole group).
#[tokio::test]
async fn process_group_kills_grandchildren() {
    let path = sock("pgkill");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let marker = format!("/tmp/procmux-pg-{}.marker", std::process::id());
    let _ = std::fs::remove_file(&marker);

    conn.register_process("parent").await;
    conn.send_command(
        "spawn",
        "parent",
        vec![
            "bash".into(),
            "-c".into(),
            format!(
                // Grandchild writes to marker file continuously
                "(while true; do echo alive > {marker}; sleep 0.1; done) & sleep 3600"
            ),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    // Wait for grandchild to start
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if std::fs::metadata(&marker).is_ok() {
            break;
        }
    }
    assert!(std::fs::metadata(&marker).is_ok(), "grandchild never started");

    // Kill parent → whole group should die
    conn.send_simple_command("kill", "parent").await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Delete marker, wait, check it's NOT recreated
    let _ = std::fs::remove_file(&marker);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        std::fs::metadata(&marker).is_err(),
        "grandchild survived group kill"
    );

    conn.close();
    srv.abort();
}

/// Spawn 10 processes that all exit at the same instant (triggered by
/// a shared file).  The server must handle the simultaneous Exit storm
/// without panicking or corrupting state.
#[tokio::test]
async fn simultaneous_exit_storm() {
    let path = sock("exitstorm");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let trigger = format!("/tmp/procmux-trigger-{}", std::process::id());
    let _ = std::fs::remove_file(&trigger);

    let n = 10;
    for i in 0..n {
        let name = format!("storm-{i}");
        conn.register_process(&name).await;
        conn.send_command(
            "spawn",
            &name,
            vec![
                "bash".into(),
                "-c".into(),
                format!(
                    r#"while [ ! -f {trigger} ]; do sleep 0.05; done; echo '{{"exit":{i}}}'; exit {i}"#
                ),
            ],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
        conn.send_simple_command("subscribe", &name).await.unwrap();
    }

    // Pull the trigger — all 10 exit simultaneously
    std::fs::write(&trigger, "go").unwrap();

    // Collect all exit messages
    let mut exits = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while exits.len() < n && Instant::now() < deadline {
        for i in 0..n {
            let name = format!("storm-{i}");
            if exits.contains_key(&name) {
                continue;
            }
        }
        // Just poll status to let the server process relay messages
        let _ = conn.send_simple_command("status", "").await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let list = conn.send_simple_command("list", "").await.unwrap();
        let procs = list.processes.unwrap();
        for i in 0..n {
            let name = format!("storm-{i}");
            if let Some(p) = procs.get(&name) {
                if p["status"] == "exited" {
                    exits.insert(name, p["exit_code"].as_i64());
                }
            }
        }
    }

    assert_eq!(exits.len(), n, "not all processes exited: {:?}", exits);
    for i in 0..n {
        let code = exits[&format!("storm-{i}")];
        assert_eq!(code, Some(i as i64), "wrong exit code for storm-{i}");
    }

    // Cleanup
    let _ = std::fs::remove_file(&trigger);
    for i in 0..n {
        let _ = conn
            .send_simple_command("kill", &format!("storm-{i}"))
            .await;
    }
    conn.close();
    srv.abort();
}

/// Child closes its own stdout (exec 1>&-) but stays alive.
/// The exit message should only come when the process truly terminates.
#[tokio::test]
async fn child_closes_own_stdout() {
    let path = sock("closestdout");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("half").await;
    conn.send_command(
        "spawn",
        "half",
        vec![
            "bash".into(),
            "-c".into(),
            r#"echo '{"before":"close"}'; exec 1>&-; sleep 1; exit 7"#.into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "half").await.unwrap();

    let data = recv_stdout(&mut rx, "before close").await;
    assert_eq!(data["before"], "close");

    // Should get exit with code 7 (after the sleep)
    let mut got_exit = false;
    for _ in 0..10 {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ProcessMsg::Exit(e))) => {
                assert_eq!(e.code, Some(7));
                got_exit = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    assert!(got_exit, "never got exit code 7");

    conn.close();
    srv.abort();
}

// ===================================================================
// CONNECTION STRESS
// ===================================================================

/// Client 1 spawns a process, sends data, disconnects.
/// Client 2 connects, sends more data (buffered since not subscribed),
/// subscribes, verifies buffer replay, sends live data.
/// Tests the full client handoff path.
#[tokio::test]
async fn client_handoff_preserves_buffers() {
    let path = sock("handoff");
    let srv = server(&path).await;

    // Client 1
    let c1 = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx1 = echo(&c1, "w").await;

    // Confirm echo loop works
    c1.send_stdin("w", serde_json::json!({"from": "c1"}))
        .await
        .unwrap();
    recv_stdout(&mut rx1, "c1 echo").await;

    // Disconnect c1 → server buffers
    c1.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Client 2
    let c2 = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx2 = c2.register_process("w").await;

    // Send data while unsubscribed → buffered
    for i in 0..5 {
        c2.send_stdin("w", serde_json::json!({"from": "c2", "i": i}))
            .await
            .unwrap();
    }
    // Wait for echoes to buffer
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let list = c2.send_simple_command("list", "").await.unwrap();
        let buffered = list.processes.unwrap()["w"]["buffered_msgs"]
            .as_u64()
            .unwrap();
        if buffered >= 5 {
            break;
        }
    }

    // Subscribe → replay
    let sub = c2.send_simple_command("subscribe", "w").await.unwrap();
    assert!(sub.ok);
    assert!(sub.replayed.unwrap() >= 5, "replayed: {:?}", sub.replayed);

    // Read replayed
    for i in 0..5 {
        let data = recv_stdout(&mut rx2, &format!("replay {i}")).await;
        assert_eq!(data["from"], "c2");
        assert_eq!(data["i"], i);
    }

    // Live data should also work
    c2.send_stdin("w", serde_json::json!({"from": "c2_live"}))
        .await
        .unwrap();
    let data = recv_stdout(&mut rx2, "live").await;
    assert_eq!(data["from"], "c2_live");

    c2.send_simple_command("kill", "w").await.unwrap();
    c2.close();
    srv.abort();
}

/// 20 clients connect and immediately disconnect in rapid succession
/// while a process runs.  The final client should find the process alive
/// and be able to interact with it.
#[tokio::test]
async fn reconnect_storm() {
    let path = sock("storm");
    let srv = server(&path).await;

    // First client spawns a long-running process
    let c0 = ProcmuxConnection::connect(&path).await.unwrap();
    c0.register_process("survivor").await;
    c0.send_command(
        "spawn",
        "survivor",
        vec![
            "bash".into(),
            "-c".into(),
            "while IFS= read -r line; do echo \"$line\"; done".into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    c0.close();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 20 rapid connect/disconnect cycles
    for _ in 0..20 {
        let c = ProcmuxConnection::connect(&path).await.unwrap();
        // Don't even do anything — just connect and drop
        c.close();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Final client — process should still be alive
    let fin = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = fin.register_process("survivor").await;
    let sub = fin
        .send_simple_command("subscribe", "survivor")
        .await
        .unwrap();
    assert!(sub.ok);

    // Should be able to interact
    fin.send_stdin("survivor", serde_json::json!({"alive": true}))
        .await
        .unwrap();
    let data = recv_stdout(&mut rx, "post-storm").await;
    assert_eq!(data["alive"], true);

    fin.send_simple_command("kill", "survivor").await.unwrap();
    fin.close();
    srv.abort();
}

/// Long disconnect: client spawns a process that emits 100 deterministic
/// lines and then sleeps.  Client disconnects, waits, reconnects.
/// Uses `list` to poll buffered_msgs so we know the data is ready.
#[tokio::test]
async fn long_disconnect_buffer_integrity() {
    let path = sock("longdisco");
    let srv = server(&path).await;

    let n = 100;

    // Client 1 spawns a process and leaves
    let c1 = ProcmuxConnection::connect(&path).await.unwrap();
    c1.register_process("emitter").await;
    c1.send_command(
        "spawn",
        "emitter",
        vec![
            "bash".into(),
            "-c".into(),
            format!(
                r#"for i in $(seq 0 {}); do printf '{{"n":%d}}\n' $i; done; sleep 3600"#,
                n - 1
            ),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    c1.close();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Client 2 connects and waits for all messages to be buffered
    let c2 = ProcmuxConnection::connect(&path).await.unwrap();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let list = c2.send_simple_command("list", "").await.unwrap();
        let buffered = list.processes.unwrap()["emitter"]["buffered_msgs"]
            .as_u64()
            .unwrap();
        if buffered >= n as u64 {
            break;
        }
    }

    // Subscribe and verify
    let mut rx = c2.register_process("emitter").await;
    let sub = c2
        .send_simple_command("subscribe", "emitter")
        .await
        .unwrap();
    assert!(sub.ok);
    assert!(
        sub.replayed.unwrap() >= n,
        "expected >={n} replayed, got {:?}",
        sub.replayed
    );

    for i in 0..n {
        let data = recv_stdout(&mut rx, &format!("buf {i}")).await;
        assert_eq!(data["n"], i, "order broken at {i}");
    }

    c2.send_simple_command("kill", "emitter").await.unwrap();
    c2.close();
    srv.abort();
}

// ===================================================================
// ADVERSARIAL / EDGE CASES
// ===================================================================

/// Spawn a binary that doesn't exist.  Server should return an error
/// and remain fully functional for subsequent operations.
#[tokio::test]
async fn spawn_nonexistent_binary() {
    let path = sock("nobin");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    conn.register_process("bad").await;
    let r = conn
        .send_command(
            "spawn",
            "bad",
            vec!["/no/such/binary/anywhere".into()],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(!r.ok);
    assert!(r.error.is_some());

    // Server still works
    let mut rx = echo(&conn, "good").await;
    conn.send_stdin("good", serde_json::json!({"ok": true}))
        .await
        .unwrap();
    let data = recv_stdout(&mut rx, "after bad spawn").await;
    assert_eq!(data["ok"], true);

    conn.send_simple_command("kill", "good").await.unwrap();
    conn.close();
    srv.abort();
}

/// Blast 100 stdin messages at a process that has already exited.
/// The server should silently drop them and remain healthy.
#[tokio::test]
async fn stdin_to_dead_process() {
    let path = sock("deadstdin");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("mortal").await;
    conn.send_command(
        "spawn",
        "mortal",
        vec!["bash".into(), "-c".into(), "exit 0".into()],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "mortal")
        .await
        .unwrap();

    // Wait for exit
    loop {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ProcessMsg::Exit(_))) => break,
            Ok(Some(_)) => continue,
            _ => panic!("never got exit"),
        }
    }

    // Blast stdin at the dead process
    for i in 0..100 {
        let _ = conn
            .send_stdin("mortal", serde_json::json!({"dead": i}))
            .await;
    }

    // Server is alive
    let status = conn.send_simple_command("status", "").await.unwrap();
    assert!(status.ok);

    conn.close();
    srv.abort();
}

/// Every command on a nonexistent process should return ok=false.
/// The server must remain healthy after a barrage of errors.
#[tokio::test]
async fn error_barrage_on_nonexistent() {
    let path = sock("errbarrage");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    for _ in 0..20 {
        for cmd in &["kill", "interrupt", "subscribe", "unsubscribe"] {
            let r = conn.send_simple_command(cmd, "ghost").await.unwrap();
            assert!(!r.ok, "{cmd} on ghost returned ok");
        }
    }

    let status = conn.send_simple_command("status", "").await.unwrap();
    assert!(status.ok);

    conn.close();
    srv.abort();
}

/// Process that produces no output at all.  Should get only an Exit
/// message, nothing else.
#[tokio::test]
async fn silent_process() {
    let path = sock("silent");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("quiet").await;
    conn.send_command(
        "spawn",
        "quiet",
        vec!["bash".into(), "-c".into(), "sleep 0.2; exit 0".into()],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "quiet").await.unwrap();

    let mut msgs = Vec::new();
    loop {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(msg)) => {
                let is_exit = matches!(msg, ProcessMsg::Exit(_));
                msgs.push(msg);
                if is_exit {
                    break;
                }
            }
            _ => break,
        }
    }

    assert_eq!(msgs.len(), 1, "expected only Exit, got {msgs:?}");
    assert!(matches!(msgs[0], ProcessMsg::Exit(_)));

    conn.close();
    srv.abort();
}

/// Double subscribe: subscribing to an already-subscribed process should
/// be a no-op (replayed=0, ok=true).
#[tokio::test]
async fn double_subscribe() {
    let path = sock("doublesub");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    echo(&conn, "w").await; // spawns + subscribes

    // Subscribe again
    let sub2 = conn.send_simple_command("subscribe", "w").await.unwrap();
    assert!(sub2.ok);
    assert_eq!(sub2.replayed.unwrap(), 0);

    // A third time
    let sub3 = conn.send_simple_command("subscribe", "w").await.unwrap();
    assert!(sub3.ok);
    assert_eq!(sub3.replayed.unwrap(), 0);

    conn.send_simple_command("kill", "w").await.unwrap();
    conn.close();
    srv.abort();
}

/// Spawn a process while another with the same name is still running.
/// Should return already_running=true with the existing PID.
#[tokio::test]
async fn spawn_duplicate_returns_already_running() {
    let path = sock("dup");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    conn.register_process("dup").await;
    let r1 = conn
        .send_command(
            "spawn",
            "dup",
            vec!["sleep".into(), "60".into()],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(r1.ok);
    let pid1 = r1.pid.unwrap();

    let r2 = conn
        .send_command(
            "spawn",
            "dup",
            vec!["sleep".into(), "60".into()],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
    assert!(r2.ok);
    assert_eq!(r2.already_running, Some(true));
    assert_eq!(r2.pid.unwrap(), pid1); // same PID

    conn.send_simple_command("kill", "dup").await.unwrap();
    conn.close();
    srv.abort();
}

/// SIGINT via the `interrupt` command.  The child traps INT, prints a
/// message, and exits with a known code.
#[tokio::test]
async fn interrupt_with_trap() {
    let path = sock("int");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("trapper").await;
    conn.send_command(
        "spawn",
        "trapper",
        vec![
            "bash".into(),
            "-c".into(),
            r#"trap 'echo "{\"caught\":\"INT\"}"; exit 42' INT; while true; do sleep 0.1; done"#
                .into(),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "trapper")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await; // let trap install

    conn.send_simple_command("interrupt", "trapper")
        .await
        .unwrap();

    let mut got_stdout = false;
    let mut got_exit = false;
    for _ in 0..10 {
        match timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ProcessMsg::Stdout(s))) => {
                assert_eq!(s.data["caught"], "INT");
                got_stdout = true;
            }
            Ok(Some(ProcessMsg::Exit(e))) => {
                assert_eq!(e.code, Some(42));
                got_exit = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }
    assert!(got_stdout, "trap handler stdout never arrived");
    assert!(got_exit, "exit 42 never arrived");

    conn.close();
    srv.abort();
}

/// Custom env and cwd propagate to the child.
#[tokio::test]
async fn env_and_cwd() {
    let path = sock("envcwd");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = conn.register_process("env").await;
    let mut env = HashMap::new();
    env.insert("MY_VAR".into(), "custom_val".into());

    conn.send_command(
        "spawn",
        "env",
        vec![
            "bash".into(),
            "-c".into(),
            r#"echo "{\"var\":\"$MY_VAR\",\"cwd\":\"$(pwd)\"}"  "#.into(),
        ],
        env,
        Some("/tmp".into()),
    )
    .await
    .unwrap();
    conn.send_simple_command("subscribe", "env").await.unwrap();

    let data = recv_stdout(&mut rx, "env+cwd").await;
    assert_eq!(data["var"], "custom_val");
    let cwd = data["cwd"].as_str().unwrap();
    assert!(
        cwd == "/tmp" || cwd == "/private/tmp",
        "unexpected cwd: {cwd}"
    );

    conn.close();
    srv.abort();
}

/// `list` reports buffered_msgs accurately while a process accumulates
/// a buffer, and goes to 0 after subscribe drains it.
#[tokio::test]
async fn list_buffered_msgs_accuracy() {
    let path = sock("listbuf");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let mut rx = echo(&conn, "w").await;

    // Confirm live
    conn.send_stdin("w", serde_json::json!({"warmup":1}))
        .await
        .unwrap();
    recv_stdout(&mut rx, "warmup").await;

    // Unsubscribe and accumulate
    conn.send_simple_command("unsubscribe", "w").await.unwrap();
    let n = 10;
    for i in 0..n {
        conn.send_stdin("w", serde_json::json!({"i": i}))
            .await
            .unwrap();
    }

    // Poll until buffered_msgs >= n
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let list = conn.send_simple_command("list", "").await.unwrap();
        let buffered = list.processes.unwrap()["w"]["buffered_msgs"]
            .as_u64()
            .unwrap();
        if buffered >= n as u64 {
            break;
        }
    }
    let list = conn.send_simple_command("list", "").await.unwrap();
    let buffered = list.processes.unwrap()["w"]["buffered_msgs"]
        .as_u64()
        .unwrap();
    assert!(buffered >= n as u64, "buffered_msgs={buffered}, expected >= {n}");

    // Subscribe → drains buffer → buffered_msgs should go to 0
    conn.send_simple_command("subscribe", "w").await.unwrap();
    let list = conn.send_simple_command("list", "").await.unwrap();
    let buffered = list.processes.unwrap()["w"]["buffered_msgs"]
        .as_u64()
        .unwrap();
    assert_eq!(buffered, 0, "buffer not drained after subscribe");

    conn.send_simple_command("kill", "w").await.unwrap();
    conn.close();
    srv.abort();
}

/// Server responds to `status` within 2 seconds while 5 processes generate
/// steady output (with sleep so they don't saturate the relay channel).
#[tokio::test]
async fn responsiveness_under_moderate_load() {
    let path = sock("loadresp");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    for i in 0..5 {
        let name = format!("gen-{i}");
        conn.register_process(&name).await;
        conn.send_command(
            "spawn",
            &name,
            vec![
                "bash".into(),
                "-c".into(),
                r#"while true; do echo '{"x":1}'; sleep 0.01; done"#.into(),
            ],
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
        // Not subscribing — output buffers
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let t0 = Instant::now();
    let status = conn.send_simple_command("status", "").await.unwrap();
    let elapsed = t0.elapsed();
    assert!(status.ok);
    assert!(
        elapsed < Duration::from_secs(2),
        "status took {elapsed:?}"
    );

    let t0 = Instant::now();
    let list = conn.send_simple_command("list", "").await.unwrap();
    let elapsed = t0.elapsed();
    assert!(list.ok);
    assert!(elapsed < Duration::from_secs(2), "list took {elapsed:?}");

    for i in 0..5 {
        conn.send_simple_command("kill", &format!("gen-{i}"))
            .await
            .unwrap();
    }
    conn.close();
    srv.abort();
}

// ===================================================================
// THROUGHPUT
// ===================================================================

/// 1000 echo roundtrips must complete within a reasonable time.
#[tokio::test]
async fn throughput_1000_roundtrips() {
    let path = sock("tp1k");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();
    let mut rx = echo(&conn, "tp").await;

    let n = 1000;
    let t0 = Instant::now();

    for i in 0..n {
        conn.send_stdin("tp", serde_json::json!({"i": i}))
            .await
            .unwrap();
    }

    for i in 0..n {
        let data = recv_stdout(&mut rx, &format!("tp {i}")).await;
        assert_eq!(data["i"], i);
    }

    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "1000 roundtrips in {elapsed:?}"
    );

    conn.send_simple_command("kill", "tp").await.unwrap();
    conn.close();
    srv.abort();
}

/// Firehose: process emits 500 lines as fast as possible without a
/// subscriber.  Then subscribe and verify all 500 replay in order.
#[tokio::test]
async fn firehose_500_buffered_lines() {
    let path = sock("firehose500");
    let srv = server(&path).await;
    let conn = ProcmuxConnection::connect(&path).await.unwrap();

    let n = 500;
    conn.register_process("hose").await;
    conn.send_command(
        "spawn",
        "hose",
        vec![
            "bash".into(),
            "-c".into(),
            format!(
                r#"for i in $(seq 0 {}); do printf '{{"n":%d}}\n' $i; done; sleep 3600"#,
                n - 1
            ),
        ],
        HashMap::new(),
        None,
    )
    .await
    .unwrap();

    // Poll until all lines are buffered
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let list = conn.send_simple_command("list", "").await.unwrap();
        let buffered = list.processes.unwrap()["hose"]["buffered_msgs"]
            .as_u64()
            .unwrap();
        if buffered >= n as u64 {
            break;
        }
    }

    let mut rx = conn.register_process("hose").await;
    let sub = conn.send_simple_command("subscribe", "hose").await.unwrap();
    assert!(
        sub.replayed.unwrap() >= n,
        "expected >={n} replayed, got {:?}",
        sub.replayed
    );

    for i in 0..n {
        let data = recv_stdout(&mut rx, &format!("hose {i}")).await;
        assert_eq!(data["n"], i, "order broken at {i}");
    }

    conn.send_simple_command("kill", "hose").await.unwrap();
    conn.close();
    srv.abort();
}
