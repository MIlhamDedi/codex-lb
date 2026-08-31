use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_tungstenite::accept_async_with_config;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;

#[tokio::test]
async fn missing_pong_emits_liveness_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind websocket server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept websocket client");
        let mut extensions = ExtensionsConfig::default();
        extensions.permessage_deflate = Some(DeflateConfig::default());
        let mut config = WebSocketConfig::default();
        config.extensions = extensions;
        let _websocket = accept_async_with_config(stream, Some(config))
            .await
            .expect("accept websocket handshake");
        // Do not poll the server stream: tungstenite therefore cannot observe
        // or automatically answer the helper's ping.
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    let mut helper = Command::new(env!("CARGO_BIN_EXE_codex-lb-native-egress"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn native helper");
    let mut stdin = helper.stdin.take().expect("helper stdin");
    let stdout = helper.stdout.take().expect("helper stdout");
    let mut lines = BufReader::new(stdout).lines();
    let hello = json!({
        "type": "client_hello",
        "min_protocol_version": 1,
        "max_protocol_version": 1
    });
    stdin
        .write_all(format!("{hello}\n").as_bytes())
        .await
        .expect("send protocol handshake");
    let ready: Value = serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("handshake timeout")
            .expect("read handshake event")
            .expect("handshake event line"),
    )
    .expect("decode handshake event");
    assert_eq!(ready["type"], "server_hello");
    assert_eq!(ready["protocol_version"], 1);

    let connect = json!({
        "type": "websocket_connect",
        "request_id": "liveness-test",
        "url": format!("ws://{address}/v1/responses"),
        "headers": [],
        "connect_timeout_ms": 2_000,
        "max_message_bytes": 1_024,
        "ping_interval_ms": 20,
        "ping_timeout_ms": 40,
        "proxy_url": null
    });
    stdin
        .write_all(format!("{connect}\n").as_bytes())
        .await
        .expect("send websocket command");

    let open: Value = serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("open event timeout")
            .expect("read open event")
            .expect("open event line"),
    )
    .expect("decode open event");
    assert_eq!(open["type"], "websocket_open");

    let failure: Value = serde_json::from_str(
        &tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("liveness event timeout")
            .expect("read liveness event")
            .expect("liveness event line"),
    )
    .expect("decode liveness event");
    assert_eq!(failure["type"], "websocket_error");
    assert_eq!(failure["failure_phase"], "liveness_timeout");
    assert_eq!(failure["retryable_same_contract"], false);

    drop(stdin);
    tokio::time::timeout(Duration::from_secs(2), helper.wait())
        .await
        .expect("helper exit timeout")
        .expect("wait for helper");
    server.await.expect("websocket server task");
}
