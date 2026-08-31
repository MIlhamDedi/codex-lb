use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use futures_util::SinkExt;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rustls::ClientConfig;
use rustls::RootCertStore;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::Connector;
use tokio_tungstenite::client_async_tls_with_config;
use tokio_tungstenite::proxy::connect_via_proxy;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::TlsError;
use tokio_tungstenite::tungstenite::handshake::client::Response as WebSocketResponse;
use tokio_tungstenite::tungstenite::http::HeaderName as WebSocketHeaderName;
use tokio_tungstenite::tungstenite::http::HeaderValue as WebSocketHeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::proxy::ProxyConfig;
use tungstenite::Bytes;
use tungstenite::extensions::ExtensionsConfig;
use tungstenite::extensions::compression::deflate::DeflateConfig;
use url::Url;

type Output = Arc<Mutex<BufWriter<tokio::io::Stdout>>>;
type ActiveRequests = Arc<Mutex<HashMap<String, ActiveRequest>>>;
type RequestError = Box<dyn std::error::Error + Send + Sync>;

trait AsyncIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

type NativeWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<Box<dyn AsyncIo>>>;

const CODEX_H2_INITIAL_STREAM_WINDOW_SIZE: u32 = 2 * 1024 * 1024;
const CODEX_H2_INITIAL_CONNECTION_WINDOW_SIZE: u32 = 5 * 1024 * 1024;
const CODEX_H2_MAX_FRAME_SIZE: u32 = 16 * 1024;
const CODEX_H2_MAX_HEADER_LIST_SIZE: u32 = 16 * 1024;

enum ActiveRequest {
    Http(oneshot::Sender<()>),
    WebSocket(mpsc::Sender<WebSocketCommand>),
}

enum WebSocketCommand {
    Send {
        command_id: String,
        message: Message,
    },
    Close {
        command_id: String,
        code: u16,
        reason: String,
    },
    Cancel,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NativeCommand {
    Request(NativeRequest),
    WebsocketConnect(NativeWebSocketRequest),
    WebsocketSendText {
        request_id: String,
        command_id: String,
        text: String,
    },
    WebsocketSendBinary {
        request_id: String,
        command_id: String,
        data: String,
    },
    WebsocketClose {
        request_id: String,
        command_id: String,
        code: u16,
        reason: String,
    },
    Cancel {
        request_id: String,
    },
}

#[derive(Deserialize)]
struct NativeRequest {
    request_id: String,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
    timeout_ms: u64,
    connect_timeout_ms: Option<u64>,
    proxy_url: Option<String>,
}

#[derive(Deserialize)]
struct NativeWebSocketRequest {
    request_id: String,
    url: String,
    headers: Vec<(String, String)>,
    connect_timeout_ms: u64,
    max_message_bytes: usize,
    ping_interval_ms: Option<u64>,
    ping_timeout_ms: Option<u64>,
    proxy_url: Option<String>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NativeEvent {
    Head {
        request_id: String,
        status: u16,
        http_version: String,
        headers: Vec<(String, String)>,
    },
    Chunk {
        request_id: String,
        data: String,
    },
    End {
        request_id: String,
    },
    WebsocketOpen {
        request_id: String,
        status: u16,
        headers: Vec<(String, String)>,
    },
    WebsocketText {
        request_id: String,
        text: String,
    },
    WebsocketBinary {
        request_id: String,
        data: String,
    },
    WebsocketSent {
        request_id: String,
        command_id: String,
    },
    WebsocketClose {
        request_id: String,
        code: Option<u16>,
        reason: Option<String>,
    },
    WebsocketError {
        request_id: String,
        command_id: Option<String>,
        message: String,
        failure_phase: String,
        retryable_same_contract: bool,
        is_tls_verification_failure: bool,
        status: Option<u16>,
        headers: Vec<(String, String)>,
        body: Option<String>,
    },
    Cancelled {
        request_id: String,
    },
    Error {
        request_id: String,
        message: String,
        failure_phase: String,
        retryable_same_contract: bool,
        is_tls_verification_failure: bool,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ClientKey {
    proxy_url: Option<String>,
    connect_timeout_ms: Option<u64>,
}

#[derive(Default)]
struct ClientPool {
    clients: HashMap<ClientKey, reqwest::Client>,
}

impl ClientPool {
    fn get(&mut self, key: &ClientKey) -> Result<reqwest::Client, reqwest::Error> {
        if let Some(client) = self.clients.get(key) {
            return Ok(client.clone());
        }
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .http2_initial_stream_window_size(CODEX_H2_INITIAL_STREAM_WINDOW_SIZE)
            .http2_initial_connection_window_size(CODEX_H2_INITIAL_CONNECTION_WINDOW_SIZE)
            .http2_max_frame_size(CODEX_H2_MAX_FRAME_SIZE)
            .http2_max_header_list_size(CODEX_H2_MAX_HEADER_LIST_SIZE)
            .pool_idle_timeout(Duration::from_secs(120))
            .pool_max_idle_per_host(8);
        if let Some(connect_timeout_ms) = key.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(connect_timeout_ms));
        }
        if let Some(proxy_url) = key.proxy_url.as_deref() {
            builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
        }
        let client = builder.build()?;
        self.clients.insert(key.clone(), client.clone());
        Ok(client)
    }
}

#[tokio::main]
async fn main() {
    if run().await.is_err() {
        std::process::exit(1);
    }
}

async fn run() -> Result<(), RequestError> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| "failed to install aws-lc-rs crypto provider")?;

    let output = Arc::new(Mutex::new(BufWriter::new(tokio::io::stdout())));
    let active: ActiveRequests = Arc::new(Mutex::new(HashMap::new()));
    let mut clients = ClientPool::default();
    let mut tasks = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else {
                    break;
                };
                let command: NativeCommand = serde_json::from_str(&line)?;
                match command {
                    NativeCommand::Request(request) => {
                        if request.request_id.is_empty() || active.lock().await.contains_key(&request.request_id) {
                            emit_error(
                                &output,
                                &request.request_id,
                                "native helper rejected the request",
                                "setup",
                                false,
                                false,
                            ).await?;
                            continue;
                        }
                        if request.timeout_ms == 0 || request.connect_timeout_ms == Some(0) {
                            emit_error(
                                &output,
                                &request.request_id,
                                "native helper rejected the request",
                                "setup",
                                false,
                                false,
                            ).await?;
                            continue;
                        }
                        let key = ClientKey {
                            proxy_url: request.proxy_url.clone(),
                            connect_timeout_ms: request.connect_timeout_ms,
                        };
                        let client = match clients.get(&key) {
                            Ok(client) => client,
                            Err(error) => {
                                let (message, phase, retryable, tls_verification) = classify_error(&error);
                                emit_error(
                                    &output,
                                    &request.request_id,
                                    message,
                                    phase,
                                    retryable,
                                    tls_verification,
                                ).await?;
                                continue;
                            }
                        };
                        let request_id = request.request_id.clone();
                        let (cancel_tx, cancel_rx) = oneshot::channel();
                        active
                            .lock()
                            .await
                            .insert(request_id.clone(), ActiveRequest::Http(cancel_tx));
                        let task_output = output.clone();
                        let task_active = active.clone();
                        tasks.spawn(async move {
                            tokio::select! {
                                result = execute_request(request, client, &task_output) => {
                                    if let Err(error) = result {
                                        let (message, phase, retryable, tls_verification) =
                                            classify_error(error.as_ref());
                                        let _ = emit_error(
                                            &task_output,
                                            &request_id,
                                            message,
                                            phase,
                                            retryable,
                                            tls_verification,
                                        ).await;
                                    }
                                }
                                _ = cancel_rx => {
                                    let _ = emit(
                                        &task_output,
                                        &NativeEvent::Cancelled { request_id: request_id.clone() },
                                    ).await;
                                }
                            }
                            task_active.lock().await.remove(&request_id);
                        });
                    }
                    NativeCommand::WebsocketConnect(request) => {
                        if request.request_id.is_empty()
                            || request.connect_timeout_ms == 0
                            || request.max_message_bytes == 0
                            || request.ping_interval_ms == Some(0)
                            || request.ping_timeout_ms == Some(0)
                            || active.lock().await.contains_key(&request.request_id)
                        {
                            emit_websocket_setup_error(
                                &output,
                                &request.request_id,
                                None,
                                "native helper rejected the websocket request",
                            )
                            .await?;
                            continue;
                        }
                        let request_id = request.request_id.clone();
                        let (command_tx, command_rx) = mpsc::channel(32);
                        active.lock().await.insert(
                            request_id.clone(),
                            ActiveRequest::WebSocket(command_tx),
                        );
                        let task_output = output.clone();
                        let task_active = active.clone();
                        tasks.spawn(async move {
                            if let Err(error) =
                                execute_websocket(request, command_rx, &task_output).await
                            {
                                let _ = emit_websocket_error(
                                    &task_output,
                                    &request_id,
                                    None,
                                    &error,
                                )
                                .await;
                            }
                            task_active.lock().await.remove(&request_id);
                        });
                    }
                    NativeCommand::WebsocketSendText {
                        request_id,
                        command_id,
                        text,
                    } => {
                        dispatch_websocket_command(
                            &active,
                            &output,
                            request_id,
                            command_id.clone(),
                            WebSocketCommand::Send {
                                command_id,
                                message: Message::Text(text.into()),
                            },
                        )
                        .await?;
                    }
                    NativeCommand::WebsocketSendBinary {
                        request_id,
                        command_id,
                        data,
                    } => {
                        let decoded = match base64::engine::general_purpose::STANDARD.decode(data) {
                            Ok(decoded) => decoded,
                            Err(_) => {
                                emit_websocket_setup_error(
                                    &output,
                                    &request_id,
                                    Some(command_id),
                                    "native helper rejected websocket binary data",
                                )
                                .await?;
                                continue;
                            }
                        };
                        dispatch_websocket_command(
                            &active,
                            &output,
                            request_id,
                            command_id.clone(),
                            WebSocketCommand::Send {
                                command_id,
                                message: Message::Binary(decoded.into()),
                            },
                        )
                        .await?;
                    }
                    NativeCommand::WebsocketClose {
                        request_id,
                        command_id,
                        code,
                        reason,
                    } => {
                        dispatch_websocket_command(
                            &active,
                            &output,
                            request_id,
                            command_id.clone(),
                            WebSocketCommand::Close {
                                command_id,
                                code,
                                reason,
                            },
                        )
                        .await?;
                    }
                    NativeCommand::Cancel { request_id } => {
                        let cancellation = active.lock().await.remove(&request_id);
                        match cancellation {
                            Some(ActiveRequest::Http(cancellation)) => {
                                let _ = cancellation.send(());
                            }
                            Some(ActiveRequest::WebSocket(commands)) => {
                                let _ = commands.send(WebSocketCommand::Cancel).await;
                            }
                            None => {
                                emit(&output, &NativeEvent::Cancelled { request_id }).await?;
                            }
                        }
                    }
                }
            }
            Some(_result) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }

    let cancellations = {
        let mut active = active.lock().await;
        active
            .drain()
            .map(|(_, cancellation)| cancellation)
            .collect::<Vec<_>>()
    };
    for cancellation in cancellations {
        match cancellation {
            ActiveRequest::Http(cancellation) => {
                let _ = cancellation.send(());
            }
            ActiveRequest::WebSocket(commands) => {
                let _ = commands.send(WebSocketCommand::Cancel).await;
            }
        }
    }
    while tasks.join_next().await.is_some() {}
    output.lock().await.flush().await?;
    Ok(())
}

#[derive(Debug)]
enum NativeWebSocketFailure {
    WebSocket(WebSocketError),
    Timeout,
    LivenessTimeout,
    Output(std::io::Error),
}

impl std::fmt::Display for NativeWebSocketFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WebSocket(error) => write!(formatter, "{error}"),
            Self::Timeout => formatter.write_str("websocket connect timed out"),
            Self::LivenessTimeout => formatter.write_str("websocket pong timed out"),
            Self::Output(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for NativeWebSocketFailure {}

impl From<WebSocketError> for NativeWebSocketFailure {
    fn from(error: WebSocketError) -> Self {
        Self::WebSocket(error)
    }
}

impl From<std::io::Error> for NativeWebSocketFailure {
    fn from(error: std::io::Error) -> Self {
        Self::Output(error)
    }
}

async fn dispatch_websocket_command(
    active: &ActiveRequests,
    output: &Output,
    request_id: String,
    command_id: String,
    command: WebSocketCommand,
) -> Result<(), std::io::Error> {
    let sender = {
        let active = active.lock().await;
        match active.get(&request_id) {
            Some(ActiveRequest::WebSocket(sender)) => Some(sender.clone()),
            _ => None,
        }
    };
    let Some(sender) = sender else {
        return emit_websocket_setup_error(
            output,
            &request_id,
            Some(command_id),
            "native websocket is not active",
        )
        .await;
    };
    if sender.send(command).await.is_err() {
        emit_websocket_setup_error(
            output,
            &request_id,
            Some(command_id),
            "native websocket command channel closed",
        )
        .await?;
    }
    Ok(())
}

async fn execute_websocket(
    request: NativeWebSocketRequest,
    mut commands: mpsc::Receiver<WebSocketCommand>,
    output: &Output,
) -> Result<(), NativeWebSocketFailure> {
    let request_id = request.request_id.clone();
    let connect_timeout = Duration::from_millis(request.connect_timeout_ms);
    let connected = tokio::time::timeout(connect_timeout, connect_native_websocket(&request))
        .await
        .map_err(|_| NativeWebSocketFailure::Timeout)??;
    let (mut websocket, response) = connected;
    let response_headers = websocket_headers(response.headers());
    emit(
        output,
        &NativeEvent::WebsocketOpen {
            request_id: request_id.clone(),
            status: response.status().as_u16(),
            headers: response_headers,
        },
    )
    .await?;

    let ping_interval = request.ping_interval_ms.map(Duration::from_millis);
    let ping_timeout = request.ping_timeout_ms.map(Duration::from_millis);
    let dormant_timer = Duration::from_secs(100 * 365 * 24 * 60 * 60);
    let mut next_ping = Box::pin(tokio::time::sleep(ping_interval.unwrap_or(dormant_timer)));
    let mut pong_deadline = Box::pin(tokio::time::sleep(dormant_timer));
    let mut ping_sequence = 0_u64;
    let mut awaiting_pong = None;

    loop {
        tokio::select! {
            _ = &mut next_ping, if ping_interval.is_some() => {
                let interval = ping_interval.expect("guarded ping interval");
                next_ping.as_mut().reset(tokio::time::Instant::now() + interval);
                if awaiting_pong.is_none() {
                    ping_sequence = ping_sequence.wrapping_add(1);
                    let payload = Bytes::copy_from_slice(&ping_sequence.to_be_bytes());
                    websocket.send(Message::Ping(payload.clone())).await?;
                    if let Some(timeout) = ping_timeout {
                        awaiting_pong = Some(payload);
                        pong_deadline.as_mut().reset(tokio::time::Instant::now() + timeout);
                    }
                }
            }
            _ = &mut pong_deadline, if awaiting_pong.is_some() => {
                return Err(NativeWebSocketFailure::LivenessTimeout);
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return Ok(());
                };
                match command {
                    WebSocketCommand::Send { command_id, message } => {
                        match websocket.send(message).await {
                            Ok(()) => {
                                emit(
                                    output,
                                    &NativeEvent::WebsocketSent {
                                        request_id: request_id.clone(),
                                        command_id,
                                    },
                                ).await?;
                            }
                            Err(error) => {
                                emit_websocket_error(
                                    output,
                                    &request_id,
                                    Some(command_id),
                                    &NativeWebSocketFailure::WebSocket(error),
                                ).await?;
                                return Ok(());
                            }
                        }
                    }
                    WebSocketCommand::Close { command_id, code, reason } => {
                        let frame = CloseFrame {
                            code: CloseCode::from(code),
                            reason: reason.clone().into(),
                        };
                        match websocket.send(Message::Close(Some(frame))).await {
                            Ok(()) => {
                                emit(
                                    output,
                                    &NativeEvent::WebsocketSent {
                                        request_id: request_id.clone(),
                                        command_id,
                                    },
                                ).await?;
                                emit(
                                    output,
                                    &NativeEvent::WebsocketClose {
                                        request_id,
                                        code: Some(code),
                                        reason: (!reason.is_empty()).then_some(reason),
                                    },
                                ).await?;
                                return Ok(());
                            }
                            Err(error) => {
                                emit_websocket_error(
                                    output,
                                    &request_id,
                                    Some(command_id),
                                    &NativeWebSocketFailure::WebSocket(error),
                                ).await?;
                                return Ok(());
                            }
                        }
                    }
                    WebSocketCommand::Cancel => {
                        emit(
                            output,
                            &NativeEvent::Cancelled {
                                request_id,
                            },
                        ).await?;
                        return Ok(());
                    }
                }
            }
            incoming = websocket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        emit(
                            output,
                            &NativeEvent::WebsocketText {
                                request_id: request_id.clone(),
                                text: text.to_string(),
                            },
                        ).await?;
                    }
                    Some(Ok(Message::Binary(data))) => {
                        emit(
                            output,
                            &NativeEvent::WebsocketBinary {
                                request_id: request_id.clone(),
                                data: base64::engine::general_purpose::STANDARD.encode(data),
                            },
                        ).await?;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(error) = websocket.send(Message::Pong(payload)).await {
                            return Err(error.into());
                        }
                    }
                    Some(Ok(Message::Pong(payload))) => {
                        if awaiting_pong.as_ref().is_some_and(|expected| expected == &payload) {
                            awaiting_pong = None;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = frame
                            .map(|frame| {
                                (
                                    Some(u16::from(frame.code)),
                                    (!frame.reason.is_empty()).then(|| frame.reason.to_string()),
                                )
                            })
                            .unwrap_or((None, None));
                        emit(
                            output,
                            &NativeEvent::WebsocketClose {
                                request_id,
                                code,
                                reason,
                            },
                        ).await?;
                        return Ok(());
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => {
                        emit(
                            output,
                            &NativeEvent::WebsocketClose {
                                request_id,
                                code: None,
                                reason: None,
                            },
                        ).await?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn connect_native_websocket(
    native_request: &NativeWebSocketRequest,
) -> Result<(NativeWebSocket, WebSocketResponse), WebSocketError> {
    let mut request = native_request.url.as_str().into_client_request()?;
    for (name, value) in &native_request.headers {
        request.headers_mut().append(
            WebSocketHeaderName::from_bytes(name.as_bytes())?,
            WebSocketHeaderValue::from_str(value)?,
        );
    }

    let host = request
        .uri()
        .host()
        .ok_or(tokio_tungstenite::tungstenite::error::UrlError::NoHostName)?
        .to_owned();
    let port = request
        .uri()
        .port_u16()
        .or_else(|| match request.uri().scheme_str() {
            Some("ws") => Some(80),
            Some("wss") => Some(443),
            _ => None,
        })
        .ok_or(tokio_tungstenite::tungstenite::error::UrlError::UnsupportedUrlScheme)?;

    let tls_config = native_tls_config()?;
    let stream: Box<dyn AsyncIo> = match native_request.proxy_url.as_deref() {
        None => Box::new(TcpStream::connect(host_port(&host, port)).await?),
        Some(proxy_url) => {
            let proxy = ProxyEndpoint::parse(proxy_url)?;
            let stream = TcpStream::connect(proxy.config.authority()).await?;
            let stream: Box<dyn AsyncIo> = if proxy.tls {
                let server_name = rustls::pki_types::ServerName::try_from(
                    proxy.config.host.clone(),
                )
                .map_err(|_| tokio_tungstenite::tungstenite::error::TlsError::InvalidDnsName)?;
                Box::new(
                    TlsConnector::from(tls_config.clone())
                        .connect(server_name, stream)
                        .await
                        .map_err(WebSocketError::Io)?,
                )
            } else {
                Box::new(stream)
            };
            Box::new(connect_via_proxy(stream, &proxy.config, &host, port).await?)
        }
    };

    client_async_tls_with_config(
        request,
        stream,
        Some(native_websocket_config(native_request.max_message_bytes)),
        Some(Connector::Rustls(tls_config)),
    )
    .await
}

fn native_websocket_config(max_message_bytes: usize) -> WebSocketConfig {
    let mut extensions = ExtensionsConfig::default();
    extensions.permessage_deflate = Some(DeflateConfig::default());
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(max_message_bytes);
    config.extensions = extensions;
    config
}

fn native_tls_config() -> Result<Arc<ClientConfig>, WebSocketError> {
    let certificates = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certificates.certs);
    if roots.is_empty() {
        return Err(WebSocketError::Io(std::io::Error::other(
            "native certificate store is empty",
        )));
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

#[derive(Debug)]
struct ProxyEndpoint {
    config: ProxyConfig,
    tls: bool,
}

impl ProxyEndpoint {
    fn parse(value: &str) -> Result<Self, WebSocketError> {
        let mut url = Url::parse(value).map_err(|_| invalid_proxy_config())?;
        let tls = url.scheme() == "https";
        if tls {
            let port = url
                .port_or_known_default()
                .ok_or_else(invalid_proxy_config)?;
            url.set_scheme("http").map_err(|_| invalid_proxy_config())?;
            url.set_port(Some(port))
                .map_err(|_| invalid_proxy_config())?;
        }
        let config = ProxyConfig::parse(url.as_str()).map_err(|_| invalid_proxy_config())?;
        Ok(Self { config, tls })
    }
}

fn invalid_proxy_config() -> WebSocketError {
    WebSocketError::Url(
        tokio_tungstenite::tungstenite::error::UrlError::InvalidProxyConfig(
            "<redacted>".to_owned(),
        ),
    )
}

fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn websocket_headers(
    headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

async fn emit_websocket_setup_error(
    output: &Output,
    request_id: &str,
    command_id: Option<String>,
    message: &str,
) -> Result<(), std::io::Error> {
    emit(
        output,
        &NativeEvent::WebsocketError {
            request_id: request_id.to_owned(),
            command_id,
            message: message.to_owned(),
            failure_phase: "setup".to_owned(),
            retryable_same_contract: false,
            is_tls_verification_failure: false,
            status: None,
            headers: Vec::new(),
            body: None,
        },
    )
    .await
}

async fn emit_websocket_error(
    output: &Output,
    request_id: &str,
    command_id: Option<String>,
    failure: &NativeWebSocketFailure,
) -> Result<(), std::io::Error> {
    let (message, phase, retryable, tls_verification, status, headers, body) = match failure {
        NativeWebSocketFailure::Timeout => (
            "native websocket connection timed out",
            "connect",
            true,
            false,
            None,
            Vec::new(),
            None,
        ),
        NativeWebSocketFailure::LivenessTimeout => (
            "native websocket pong timed out",
            "liveness_timeout",
            false,
            false,
            None,
            Vec::new(),
            None,
        ),
        NativeWebSocketFailure::WebSocket(WebSocketError::Http(response)) => (
            "native websocket handshake failed",
            "connect",
            false,
            false,
            Some(response.status().as_u16()),
            websocket_headers(response.headers()),
            response
                .body()
                .as_ref()
                .map(|body| base64::engine::general_purpose::STANDARD.encode(body)),
        ),
        NativeWebSocketFailure::WebSocket(WebSocketError::Io(_)) => (
            "native websocket transport failed",
            "transport",
            false,
            websocket_tls_verification_failure(failure),
            None,
            Vec::new(),
            None,
        ),
        NativeWebSocketFailure::WebSocket(_) => (
            "native websocket protocol failed",
            "protocol",
            false,
            websocket_tls_verification_failure(failure),
            None,
            Vec::new(),
            None,
        ),
        NativeWebSocketFailure::Output(_) => (
            "native helper output failed",
            "helper_write",
            false,
            false,
            None,
            Vec::new(),
            None,
        ),
    };
    emit(
        output,
        &NativeEvent::WebsocketError {
            request_id: request_id.to_owned(),
            command_id,
            message: message.to_owned(),
            failure_phase: phase.to_owned(),
            retryable_same_contract: retryable,
            is_tls_verification_failure: tls_verification,
            status,
            headers,
            body,
        },
    )
    .await
}

async fn execute_request(
    request: NativeRequest,
    client: reqwest::Client,
    output: &Output,
) -> Result<(), RequestError> {
    let method = reqwest::Method::from_bytes(request.method.as_bytes())?;
    let mut headers = HeaderMap::new();
    for (name, value) in request.headers {
        headers.append(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    let mut builder = client
        .request(method, request.url)
        .headers(headers)
        .timeout(Duration::from_millis(request.timeout_ms));
    if let Some(encoded_body) = request.body {
        builder = builder.body(base64::engine::general_purpose::STANDARD.decode(encoded_body)?);
    }

    let mut response = builder.send().await?;
    let response_headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    emit(
        output,
        &NativeEvent::Head {
            request_id: request.request_id.clone(),
            status: response.status().as_u16(),
            http_version: format!("{:?}", response.version()),
            headers: response_headers,
        },
    )
    .await?;

    while let Some(chunk) = response.chunk().await? {
        emit(
            output,
            &NativeEvent::Chunk {
                request_id: request.request_id.clone(),
                data: base64::engine::general_purpose::STANDARD.encode(chunk),
            },
        )
        .await?;
    }
    emit(
        output,
        &NativeEvent::End {
            request_id: request.request_id,
        },
    )
    .await?;
    Ok(())
}

fn classify_error(
    error: &(dyn std::error::Error + 'static),
) -> (&'static str, &'static str, bool, bool) {
    let Some(request_error) = error.downcast_ref::<reqwest::Error>() else {
        return ("native helper rejected the request", "setup", false, false);
    };
    let tls_verification = error_chain_has_invalid_certificate(request_error);
    if request_error.is_connect() {
        return (
            "native upstream connection failed",
            "connect",
            !tls_verification,
            tls_verification,
        );
    }
    if request_error.is_timeout() {
        return (
            "native upstream request timed out",
            "timeout",
            false,
            tls_verification,
        );
    }
    if request_error.is_body() || request_error.is_decode() {
        return (
            "native upstream response body failed",
            "body_read",
            false,
            tls_verification,
        );
    }
    (
        "native upstream request failed",
        "request",
        false,
        tls_verification,
    )
}

fn websocket_tls_verification_failure(failure: &NativeWebSocketFailure) -> bool {
    match failure {
        NativeWebSocketFailure::WebSocket(WebSocketError::Tls(TlsError::Rustls(error))) => {
            matches!(error.as_ref(), rustls::Error::InvalidCertificate(_))
        }
        NativeWebSocketFailure::WebSocket(error) => error_chain_has_invalid_certificate(error),
        _ => false,
    }
}

fn error_chain_has_invalid_certificate(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<rustls::Error>()
            .is_some_and(|error| matches!(error, rustls::Error::InvalidCertificate(_)))
        {
            return true;
        }
        current = source.source();
    }
    false
}

async fn emit_error(
    output: &Output,
    request_id: &str,
    message: &str,
    failure_phase: &str,
    retryable_same_contract: bool,
    is_tls_verification_failure: bool,
) -> Result<(), std::io::Error> {
    emit(
        output,
        &NativeEvent::Error {
            request_id: request_id.to_owned(),
            message: message.to_owned(),
            failure_phase: failure_phase.to_owned(),
            retryable_same_contract,
            is_tls_verification_failure,
        },
    )
    .await
}

async fn emit(output: &Output, event: &NativeEvent) -> Result<(), std::io::Error> {
    let mut output = output.lock().await;
    output.write_all(&serde_json::to_vec(event)?).await?;
    output.write_all(b"\n").await?;
    output.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async_with_config;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    use super::{
        CODEX_H2_INITIAL_CONNECTION_WINDOW_SIZE, CODEX_H2_INITIAL_STREAM_WINDOW_SIZE,
        CODEX_H2_MAX_FRAME_SIZE, CODEX_H2_MAX_HEADER_LIST_SIZE, ClientKey, ClientPool,
        NativeWebSocketRequest, connect_native_websocket, error_chain_has_invalid_certificate,
        native_websocket_config,
    };

    static INSTALL_PROVIDER: Once = Once::new();

    fn install_provider() {
        INSTALL_PROVIDER.call_once(|| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("install test crypto provider");
        });
    }

    #[test]
    fn compatible_requests_share_one_client_pool_entry() {
        install_provider();
        let mut pool = ClientPool::default();
        let key = ClientKey {
            proxy_url: None,
            connect_timeout_ms: Some(10_000),
        };

        pool.get(&key).expect("first client");
        pool.get(&key).expect("reused client");

        assert_eq!(pool.clients.len(), 1);
    }

    #[test]
    fn connector_policy_partitions_client_pool_entries() {
        install_provider();
        let mut pool = ClientPool::default();
        let direct = ClientKey {
            proxy_url: None,
            connect_timeout_ms: Some(10_000),
        };
        let proxied = ClientKey {
            proxy_url: Some("http://127.0.0.1:18080".to_owned()),
            connect_timeout_ms: Some(10_000),
        };

        pool.get(&direct).expect("direct client");
        pool.get(&proxied).expect("proxied client");

        assert_eq!(pool.clients.len(), 2);
    }

    #[test]
    fn codex_http2_startup_profile_uses_measured_fixed_windows() {
        assert_eq!(CODEX_H2_INITIAL_STREAM_WINDOW_SIZE, 2_097_152);
        assert_eq!(CODEX_H2_INITIAL_CONNECTION_WINDOW_SIZE, 5_242_880);
        assert_eq!(CODEX_H2_MAX_FRAME_SIZE, 16_384);
        assert_eq!(CODEX_H2_MAX_HEADER_LIST_SIZE, 16_384);
    }

    #[test]
    fn tls_certificate_failure_is_typed_without_message_matching() {
        let error = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);

        assert!(error_chain_has_invalid_certificate(&error));
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)] // callback error type is fixed by tungstenite
    async fn codex_websocket_fork_negotiates_compression_and_relays_frames() {
        install_provider();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind websocket server");
        let address = listener.local_addr().expect("server address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept websocket client");
            let mut websocket = accept_hdr_async_with_config(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                 mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    assert!(
                        request
                            .headers()
                            .get("sec-websocket-extensions")
                            .and_then(|value| value.to_str().ok())
                            .is_some_and(|value| value.contains("permessage-deflate"))
                    );
                    assert_eq!(
                        request
                            .headers()
                            .get("sec-websocket-protocol")
                            .and_then(|value| value.to_str().ok()),
                        Some("openai")
                    );
                    response.headers_mut().insert(
                        "sec-websocket-protocol",
                        HeaderValue::from_static("openai"),
                    );
                    Ok(response)
                },
                Some(native_websocket_config(1024)),
            )
            .await
            .expect("accept websocket handshake");
            websocket
                .send(Message::Text("server-frame".into()))
                .await
                .expect("send server frame");
            let message = websocket
                .next()
                .await
                .expect("receive client frame")
                .expect("valid client frame");
            assert_eq!(message, Message::Binary(vec![0, 255].into()));
        });

        let request = NativeWebSocketRequest {
            request_id: "ws-test".to_owned(),
            url: format!("ws://{address}/v1/responses"),
            headers: vec![("sec-websocket-protocol".to_owned(), "openai".to_owned())],
            connect_timeout_ms: 2_000,
            max_message_bytes: 1024,
            ping_interval_ms: Some(20_000),
            ping_timeout_ms: Some(120_000),
            proxy_url: None,
        };
        let (mut websocket, response) = connect_native_websocket(&request)
            .await
            .expect("connect native websocket");

        assert_eq!(response.status().as_u16(), 101);
        assert_eq!(
            response
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|value| value.to_str().ok()),
            Some("openai")
        );
        assert!(
            response
                .headers()
                .get("sec-websocket-extensions")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("permessage-deflate"))
        );
        assert_eq!(
            websocket
                .next()
                .await
                .expect("receive server frame")
                .expect("valid server frame"),
            Message::Text("server-frame".into())
        );
        websocket
            .send(Message::Binary(vec![0, 255].into()))
            .await
            .expect("send client frame");
        server.await.expect("websocket server task");
    }
}
