use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serenity::http::Http;
use serenity::model::id::ChannelId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use crate::embed_builder::send_bridge_event;
use crate::protocol::{IncomingEvent, OutgoingChatMessage};

type WsWriter = SplitSink<WebSocket, WsMessage>;
type ActiveWriter = Arc<Mutex<Option<ActiveConnection>>>;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);
const MAX_DISCORD_MESSAGE_LENGTH: usize = 2_000;

struct ActiveConnection {
    id: u64,
    writer: WsWriter,
}

#[derive(Clone)]
struct AppState {
    bridge_auth_token: Arc<str>,
    panel_access_token: Arc<str>,
    discord_http: Arc<Http>,
    discord_channel_id: u64,
    active_writer: ActiveWriter,
}

#[derive(Deserialize)]
struct OperatorChatRequest {
    message: String,
}

#[derive(Serialize)]
struct ApiMessage {
    message: String,
}

pub async fn run_server(
    listen_port: u16,
    auth_token: String,
    panel_access_token: String,
    discord_http: Arc<Http>,
    discord_channel_id: u64,
    outgoing_receiver: UnboundedReceiver<OutgoingChatMessage>,
) {
    let bind_addr = format!("0.0.0.0:{listen_port}");

    let listener = match TcpListener::bind(&bind_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("Gagal bind server bot ke {bind_addr}: {err}");
            return;
        }
    };

    let state = AppState {
        bridge_auth_token: Arc::from(auth_token),
        panel_access_token: Arc::from(panel_access_token),
        discord_http,
        discord_channel_id,
        active_writer: Arc::new(Mutex::new(None)),
    };

    let forward_writer = state.active_writer.clone();
    tokio::spawn(forward_outgoing_messages(forward_writer, outgoing_receiver));

    let app = Router::new()
        // Path root dipertahankan khusus untuk mod Fabric yang sudah memakai wss://domain.
        .route("/", get(bridge_upgrade))
        .route("/panel", get(panel_page))
        .route("/api/chat", post(send_operator_message))
        .with_state(state);

    println!("Bridge WebSocket dan panel operator mendengarkan di {bind_addr}.");
    if let Err(err) = axum::serve(listener, app).await {
        eprintln!("Server HTTP/WebSocket berhenti: {err}");
    }
}

async fn forward_outgoing_messages(
    active_writer: ActiveWriter,
    mut outgoing_receiver: UnboundedReceiver<OutgoingChatMessage>,
) {
    while let Some(chat_message) = outgoing_receiver.recv().await {
        let json = match serde_json::to_string(&chat_message) {
            Ok(json) => json,
            Err(err) => {
                eprintln!("Gagal serialize pesan Discord: {err}");
                continue;
            }
        };

        let (connection_id, send_result) = {
            let mut guard = active_writer.lock().await;
            let Some(connection) = guard.as_mut() else {
                continue;
            };

            let connection_id = connection.id;
            let send_result = connection.writer.send(WsMessage::text(json)).await;
            (connection_id, send_result)
        };

        if let Err(err) = send_result {
            eprintln!("Gagal mengirim pesan ke mod (koneksi akan dibersihkan): {err}");
            clear_connection_if_current(&active_writer, connection_id).await;
        }
    }
}

async fn bridge_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let supplied_token = headers
        .get("X-Auth-Token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    if supplied_token != state.bridge_auth_token.as_ref() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    // Satu mod hanya memerlukan satu socket. Jangan menerima koneksi pengganti
    // lalu menutup socket aktif, karena pola itu memicu loop reset/reconnect.
    if state.active_writer.lock().await.is_some() {
        println!("Koneksi bridge tambahan ditolak karena masih ada koneksi aktif.");
        return StatusCode::CONFLICT.into_response();
    }

    websocket
        .on_upgrade(move |socket| handle_connection(socket, state))
        .into_response()
}

async fn handle_connection(socket: WebSocket, state: AppState) {
    static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
    let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    println!("Mod Fabric terhubung (koneksi #{connection_id}).");

    let (writer, mut reader) = socket.split();
    let accepted = {
        let mut guard = state.active_writer.lock().await;
        if guard.is_some() {
            false
        } else {
            *guard = Some(ActiveConnection {
                id: connection_id,
                writer,
            });
            true
        }
    };

    if !accepted {
        println!("Koneksi #{connection_id} ditutup karena koneksi lain sudah aktif.");
        return;
    }

    let heartbeat_task = tokio::spawn(send_heartbeats(connection_id, state.active_writer.clone()));

    while let Some(message) = reader.next().await {
        match message {
            Ok(WsMessage::Text(text)) => {
                handle_incoming_from_mod(text.as_str(), &state.discord_http, state.discord_channel_id)
                    .await;
            }
            Ok(WsMessage::Close(_)) => {
                break;
            }
            Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => {
                // tungstenite menjawab ping otomatis saat stream terus dipoll.
            }
            Err(err) => {
                eprintln!("Kesalahan koneksi dari mod (koneksi #{connection_id}): {err}");
                break;
            }
            _ => {}
        }
    }

    heartbeat_task.abort();
    clear_connection_if_current(&state.active_writer, connection_id).await;
    println!("Mod Fabric terputus (koneksi #{connection_id}).");
}

async fn send_heartbeats(connection_id: u64, active_writer: ActiveWriter) {
    let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let send_result = {
            let mut guard = active_writer.lock().await;
            match guard.as_mut() {
                Some(connection) if connection.id == connection_id => {
                    connection
                        .writer
                        .send(WsMessage::Ping(Vec::new().into()))
                        .await
                }
                _ => return,
            }
        };

        if let Err(err) = send_result {
            eprintln!("Heartbeat WebSocket gagal pada koneksi #{connection_id}: {err}");
            clear_connection_if_current(&active_writer, connection_id).await;
            return;
        }
    }
}

async fn clear_connection_if_current(active_writer: &ActiveWriter, connection_id: u64) {
    let mut guard = active_writer.lock().await;
    if guard
        .as_ref()
        .is_some_and(|connection| connection.id == connection_id)
    {
        *guard = None;
    }
}

async fn panel_page() -> Html<&'static str> {
    Html(PANEL_HTML)
}

async fn send_operator_message(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<OperatorChatRequest>,
) -> Result<Json<ApiMessage>, (StatusCode, Json<ApiMessage>)> {
    let supplied_token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");

    if supplied_token != state.panel_access_token.as_ref() {
        return Err(api_error(StatusCode::UNAUTHORIZED, "Akses panel ditolak."));
    }

    let message = match validate_operator_message(&request.message) {
        Ok(message) => message,
        Err(error) => return Err(api_error(StatusCode::BAD_REQUEST, error)),
    };

    let channel_id = ChannelId::new(state.discord_channel_id);
    if let Err(err) = channel_id
        .send_message(&state.discord_http, serenity::builder::CreateMessage::new().content(message))
        .await
    {
        eprintln!("Gagal mengirim chat dari panel ke Discord: {err}");
        return Err(api_error(
            StatusCode::BAD_GATEWAY,
            "Bot tidak dapat mengirim pesan ke Discord.",
        ));
    }

    Ok(Json(ApiMessage {
        message: "Pesan terkirim ke Discord.".to_string(),
    }))
}

fn api_error(status: StatusCode, message: &str) -> (StatusCode, Json<ApiMessage>) {
    (
        status,
        Json(ApiMessage {
            message: message.to_string(),
        }),
    )
}

fn validate_operator_message(raw_message: &str) -> Result<&str, &'static str> {
    let message = raw_message.trim();
    if message.is_empty() {
        return Err("Pesan tidak boleh kosong.");
    }

    if message.chars().count() > MAX_DISCORD_MESSAGE_LENGTH {
        return Err("Pesan maksimal 2.000 karakter.");
    }

    Ok(message)
}

async fn handle_incoming_from_mod(text: &str, discord_http: &Arc<Http>, discord_channel_id: u64) {
    let event: IncomingEvent = match serde_json::from_str(text) {
        Ok(event) => event,
        Err(err) => {
            eprintln!("Gagal parse event dari mod: {err}");
            return;
        }
    };

    let channel_id = ChannelId::new(discord_channel_id);

    send_bridge_event(discord_http, channel_id, event).await;
}

const PANEL_HTML: &str = r#"<!doctype html>
<html lang="id">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <title>Val0x04 — Discord Console</title>
  <style>
    :root { color-scheme: dark; --bg:#080a10; --panel:#101522; --line:#263049; --muted:#a6b0c6; --text:#f3f6ff; --accent:#7c5cff; --good:#41d4a5; --danger:#ff7285; }
    * { box-sizing:border-box; } body { margin:0; min-height:100vh; display:grid; place-items:center; padding:24px; background:radial-gradient(circle at top right,#22214a 0%,transparent 38%),radial-gradient(circle at bottom left,#102d38 0%,transparent 36%),var(--bg); color:var(--text); font:15px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
    main { width:min(100%,760px); border:1px solid var(--line); border-radius:20px; overflow:hidden; background:rgba(16,21,34,.94); box-shadow:0 24px 80px rgba(0,0,0,.42); }
    header { display:flex; justify-content:space-between; gap:16px; align-items:center; padding:23px 26px; border-bottom:1px solid var(--line); }
    .brand { display:flex; align-items:center; gap:12px; } .mark { width:38px; height:38px; display:grid; place-items:center; border-radius:11px; color:#fff; background:linear-gradient(135deg,#9c87ff,#5a3bd9); font-weight:800; box-shadow:0 8px 24px rgba(124,92,255,.38); } h1 { margin:0; font-size:17px; letter-spacing:.01em; } .sub { color:var(--muted); font-size:12px; } .badge { border:1px solid rgba(65,212,165,.35); border-radius:99px; padding:5px 9px; color:var(--good); font-size:12px; }
    section { padding:26px; } .lock { display:grid; gap:12px; } label { font-weight:650; font-size:13px; } input,textarea { width:100%; border:1px solid var(--line); outline:0; border-radius:11px; background:#090d16; color:var(--text); font:inherit; } input { padding:12px 13px; } textarea { min-height:136px; padding:14px; resize:vertical; } input:focus,textarea:focus { border-color:var(--accent); box-shadow:0 0 0 3px rgba(124,92,255,.18); }
    button { border:0; border-radius:10px; padding:11px 15px; background:var(--accent); color:white; font:inherit; font-weight:750; cursor:pointer; } button:hover { filter:brightness(1.09); } button:disabled { opacity:.55; cursor:not-allowed; } .hint { margin:0; color:var(--muted); font-size:13px; } .error { min-height:20px; color:var(--danger); font-size:13px; } .compose { display:none; gap:16px; } .compose.open { display:grid; } .toolbar { display:flex; align-items:center; justify-content:space-between; gap:12px; color:var(--muted); font-size:13px; } .send { display:flex; justify-content:flex-end; gap:11px; align-items:center; } .status { min-height:20px; color:var(--good); font-size:13px; } kbd { border:1px solid var(--line); border-bottom-width:2px; border-radius:5px; padding:1px 5px; color:var(--text); font:12px ui-monospace,SFMono-Regular,Menlo,monospace; }
    @media (max-width:560px) { body { padding:12px; } header,section { padding:20px; } header { align-items:flex-start; flex-direction:column; } }
  </style>
</head>
<body>
  <main>
    <header>
      <div class="brand"><div class="mark">V</div><div><h1>Val0x04 Console</h1><div class="sub">Kirim pesan langsung ke channel Discord bridge</div></div></div>
      <div class="badge">Operator panel</div>
    </header>
    <section id="login" class="lock">
      <label for="token">Token akses panel</label>
      <input id="token" type="password" autocomplete="current-password" placeholder="Masukkan PANEL_ACCESS_TOKEN">
      <p class="hint">Token tidak disimpan di browser dan hanya dipakai untuk request pada sesi halaman ini.</p>
      <div><button id="unlock" type="button">Buka panel</button></div>
      <div id="login-error" class="error" role="alert"></div>
    </section>
    <section id="composer" class="compose">
      <div class="toolbar"><span>Tujuan: channel Discord yang dikonfigurasi di bot</span><span id="counter">0 / 2000</span></div>
      <label for="message">Pesan sebagai bot</label>
      <textarea id="message" maxlength="2000" placeholder="Tulis pesan untuk dikirim ke Discord…"></textarea>
      <div class="send"><span class="hint"><kbd>Ctrl</kbd> + <kbd>Enter</kbd> untuk kirim</span><button id="send" type="button">Kirim ke Discord</button></div>
      <div id="status" class="status" role="status"></div>
    </section>
  </main>
  <script>
    let token = "";
    const $ = (id) => document.getElementById(id);
    const input = $("message");
    const updateCounter = () => $("counter").textContent = `${input.value.length} / 2000`;
    $("unlock").addEventListener("click", () => {
      const candidate = $("token").value.trim();
      if (!candidate) { $("login-error").textContent = "Masukkan token akses terlebih dahulu."; return; }
      token = candidate; $("token").value = ""; $("login").style.display = "none"; $("composer").classList.add("open"); input.focus();
    });
    input.addEventListener("input", updateCounter);
    async function sendMessage() {
      const message = input.value.trim(); const button = $("send"); const status = $("status");
      if (!message) { status.style.color = "var(--danger)"; status.textContent = "Pesan tidak boleh kosong."; return; }
      button.disabled = true; status.style.color = "var(--muted)"; status.textContent = "Mengirim…";
      try {
        const response = await fetch("/api/chat", { method:"POST", headers:{ "Content-Type":"application/json", "Authorization":`Bearer ${token}` }, body:JSON.stringify({message}) });
        const data = await response.json();
        if (!response.ok) throw new Error(data.message || "Pesan gagal dikirim.");
        input.value = ""; updateCounter(); status.style.color = "var(--good)"; status.textContent = data.message;
      } catch (error) { status.style.color = "var(--danger)"; status.textContent = error.message || "Koneksi ke panel gagal."; }
      finally { button.disabled = false; input.focus(); }
    }
    $("send").addEventListener("click", sendMessage);
    input.addEventListener("keydown", (event) => { if ((event.ctrlKey || event.metaKey) && event.key === "Enter") { event.preventDefault(); sendMessage(); } });
  </script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panel_rejects_empty_messages() {
        assert_eq!(validate_operator_message("  \n "), Err("Pesan tidak boleh kosong."));
    }

    #[test]
    fn panel_trims_and_accepts_valid_messages() {
        assert_eq!(validate_operator_message("  Halo Discord  "), Ok("Halo Discord"));
    }

    #[test]
    fn panel_rejects_messages_over_discord_limit() {
        let too_long = "a".repeat(MAX_DISCORD_MESSAGE_LENGTH + 1);
        assert_eq!(
            validate_operator_message(&too_long),
            Err("Pesan maksimal 2.000 karakter.")
        );
    }
}
