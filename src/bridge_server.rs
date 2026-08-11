use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serenity::http::Http;
use serenity::model::id::ChannelId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{accept_hdr_async, WebSocketStream};

use crate::embed_builder::send_bridge_event;
use crate::protocol::{IncomingEvent, OutgoingChatMessage};

type WsWriter = SplitSink<WebSocketStream<TcpStream>, WsMessage>;

pub async fn run_bridge_server(
	listen_port: u16,
	auth_token: String,
	discord_http: Arc<Http>,
	discord_channel_id: u64,
	outgoing_receiver: UnboundedReceiver<OutgoingChatMessage>,
) {
	let bind_addr = format!("0.0.0.0:{listen_port}");

	let listener = match TcpListener::bind(&bind_addr).await {
		Ok(listener) => listener,
		Err(err) => {
			eprintln!("Gagal bind WebSocket server ke {bind_addr}: {err}");
			return;
		}
	};

	println!("WebSocket bridge server mendengarkan di {bind_addr}.");

	let active_writer: Arc<Mutex<Option<WsWriter>>> = Arc::new(Mutex::new(None));

	let forward_writer = active_writer.clone();
	tokio::spawn(forward_outgoing_messages(forward_writer, outgoing_receiver));

	loop {
		match listener.accept().await {
			Ok((stream, peer_addr)) => {
				let auth_token = auth_token.clone();
				let discord_http = discord_http.clone();
				let active_writer = active_writer.clone();

				tokio::spawn(handle_connection(
					stream,
					peer_addr,
					auth_token,
					discord_http,
					discord_channel_id,
					active_writer,
				));
			}
			Err(err) => {
				eprintln!("Gagal menerima koneksi TCP: {err}");
			}
		}
	}
}

async fn forward_outgoing_messages(
	active_writer: Arc<Mutex<Option<WsWriter>>>,
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

		let mut guard = active_writer.lock().await;

		if let Some(writer) = guard.as_mut() {
			if let Err(err) = writer.send(WsMessage::text(json)).await {
				eprintln!("Gagal mengirim pesan ke mod (mod mungkin belum terhubung): {err}");
				*guard = None;
			}
		}
	}
}

async fn handle_connection(
	stream: TcpStream,
	peer_addr: SocketAddr,
	auth_token: String,
	discord_http: Arc<Http>,
	discord_channel_id: u64,
	active_writer: Arc<Mutex<Option<WsWriter>>>,
) {
	let auth_check = move |req: &Request, mut res: Response| {
		let supplied = req
			.headers()
			.get("X-Auth-Token")
			.and_then(|value| value.to_str().ok())
			.unwrap_or("");

		if supplied != auth_token {
			*res.status_mut() = StatusCode::UNAUTHORIZED;
		}

		Ok(res)
	};

	let ws_stream = match accept_hdr_async(stream, auth_check).await {
		Ok(ws_stream) => ws_stream,
		Err(err) => {
			eprintln!("Handshake WebSocket gagal dari {peer_addr}: {err}");
			return;
		}
	};

	println!("Mod Fabric terhubung dari {peer_addr}.");

	let (writer, mut reader) = ws_stream.split();

	{
		let mut guard = active_writer.lock().await;
		*guard = Some(writer);
	}

	while let Some(message) = reader.next().await {
		match message {
			Ok(WsMessage::Text(text)) => {
				handle_incoming_from_mod(text.as_str(), &discord_http, discord_channel_id).await;
			}
			Ok(WsMessage::Close(_)) => {
				break;
			}
			Err(err) => {
				eprintln!("Kesalahan koneksi dari mod: {err}");
				break;
			}
			_ => {}
		}
	}

	println!("Mod Fabric terputus dari {peer_addr}.");

	let mut guard = active_writer.lock().await;
	*guard = None;
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
