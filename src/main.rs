mod bridge_server;
mod config;
mod discord_handler;
mod embed_builder;
mod protocol;

use config::Config;
use discord_handler::DiscordHandler;
use serenity::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
	let config = match Config::load_from_env() {
		Ok(config) => config,
		Err(err) => {
			eprintln!("Gagal memuat konfigurasi: {err}");
			std::process::exit(1);
		}
	};

	let intents = GatewayIntents::GUILDS
		| GatewayIntents::GUILD_MESSAGES
		| GatewayIntents::GUILD_MEMBERS
		| GatewayIntents::MESSAGE_CONTENT;

	let (outgoing_sender, outgoing_receiver) = mpsc::unbounded_channel();

	let handler = DiscordHandler {
		channel_id: config.discord_channel_id,
		outgoing_sender,
	};

	let mut client = Client::builder(&config.discord_token, intents)
		.event_handler(handler)
		.await
		.expect("Gagal membuat client Discord");

	let discord_http: Arc<serenity::http::Http> = client.http.clone();

	let server_task = tokio::spawn(bridge_server::run_server(
		config.listen_port,
		config.websocket_auth_token,
		config.panel_access_token,
		discord_http,
		config.discord_channel_id,
		outgoing_receiver,
	));

	let discord_task = tokio::spawn(async move {
		if let Err(err) = client.start().await {
			eprintln!("Client Discord error: {err}");
		}
	});

	let _ = tokio::join!(server_task, discord_task);
}
