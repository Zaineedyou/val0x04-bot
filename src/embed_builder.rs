use serenity::builder::{CreateEmbed, CreateMessage};
use serenity::http::Http;
use serenity::model::id::ChannelId;
use std::sync::Arc;

use crate::protocol::IncomingEvent;

pub async fn send_bridge_event(http: &Arc<Http>, channel_id: ChannelId, event: IncomingEvent) {
	let result = match event.event_type.as_str() {
		"chat" => send_plain_chat(http, channel_id, &event).await,
		"join" => send_embed(http, channel_id, &event, "Player Joined", format_join_leave(&event)).await,
		"leave" => send_embed(http, channel_id, &event, "Player Left", format_join_leave(&event)).await,
		"death" => send_embed(http, channel_id, &event, "Death", format_death(&event)).await,
		"advancement" => {
			send_embed(http, channel_id, &event, "Advancement", format_advancement(&event)).await
		}
		"bridge_status" => {
			send_embed(http, channel_id, &event, "Bridge Status", format_bridge_status(&event)).await
		}
		"server_start" => {
			send_embed(http, channel_id, &event, "Server Status", "Server telah menyala.".to_string()).await
		}
		"server_stop" => {
			send_embed(http, channel_id, &event, "Server Status", "Server sedang dimatikan.".to_string()).await
		}
		other => {
			eprintln!("Tipe event tidak dikenal dari mod: {other}");
			Ok(())
		}
	};

	if let Err(err) = result {
		eprintln!("Gagal mengirim pesan ke Discord: {err}");
	}
}

async fn send_plain_chat(
	http: &Arc<Http>,
	channel_id: ChannelId,
	event: &IncomingEvent,
) -> Result<(), serenity::Error> {
	let player = event.player.clone().unwrap_or_else(|| "Unknown".to_string());
	let message = event.message.clone().unwrap_or_default();

	let content = format!("**{player}**: {message}");
	let builder = CreateMessage::new().content(content);

	channel_id.send_message(http, builder).await?;
	Ok(())
}

async fn send_embed(
	http: &Arc<Http>,
	channel_id: ChannelId,
	event: &IncomingEvent,
	title: &str,
	description: String,
) -> Result<(), serenity::Error> {
	let emoji = event.emoji.clone().unwrap_or_default();
	let full_title = if emoji.is_empty() {
		title.to_string()
	} else {
		format!("{emoji} {title}")
	};

	let embed = CreateEmbed::new().title(full_title).description(description);
	let builder = CreateMessage::new().embed(embed);

	channel_id.send_message(http, builder).await?;
	Ok(())
}

fn format_join_leave(event: &IncomingEvent) -> String {
	let player = event.player.clone().unwrap_or_else(|| "Unknown".to_string());
	format!("**{player}**")
}

fn format_death(event: &IncomingEvent) -> String {
	event.message.clone().unwrap_or_else(|| "Seorang pemain telah meninggal.".to_string())
}

fn format_advancement(event: &IncomingEvent) -> String {
	event.message.clone().unwrap_or_else(|| "Sebuah advancement telah dicapai.".to_string())
}

fn format_bridge_status(event: &IncomingEvent) -> String {
	match event.status.as_deref() {
		Some("connect") => "Bridge tersambung ke server Minecraft.".to_string(),
		Some("disconnect") => "Bridge terputus dari server Minecraft.".to_string(),
		_ => "Status bridge berubah.".to_string(),
	}
}
