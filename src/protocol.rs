use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct IncomingEvent {
	#[serde(rename = "type")]
	pub event_type: String,
	pub player: Option<String>,
	pub message: Option<String>,
	pub embed: Option<bool>,
	pub emoji: Option<String>,
	pub status: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OutgoingChatMessage {
	#[serde(rename = "type")]
	pub event_type: String,
	pub author: String,
	pub role: String,
	pub message: String,
}

impl OutgoingChatMessage {
	pub fn new(author: String, role: String, message: String) -> Self {
		OutgoingChatMessage {
			event_type: "chat".to_string(),
			author,
			role,
			message,
		}
	}
}
