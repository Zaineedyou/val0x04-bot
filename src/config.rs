use std::env;

pub struct Config {
	pub discord_token: String,
	pub discord_channel_id: u64,
	pub listen_port: u16,
	pub websocket_auth_token: String,
}

impl Config {
	pub fn load_from_env() -> Result<Self, String> {
		let discord_token = env::var("DISCORD_TOKEN")
			.map_err(|_| "Environment variable DISCORD_TOKEN belum di-set".to_string())?;

		let discord_channel_id_raw = env::var("DISCORD_CHANNEL_ID")
			.map_err(|_| "Environment variable DISCORD_CHANNEL_ID belum di-set".to_string())?;

		let discord_channel_id = discord_channel_id_raw
			.trim()
			.parse::<u64>()
			.map_err(|_| "DISCORD_CHANNEL_ID harus berupa angka".to_string())?;

		let listen_port = env::var("PORT")
			.ok()
			.and_then(|value| value.trim().parse::<u16>().ok())
			.unwrap_or(8080);

		let websocket_auth_token = env::var("BRIDGE_WEBSOCKET_AUTH_TOKEN")
			.map_err(|_| "Environment variable BRIDGE_WEBSOCKET_AUTH_TOKEN belum di-set".to_string())?;

		Ok(Config {
			discord_token,
			discord_channel_id,
			listen_port,
			websocket_auth_token,
		})
	}
}
