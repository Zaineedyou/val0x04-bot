use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::OutgoingChatMessage;

pub struct DiscordHandler {
	pub channel_id: u64,
	pub outgoing_sender: UnboundedSender<OutgoingChatMessage>,
}

#[async_trait]
impl EventHandler for DiscordHandler {
	async fn ready(&self, _ctx: Context, ready: Ready) {
		println!("Bot Discord login sebagai {}", ready.user.name);
	}

	async fn message(&self, ctx: Context, msg: Message) {
		if msg.author.bot {
			return;
		}

		if msg.channel_id.get() != self.channel_id {
			return;
		}

		let role_name = resolve_highest_role_name(&ctx, &msg);

		let outgoing = OutgoingChatMessage::new(
			msg.author.name.clone(),
			role_name.unwrap_or_default(),
			msg.content.clone(),
		);

		if let Err(err) = self.outgoing_sender.send(outgoing) {
			eprintln!("Gagal meneruskan pesan Discord ke mod: {err}");
		}
	}
}

fn resolve_highest_role_name(ctx: &Context, msg: &Message) -> Option<String> {
	let guild_id = msg.guild_id?;
	let member = msg.member.as_ref()?;

	let guild = ctx.cache.guild(guild_id)?;

	let mut highest_position: i64 = -1;
	let mut highest_name: Option<String> = None;

	for role_id in &member.roles {
		if let Some(role) = guild.roles.get(role_id) {
			let position = role.position as i64;

			if position > highest_position {
				highest_position = position;
				highest_name = Some(role.name.clone());
			}
		}
	}

	highest_name
}
