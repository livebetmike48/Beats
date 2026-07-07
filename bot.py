import os
import json
import logging
import asyncio
from datetime import datetime, timezone

import discord
from discord.ext import tasks
import websockets
from dotenv import load_dotenv

import keywords
import storage

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
TWITTERAPI_KEY = os.getenv("TWITTERAPI_KEY")
LIST_ID = os.getenv("LIST_ID")  # the X List ID containing all monitored beat reporters

WS_URL = "wss://ws.twitterapi.io/twitter/tweet/stream"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("twitter_bot")

intents = discord.Intents.default()


def build_tweet_embed(tweet: dict, matches: list[dict]) -> discord.Embed:
    labels = " ".join(f"{m['emoji']} {m['label']}" for m in matches)
    author = tweet.get("author", {})
    embed = discord.Embed(
        title=labels or "New Tweet",
        description=tweet.get("text", ""),
        url=tweet.get("url"),
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(
        name=f"@{author.get('userName', 'unknown')}",
        icon_url=author.get("profilePicture"),
    )
    embed.set_footer(text="Beat Reporter Monitor")
    return embed


class TwitterMonitorBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self):
        storage.init_db()

        setchannel_cmd = discord.app_commands.Command(
            name="setchannel",
            description="Set this channel to receive beat reporter alerts",
            callback=self._setchannel_callback,
        )
        self.tree.add_command(setchannel_cmd)

        testfeed_cmd = discord.app_commands.Command(
            name="testfeed",
            description="Debug: test the TwitterAPI.io connection and show raw results",
            callback=self._testfeed_callback,
        )
        self.tree.add_command(testfeed_cmd)

        try:
            synced = await self.tree.sync()
            log.info("Synced %d slash commands", len(synced))
        except Exception as e:
            log.error("Slash command sync failed: %s", e)

    async def _setchannel_callback(self, interaction: discord.Interaction):
        storage.set_config("announce_channel_id", str(interaction.channel_id))
        await interaction.response.send_message(
            f"✅ Beat reporter alerts will post in {interaction.channel.mention}."
        )

    async def _testfeed_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        import requests

        # Trying the most likely endpoint name based on TwitterAPI.io's
        # naming pattern -- not yet verified against a live response. If
        # this 404s, the raw response will tell us the real path to use.
        url = "https://api.twitterapi.io/twitter/list/tweets"
        headers = {"X-API-Key": TWITTERAPI_KEY}
        params = {"listId": LIST_ID}

        try:
            resp = await asyncio.to_thread(requests.get, url, headers=headers, params=params, timeout=15)
            status = resp.status_code
            body_preview = resp.text[:1200]
        except Exception as e:
            await interaction.followup.send(f"Request failed entirely: {e}")
            return

        await interaction.followup.send(
            f"**TwitterAPI.io diagnostic**\n\nURL: `{url}`\nStatus: `{status}`\n\n```{body_preview}```"[:2000]
        )

    async def on_ready(self):
        log.info("Logged in as %s", self.user)
        if not stream_listener.is_running():
            stream_listener.start(self)


client = TwitterMonitorBot()


@tasks.loop(seconds=1, count=1)  # runs once, then the loop inside manages its own reconnect
async def stream_listener(bot: TwitterMonitorBot):
    """
    Connects to TwitterAPI.io's real-time WebSocket stream for the
    configured X List, classifies each tweet, and posts matches to Discord.
    Reconnects with exponential backoff on any disconnect.
    """
    backoff = 1
    while True:
        try:
            headers = {"X-API-Key": TWITTERAPI_KEY}
            async with websockets.connect(WS_URL, extra_headers=headers) as ws:
                log.info("Connected to TwitterAPI.io stream")
                backoff = 1  # reset on successful connect

                # NOTE: exact subscribe payload format needs to be confirmed
                # against TwitterAPI.io's real docs once we have a live key --
                # this is a reasonable placeholder based on their documented
                # rule-based pattern, not yet verified against a live response.
                await ws.send(json.dumps({"action": "subscribe", "listId": LIST_ID}))

                async for raw_message in ws:
                    try:
                        data = json.loads(raw_message)
                    except Exception:
                        continue
                    await handle_incoming_tweet(bot, data)

        except Exception as e:
            log.error("Stream disconnected, reconnecting in %ss: %s", backoff, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def handle_incoming_tweet(bot: TwitterMonitorBot, data: dict):
    if data.get("event_type") != "tweet":
        return  # ignore ping/connected events

    tweet = data.get("tweet", {})
    text = tweet.get("text", "")
    tweet_id = tweet.get("id")

    if not text or not tweet_id:
        return
    if storage.already_posted(tweet_id):
        return

    matches = keywords.classify_tweet(text)
    if not matches:
        return  # not betting-relevant, skip

    channel_id = storage.get_config("announce_channel_id")
    if not channel_id:
        return
    channel = bot.get_channel(int(channel_id))
    if channel is None:
        return

    try:
        await channel.send(embed=build_tweet_embed(tweet, matches))
        storage.mark_posted(tweet_id)
        log.info("Posted tweet %s (categories: %s)", tweet_id, [m["key"] for m in matches])
    except Exception as e:
        log.error("Failed to post tweet %s: %s", tweet_id, e)


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Set DISCORD_TOKEN in your .env file.")
    if not TWITTERAPI_KEY:
        raise SystemExit("Set TWITTERAPI_KEY in your .env file.")
    if not LIST_ID:
        raise SystemExit("Set LIST_ID in your .env file (the X List containing your beat reporters).")
    client.run(DISCORD_TOKEN)
