import os
import logging
import asyncio
from datetime import datetime, timezone

import discord
from discord.ext import tasks
from dotenv import load_dotenv

import keywords
import storage

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
LIST_ID = os.getenv("LIST_ID")  # the X List ID containing all monitored beat reporters

X_API_BASE = "https://api.x.com/2"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("twitter_bot")

intents = discord.Intents.default()


def _fetch_list_tweets(list_id: str, max_results: int = 20) -> dict:
    """
    Hits the official X API v2 List Tweets endpoint. Returns the raw
    parsed JSON response (with 'data' = tweets, 'includes' = expanded
    user objects). Raises via requests if the HTTP call itself fails;
    callers should still check for an 'errors' key in the response body,
    since X API v2 can return HTTP 200 with partial errors.
    """
    import requests

    url = f"{X_API_BASE}/lists/{list_id}/tweets"
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    params = {
        "max_results": max_results,
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "name,username,profile_image_url",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _index_users(payload: dict) -> dict:
    """Maps author_id -> user object from the 'includes.users' expansion."""
    users = (payload.get("includes") or {}).get("users") or []
    return {u["id"]: u for u in users}


def build_tweet_embed(tweet: dict, users_by_id: dict, matches: list[dict] = None) -> discord.Embed:
    labels = " ".join(f"{m['emoji']} {m['label']}" for m in matches) if matches else None
    author = users_by_id.get(tweet.get("author_id"), {})
    username = author.get("username", "unknown")
    tweet_id = tweet.get("id")
    tweet_url = f"https://x.com/{username}/status/{tweet_id}" if tweet_id else None

    tweet_dt = None
    created_at = tweet.get("created_at")  # ISO 8601, e.g. 2026-07-18T20:13:00.000Z
    if created_at:
        try:
            tweet_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            tweet_dt = datetime.now(timezone.utc)

    embed = discord.Embed(
        description=tweet.get("text", ""),
        url=tweet_url,
        color=discord.Color.blue(),
        timestamp=tweet_dt or datetime.now(timezone.utc),
    )
    embed.set_author(
        name=f"{author.get('name', 'Unknown')} (@{username})",
        icon_url=author.get("profile_image_url"),
        url=tweet_url,
    )
    if labels:
        embed.title = labels
    return embed


def _group_channel_key(group_key: str) -> str:
    return f"channel:{group_key}"


async def _resolve_channel(bot: discord.Client, config_key: str):
    """Returns the Discord channel for a stored config key, or None."""
    channel_id = storage.get_config(config_key)
    if not channel_id:
        return None
    return bot.get_channel(int(channel_id))


class TwitterMonitorBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self):
        storage.init_db()

        setchannel_cmd = discord.app_commands.Command(
            name="setchannel",
            description="Set this channel to receive general beat reporter alerts",
            callback=self._setchannel_callback,
        )
        self.tree.add_command(setchannel_cmd)

        # Group choices generated straight from keywords.GROUPS so adding a
        # new group in keywords.py automatically shows up here.
        group_choices = [
            discord.app_commands.Choice(name=grp["label"], value=key)
            for key, grp in keywords.GROUPS.items()
        ]

        @discord.app_commands.choices(group=group_choices)
        async def _setgroupchannel_callback(
            interaction: discord.Interaction,
            group: discord.app_commands.Choice[str],
        ):
            storage.set_config(_group_channel_key(group.value), str(interaction.channel_id))
            grp = keywords.GROUPS[group.value]
            await interaction.response.send_message(
                f"✅ {grp['emoji']} **{grp['label']}** alerts will post in {interaction.channel.mention}."
            )

        setgroupchannel_cmd = discord.app_commands.Command(
            name="setgroupchannel",
            description="Set this channel to receive alerts for a specific keyword group",
            callback=_setgroupchannel_callback,
        )
        self.tree.add_command(setgroupchannel_cmd)

        recenttweets_cmd = discord.app_commands.Command(
            name="recenttweets",
            description="Show the most recent tweets from your monitored list",
            callback=self._recenttweets_callback,
        )
        self.tree.add_command(recenttweets_cmd)

        search_cmd = discord.app_commands.Command(
            name="search",
            description="Search recent tweets from your list for a specific word or phrase",
            callback=self._search_callback,
        )
        self.tree.add_command(search_cmd)

        try:
            synced = await self.tree.sync()
            log.info("Synced %d slash commands", len(synced))
        except Exception as e:
            log.error("Slash command sync failed: %s", e)

    async def _setchannel_callback(self, interaction: discord.Interaction):
        storage.set_config("announce_channel_id", str(interaction.channel_id))
        await interaction.response.send_message(
            f"✅ General beat reporter alerts will post in {interaction.channel.mention}."
        )

    async def _recenttweets_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            payload = await asyncio.to_thread(_fetch_list_tweets, LIST_ID, 20)
        except Exception as e:
            await interaction.followup.send(f"Request failed: {e}")
            return

        if "errors" in payload and not payload.get("data"):
            await interaction.followup.send(f"X API returned an error:\n```{payload['errors']}```")
            return

        tweets = payload.get("data", [])
        if not tweets:
            await interaction.followup.send("No tweets in response from the list.")
            return

        users_by_id = _index_users(payload)
        await interaction.followup.send(f"Showing the {min(3, len(tweets))} most recent tweets from your list:")
        for tweet in tweets[:3]:
            await interaction.channel.send(embed=build_tweet_embed(tweet, users_by_id))

    async def _search_callback(self, interaction: discord.Interaction, term: str):
        await interaction.response.defer()
        try:
            payload = await asyncio.to_thread(_fetch_list_tweets, LIST_ID, 100)
        except Exception as e:
            await interaction.followup.send(f"Request failed: {e}")
            return

        if "errors" in payload and not payload.get("data"):
            await interaction.followup.send(f"X API returned an error:\n```{payload['errors']}```")
            return

        tweets = payload.get("data", [])
        users_by_id = _index_users(payload)
        term_lower = term.lower()
        matches = [t for t in tweets if term_lower in t.get("text", "").lower()]

        if not matches:
            await interaction.followup.send(f"No recent tweets in your list mention '{term}'.")
            return

        await interaction.followup.send(f"Found {len(matches)} recent tweet(s) mentioning '{term}':")
        for tweet in matches[:5]:
            await interaction.channel.send(embed=build_tweet_embed(tweet, users_by_id))

    async def on_ready(self):
        log.info("Logged in as %s", self.user)
        if not poll_list_tweets.is_running():
            poll_list_tweets.start(self)
        if not watchdog.is_running():
            watchdog.start()


client = TwitterMonitorBot()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "90"))


@tasks.loop(seconds=POLL_SECONDS)
async def poll_list_tweets(bot: TwitterMonitorBot):
    try:
        await _poll_list_tweets_body(bot)
    except Exception as e:
        # Top-level safety net -- an unhandled exception here would
        # otherwise permanently stop this loop with no automatic recovery.
        log.error("poll_list_tweets cycle failed unexpectedly, will retry next cycle: %s", e)


async def _poll_list_tweets_body(bot: TwitterMonitorBot):
    try:
        payload = await asyncio.to_thread(_fetch_list_tweets, LIST_ID, 20)
    except Exception as e:
        log.error("Failed to fetch list tweets: %s", e)
        return

    if "errors" in payload and not payload.get("data"):
        log.error("X API returned errors: %s", payload["errors"])
        return

    tweets = payload.get("data", [])
    users_by_id = _index_users(payload)

    # Process oldest-first so if multiple new tweets arrived since last
    # check, they post to Discord in chronological order.
    for tweet in reversed(tweets):
        text = tweet.get("text", "")
        tweet_id = tweet.get("id")
        if not text or not tweet_id:
            continue
        if storage.already_posted(tweet_id):
            continue

        author = users_by_id.get(tweet.get("author_id"), {})
        username = author.get("username", "")

        storage.mark_posted(tweet_id)  # mark seen regardless of match, so we never re-check it

        # --- Routing ---
        # 1) Account override: every post from this account goes to its
        #    mapped group's channel, no keyword match required.
        override_group = keywords.account_override_group(username)
        if override_group:
            grp = keywords.GROUPS[override_group]
            channel = await _resolve_channel(bot, _group_channel_key(override_group))
            if channel is None:
                log.warning("No channel set for group '%s' (needed for @%s override) -- run /setgroupchannel", override_group, username)
                continue
            try:
                await channel.send(embed=build_tweet_embed(
                    tweet, users_by_id,
                    [{"key": override_group, "emoji": grp["emoji"], "label": grp["label"]}],
                ))
                log.info("Posted tweet %s to group '%s' (account override @%s)", tweet_id, override_group, username)
            except Exception as e:
                log.error("Failed to post tweet %s: %s", tweet_id, e)
            continue

        # 2) Group keyword match: route to each matched group's channel.
        group_matches = keywords.classify_groups(text)
        if group_matches:
            for gm in group_matches:
                channel = await _resolve_channel(bot, _group_channel_key(gm["key"]))
                if channel is None:
                    log.warning("No channel set for group '%s' -- run /setgroupchannel in the target channel", gm["key"])
                    continue
                try:
                    await channel.send(embed=build_tweet_embed(tweet, users_by_id, [gm]))
                    log.info("Posted tweet %s to group '%s'", tweet_id, gm["key"])
                except Exception as e:
                    log.error("Failed to post tweet %s to group '%s': %s", tweet_id, gm["key"], e)
            continue

        # 3) General categories fallback: post to the general channel.
        matches = keywords.classify_tweet(text)
        if not matches:
            continue  # not betting-relevant, skip silently

        channel = await _resolve_channel(bot, "announce_channel_id")
        if channel is None:
            continue
        try:
            await channel.send(embed=build_tweet_embed(tweet, users_by_id, matches))
            log.info("Posted tweet %s to general (categories: %s)", tweet_id, [m["key"] for m in matches])
        except Exception as e:
            log.error("Failed to post tweet %s: %s", tweet_id, e)


@poll_list_tweets.before_loop
async def before_poll():
    await client.wait_until_ready()


@tasks.loop(minutes=2)
async def watchdog():
    """If the poll loop somehow stops for any reason not already caught
    above, this notices within 2 minutes and restarts it."""
    if not poll_list_tweets.is_running():
        log.error("poll_list_tweets was found stopped -- restarting it now")
        poll_list_tweets.start(client)


@watchdog.before_loop
async def before_watchdog():
    await client.wait_until_ready()


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Set DISCORD_TOKEN in your .env file.")
    if not X_BEARER_TOKEN:
        raise SystemExit("Set X_BEARER_TOKEN in your .env file (from developer.x.com, App > Keys and tokens).")
    if not LIST_ID:
        raise SystemExit("Set LIST_ID in your .env file (the X List containing your beat reporters).")
    client.run(DISCORD_TOKEN)
