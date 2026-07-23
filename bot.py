"""
Beat Reporter bot v3 -- FILTERED STREAM edition.

Cost model change: instead of polling the list every 90s and paying for
every tweet READ (~all 178 writers' full output, ~$7/day), this registers
rules with X combining your writer list + your keyword groups, and X
PUSHES only matching tweets. Billing follows delivery, so the ~85% of
tweets that never matched a keyword are never billed at all.

Everything else is unchanged: same routing (account override -> keyword
groups -> general categories), same channels/commands, same embeds, same
dedupe storage. /recenttweets and /search still work (they use the list
endpoint on demand).

New pieces:
- Rule sync on startup: fetches the list's members, builds stream rules
  (author chunks x keyword chunks, each rule <= 1024 chars), replaces
  the app's rules. /syncrules (admin) re-syncs after you edit the list
  in the X app. /streamstatus shows connection health + rule count.
- Stream consumer: persistent connection in a background thread with
  exponential-backoff reconnects and stall detection (X sends keep-alive
  newlines ~every 20s; silence beyond the read timeout forces reconnect).
- SAFETY NET: if the stream endpoint returns 403 (i.e. filtered stream
  turns out to be unavailable on this plan -- docs say it's available on
  pay-per-use, but the definitive source is this account's own access),
  the bot LOGS IT LOUDLY and automatically falls back to the old 90s
  polling loop, so worst case is today's behavior/cost, never silence.
"""
import os
import json
import time
import logging
import asyncio
import threading
from datetime import datetime, timezone

import requests
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
STREAM_URL = f"{X_API_BASE}/tweets/search/stream"
RULES_URL = f"{X_API_BASE}/tweets/search/stream/rules"

MAX_RULE_LEN = 1024        # documented per-rule character cap
KW_CHUNK_BUDGET = 380      # chars of keyword clause per rule
AUTHOR_CHUNK_BUDGET = 560  # chars of author clause per rule

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("twitter_bot")

intents = discord.Intents.default()

# --- shared stream state (worker thread <-> async side) ---
_stream_stop = threading.Event()
_stream_forbidden = threading.Event()  # set on HTTP 403 -> triggers polling fallback
_stream_state = {"connected": False, "last_message_at": None, "rule_count": 0, "last_error": None}


def _headers():
    return {"Authorization": f"Bearer {X_BEARER_TOKEN}"}


# ---------------------------------------------------------------------------
# List + embed helpers (unchanged behavior from v2)
# ---------------------------------------------------------------------------
def _fetch_list_tweets(list_id: str, max_results: int = 20) -> dict:
    params = {
        "max_results": max_results,
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "name,username,profile_image_url",
    }
    resp = requests.get(f"{X_API_BASE}/lists/{list_id}/tweets", headers=_headers(), params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _fetch_list_member_usernames(list_id: str) -> list[str]:
    """All member usernames of the list (paginated). One-time cost at
    startup/sync -- this is what the stream rules get built from."""
    usernames = []
    params = {"max_results": 100, "user.fields": "username"}
    while True:
        resp = requests.get(f"{X_API_BASE}/lists/{list_id}/members", headers=_headers(), params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        for u in payload.get("data", []):
            if u.get("username"):
                usernames.append(u["username"])
        next_token = (payload.get("meta") or {}).get("next_token")
        if not next_token:
            break
        params["pagination_token"] = next_token
    return usernames


def _index_users(payload: dict) -> dict:
    users = (payload.get("includes") or {}).get("users") or []
    return {u["id"]: u for u in users}


def build_tweet_embed(tweet: dict, users_by_id: dict, matches: list[dict] = None) -> discord.Embed:
    labels = " ".join(f"{m['emoji']} {m['label']}" for m in matches) if matches else None
    author = users_by_id.get(tweet.get("author_id"), {})
    username = author.get("username", "unknown")
    tweet_id = tweet.get("id")
    tweet_url = f"https://x.com/{username}/status/{tweet_id}" if tweet_id else None

    tweet_dt = None
    created_at = tweet.get("created_at")
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
    channel_id = storage.get_config(config_key)
    if not channel_id:
        return None
    return bot.get_channel(int(channel_id))


# ---------------------------------------------------------------------------
# Stream rule building + sync
# ---------------------------------------------------------------------------
def _format_keyword(k: str) -> str:
    k = k.strip()
    return f'"{k}"' if (" " in k or not k.isalnum()) else k


def _chunk_terms(terms: list[str], budget: int, joiner: str = " OR ") -> list[str]:
    chunks, current = [], ""
    for t in terms:
        candidate = t if not current else current + joiner + t
        if len(candidate) > budget and current:
            chunks.append(current)
            current = t
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def build_stream_rules(handles: list[str]) -> list[dict]:
    """Author chunks x keyword chunks, each combined rule <= 1024 chars,
    plus one rule per account-override (all posts from that account).
    Retweets excluded everywhere -- no paying for RTs of news."""
    all_keywords = set()
    for grp in keywords.GROUPS.values():
        all_keywords.update(grp["keywords"])
    for cat in keywords.CATEGORIES.values():
        all_keywords.update(cat["keywords"])
    kw_terms = sorted({_format_keyword(k) for k in all_keywords if k.strip()})

    override_handles = {h.lower() for h in keywords.ACCOUNT_OVERRIDES}
    author_terms = [f"from:{h}" for h in handles if h.lower() not in override_handles]

    kw_chunks = _chunk_terms(kw_terms, KW_CHUNK_BUDGET)
    author_chunks = _chunk_terms(author_terms, AUTHOR_CHUNK_BUDGET)

    rules = []
    n = 0
    for ac in author_chunks:
        for kc in kw_chunks:
            value = f"({ac}) ({kc}) -is:retweet"
            if len(value) > MAX_RULE_LEN:
                raise ValueError(f"Rule {n} exceeds {MAX_RULE_LEN} chars ({len(value)}) -- lower the chunk budgets")
            rules.append({"value": value, "tag": f"beatbot:{n}"})
            n += 1
    for username in keywords.ACCOUNT_OVERRIDES:
        rules.append({"value": f"from:{username} -is:retweet", "tag": f"beatbot:override:{username}"})
    return rules


def _get_existing_rules() -> list[dict]:
    resp = requests.get(RULES_URL, headers=_headers(), timeout=15)
    resp.raise_for_status()
    return resp.json().get("data", []) or []


def _delete_rules(rule_ids: list[str]):
    if not rule_ids:
        return
    resp = requests.post(RULES_URL, headers=_headers(), json={"delete": {"ids": rule_ids}}, timeout=15)
    resp.raise_for_status()


def _add_rules(rules: list[dict]):
    resp = requests.post(RULES_URL, headers=_headers(), json={"add": rules}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    errors = payload.get("errors")
    if errors:
        raise RuntimeError(f"Some rules were rejected: {errors}")


def sync_stream_rules(force_refresh: bool = False) -> tuple[int, int]:
    """Replaces the app's stream rules. Handles come from a DB cache when
    available -- fetching the list's members is the ONLY billed part of a
    sync (~1 cent per member), so normal restarts/redeploys rebuild rules
    from cache for free. force_refresh=True (the /syncrules command)
    re-fetches from X and updates the cache -- the one moment paying is
    correct, because the list actually changed. Returns (handles, rules)."""
    handles: list[str] = []
    if not force_refresh:
        cached = storage.get_config("cached_list_handles")
        if cached:
            try:
                handles = json.loads(cached)
            except json.JSONDecodeError:
                handles = []
    if not handles:
        handles = _fetch_list_member_usernames(LIST_ID)
        if not handles:
            raise RuntimeError("List returned no members -- check LIST_ID / token access.")
        storage.set_config("cached_list_handles", json.dumps(handles))
        log.info("Fetched %d list members from X (billed lookup) and cached them", len(handles))
    else:
        log.info("Using %d cached list members (no billed lookup)", len(handles))

    rules = build_stream_rules(handles)

    existing = _get_existing_rules()
    _delete_rules([r["id"] for r in existing])
    _add_rules(rules)
    _stream_state["rule_count"] = len(rules)
    log.info("Stream rules synced: %d handles -> %d rules", len(handles), len(rules))
    return len(handles), len(rules)


# ---------------------------------------------------------------------------
# Stream worker (background thread) -> asyncio queue -> router
# ---------------------------------------------------------------------------
def _stream_worker(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    params = {
        "tweet.fields": "created_at,author_id",
        "expansions": "author_id",
        "user.fields": "name,username,profile_image_url",
    }
    backoff = 1
    while not _stream_stop.is_set():
        try:
            log.info("Connecting to filtered stream...")
            with requests.get(STREAM_URL, headers=_headers(), params=params,
                               stream=True, timeout=(10, 90)) as resp:
                if resp.status_code == 403:
                    _stream_state["connected"] = False
                    _stream_state["last_error"] = f"403: {resp.text[:300]}"
                    log.error("STREAM RETURNED 403 -- filtered stream appears unavailable on this "
                              "plan/app. Falling back to POLLING mode automatically. Body: %s", resp.text[:300])
                    _stream_forbidden.set()
                    return  # stop trying; fallback poller takes over
                if resp.status_code == 429:
                    _stream_state["connected"] = False
                    _stream_state["last_error"] = "429 rate limited"
                    log.warning("Stream rate limited (429), backing off %ss", max(backoff, 30))
                    time.sleep(max(backoff, 30))
                    backoff = min(backoff * 2, 300)
                    continue
                if resp.status_code != 200:
                    _stream_state["connected"] = False
                    _stream_state["last_error"] = f"{resp.status_code}: {resp.text[:300]}"
                    log.error("Stream connection failed (%s): %s", resp.status_code, resp.text[:300])
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                log.info("Filtered stream connected.")
                _stream_state["connected"] = True
                _stream_state["last_error"] = None
                backoff = 1

                for line in resp.iter_lines():
                    if _stream_stop.is_set():
                        return
                    if not line:
                        continue  # keep-alive newline (~every 20s)
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    _stream_state["last_message_at"] = datetime.now(timezone.utc)
                    loop.call_soon_threadsafe(queue.put_nowait, payload)
        except requests.exceptions.RequestException as e:
            _stream_state["connected"] = False
            _stream_state["last_error"] = str(e)[:300]
            log.warning("Stream dropped (%s) -- reconnecting in %ss", e, backoff)
        except Exception as e:
            _stream_state["connected"] = False
            _stream_state["last_error"] = str(e)[:300]
            log.error("Unexpected stream error: %s -- reconnecting in %ss", e, backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
    _stream_state["connected"] = False


async def _route_tweet(bot: discord.Client, tweet: dict, users_by_id: dict):
    """Identical routing to v2: account override -> groups -> general."""
    text = tweet.get("text", "")
    tweet_id = tweet.get("id")
    if not text or not tweet_id:
        return
    if storage.already_posted(tweet_id):
        return  # stream can redeliver around reconnects; dedupe holds

    author = users_by_id.get(tweet.get("author_id"), {})
    username = author.get("username", "")

    storage.mark_posted(tweet_id)

    override_group = keywords.account_override_group(username)
    if override_group:
        grp = keywords.GROUPS[override_group]
        channel = await _resolve_channel(bot, _group_channel_key(override_group))
        if channel is None:
            log.warning("No channel set for group '%s' (needed for @%s override) -- run /setgroupchannel", override_group, username)
            return
        try:
            await channel.send(embed=build_tweet_embed(
                tweet, users_by_id,
                [{"key": override_group, "emoji": grp["emoji"], "label": grp["label"]}],
            ))
            log.info("Posted tweet %s to group '%s' (account override @%s)", tweet_id, override_group, username)
        except Exception as e:
            log.error("Failed to post tweet %s: %s", tweet_id, e)
        return

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
        return

    matches = keywords.classify_tweet(text)
    if not matches:
        return  # arrived via a broad rule but didn't classify -- skip silently
    channel = await _resolve_channel(bot, "announce_channel_id")
    if channel is None:
        return
    try:
        await channel.send(embed=build_tweet_embed(tweet, users_by_id, matches))
        log.info("Posted tweet %s to general (categories: %s)", tweet_id, [m["key"] for m in matches])
    except Exception as e:
        log.error("Failed to post tweet %s: %s", tweet_id, e)


class TwitterMonitorBot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = discord.app_commands.CommandTree(self)
        self.stream_queue: asyncio.Queue = asyncio.Queue()
        self.stream_thread: threading.Thread | None = None

    async def setup_hook(self):
        storage.init_db()

        setchannel_cmd = discord.app_commands.Command(
            name="setchannel",
            description="Set this channel to receive general beat reporter alerts",
            callback=self._setchannel_callback,
        )
        self.tree.add_command(setchannel_cmd)

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

        syncrules_cmd = discord.app_commands.Command(
            name="syncrules",
            description="ADMIN: rebuild stream rules from the list's current members (run after editing the list)",
            callback=self._syncrules_callback,
        )
        self.tree.add_command(syncrules_cmd)

        streamstatus_cmd = discord.app_commands.Command(
            name="streamstatus",
            description="Show filtered-stream connection health and rule count",
            callback=self._streamstatus_callback,
        )
        self.tree.add_command(streamstatus_cmd)

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

    async def _syncrules_callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admin only.", ephemeral=True)
            return
        await interaction.response.defer()
        try:
            num_handles, num_rules = await asyncio.to_thread(sync_stream_rules, True)
        except Exception as e:
            await interaction.followup.send(f"Rule sync failed: {e}")
            return
        await interaction.followup.send(
            f"✅ Stream rules rebuilt from a fresh list fetch: **{num_handles}** writers -> **{num_rules}** rules."
        )

    async def _streamstatus_callback(self, interaction: discord.Interaction):
        if _stream_forbidden.is_set():
            status = "🟠 POLLING FALLBACK (stream returned 403 -- filtered stream unavailable on this plan)"
        elif _stream_state["connected"]:
            status = "🟢 Stream connected"
        else:
            status = "🔴 Stream disconnected (reconnecting)"
        last = _stream_state["last_message_at"]
        last_str = last.strftime("%H:%M:%S UTC") if last else "never"
        err = _stream_state.get("last_error") or "none"
        await interaction.response.send_message(
            f"{status}\nRules active: **{_stream_state['rule_count']}**\n"
            f"Last tweet received: **{last_str}**\nLast error: `{err}`"
        )

    async def on_ready(self):
        log.info("Logged in as %s", self.user)
        if self.stream_thread is None or not self.stream_thread.is_alive():
            try:
                await asyncio.to_thread(sync_stream_rules)
            except Exception as e:
                log.error("Startup rule sync failed (stream may deliver nothing until /syncrules succeeds): %s", e)
            loop = asyncio.get_running_loop()
            self.stream_thread = threading.Thread(
                target=_stream_worker, args=(loop, self.stream_queue), daemon=True
            )
            self.stream_thread.start()
        if not stream_consumer.is_running():
            stream_consumer.start(self)
        if not fallback_monitor.is_running():
            fallback_monitor.start(self)


client = TwitterMonitorBot()


@tasks.loop(seconds=1)
async def stream_consumer(bot: TwitterMonitorBot):
    """Drains the stream queue and routes tweets. Loop interval is just a
    scheduling heartbeat; the inner while empties everything available."""
    try:
        while not bot.stream_queue.empty():
            payload = bot.stream_queue.get_nowait()
            tweet = payload.get("data") or {}
            users_by_id = _index_users(payload)
            await _route_tweet(bot, tweet, users_by_id)
    except Exception as e:
        log.error("stream_consumer cycle failed: %s", e)


@stream_consumer.before_loop
async def before_consumer():
    await client.wait_until_ready()


# ---------------------------------------------------------------------------
# Polling fallback -- only activates if the stream 403s (plan doesn't
# support it). Identical behavior/cost to the v2 bot.
# ---------------------------------------------------------------------------
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "90"))


@tasks.loop(seconds=30)
async def fallback_monitor(bot: TwitterMonitorBot):
    if _stream_forbidden.is_set() and not poll_list_tweets.is_running():
        log.error("Activating POLLING FALLBACK -- stream unavailable, reverting to v2 behavior/cost.")
        poll_list_tweets.start(bot)


@fallback_monitor.before_loop
async def before_fallback_monitor():
    await client.wait_until_ready()


@tasks.loop(seconds=POLL_SECONDS)
async def poll_list_tweets(bot: TwitterMonitorBot):
    try:
        payload = await asyncio.to_thread(_fetch_list_tweets, LIST_ID, 20)
    except Exception as e:
        log.error("Failed to fetch list tweets (fallback poll): %s", e)
        return
    if "errors" in payload and not payload.get("data"):
        log.error("X API returned errors (fallback poll): %s", payload["errors"])
        return
    tweets = payload.get("data", [])
    users_by_id = _index_users(payload)
    for tweet in reversed(tweets):
        await _route_tweet(bot, tweet, users_by_id)


@poll_list_tweets.before_loop
async def before_poll():
    await client.wait_until_ready()


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Set DISCORD_TOKEN in your .env file.")
    if not X_BEARER_TOKEN:
        raise SystemExit("Set X_BEARER_TOKEN in your .env file (from developer.x.com, App > Keys and tokens).")
    if not LIST_ID:
        raise SystemExit("Set LIST_ID in your .env file (the X List containing your beat reporters).")
    client.run(DISCORD_TOKEN)
