"""
Classifies beat reporter tweets and routes them to per-group Discord
channels. July 22 redesign: FOUR group channels, no general catch-all --
the old general categories (IL/roster/lineup) were removed entirely per
the Discord restructure. A tweet matching none of the groups never posts
(and on the filtered stream, never gets delivered or billed).

ACCOUNT_OVERRIDES -- accounts whose EVERY post routes to a group's channel,
no keyword match required (e.g. @MLBInjuryBot -> injury).
"""

GROUPS = {
    "live_action": {
        "emoji": "\u26a1",
        "label": "Live Action",
        "keywords": [
            "warming up", "getting loose", "on deck",
            "pinch hit", "pinch hitter", "pinch-hit", "pinch-hitter",
            "stretching", "bullpen",
        ],
    },
    "injury": {
        "emoji": "\U0001f691",
        "label": "Injury Watch",
        "keywords": [
            "trainer visit", "favoring", "limping",
            "grimace", "grimacing", "not right", "medical staff",
            "down tunnel", "down the tunnel",
            "down clubhouse", "down the clubhouse",
            "down stairs", "down the stairs",
        ],
    },
    "scratched": {
        "emoji": "\U0001f500",
        "label": "Starter Scratched",
        "keywords": [
            "scratched", "pushed back", "bumped back",
            "rotation change", "rotation shifted", "rotation swap",
            "skipping turn", "skipping his turn", "skip his turn",
            "will not make start", "will not make his start",
            "scheduled start", "no longer starting",
            "start moved", "start has been moved", "will now start on",
            "extra rest",
        ],
    },
    "limit": {
        "emoji": "\u26be",
        "label": "Limit",
        "keywords": [
            "pitch count", "pitch limit", "innings limit", "on a limit",
            "workload",
            "manage innings", "manage his innings", "managing his innings",
            "innings management",
            "piggyback", "piggy back",
        ],
    },
}

# Accounts whose every post routes to a group's channel with no keyword
# match required. Keys are lowercase X usernames WITHOUT the @.
ACCOUNT_OVERRIDES = {
    "mlbinjurybot": "injury",
}

# July 22 redesign: general categories removed -- everything routes via
# GROUPS above. Kept as an empty dict so bot.py's general-category code
# path stays valid and simply never matches.
CATEGORIES = {}


def _pad(text: str) -> str:
    return f" {text.lower()} "


def account_override_group(username: str) -> str | None:
    """If this account bypasses keywords, return the group name it routes to."""
    if not username:
        return None
    return ACCOUNT_OVERRIDES.get(username.lower().lstrip("@"))


def classify_groups(text: str) -> list[dict]:
    """Returns matched GROUP dicts (key/emoji/label), empty if none match."""
    text_lower = _pad(text)
    matches = []
    for key, grp in GROUPS.items():
        if any(kw in text_lower for kw in grp["keywords"]):
            matches.append({"key": key, "emoji": grp["emoji"], "label": grp["label"]})
    return matches


def classify_tweet(text: str) -> list[dict]:
    """General-category classifier -- categories were removed in the July 22
    redesign, so this always returns []. Kept for bot.py compatibility."""
    text_lower = _pad(text)
    matches = []
    for key, cat in CATEGORIES.items():
        if any(kw in text_lower for kw in cat["keywords"]):
            matches.append({"key": key, "emoji": cat["emoji"], "label": cat["label"]})
    return matches
