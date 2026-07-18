"""
Classifies beat reporter tweets into betting-relevant categories based on
keyword matching, and routes them to per-group Discord channels.

Two layers:
1. GROUPS  -- specific high-signal bundles, each with its own channel
              (set via /setgroupchannel in Discord). Checked first.
2. CATEGORIES -- the original general-info buckets. Anything that matches
              only these goes to the general channel (/setchannel).

ACCOUNT_OVERRIDES -- accounts whose EVERY post routes to a group's channel,
              no keyword match required (e.g. @MLBInjuryBot -> injury).
"""

# ---------------------------------------------------------------------------
# Per-channel keyword groups (checked first). Key = group name used by
# /setgroupchannel. Keyword matching is simple case-insensitive substring
# against the padded lowercase tweet text.
# ---------------------------------------------------------------------------
GROUPS = {
    "live_action": {
        "emoji": "⚡",
        "label": "Live Action",
        "keywords": [
            "warming up", "getting loose", "on deck", "pinch hit", "pinch-hit",
            "velocity down", "velo down", "stretching",
        ],
    },
    "injury": {
        "emoji": "🚑",
        "label": "Injury Watch",
        "keywords": [
            "trainer visit", "hurt", "injured", "grabbing at", "favoring",
            "limping", "shaking his arm", "wincing",
            "removed with an apparent injury", "exited with injury",
            "not right", "medical staff", "training staff",
        ],
    },
    "scratched": {
        "emoji": "🔀",
        "label": "Starter Scratched",
        "keywords": [
            "scratched", "pushed back", "bumped back", "shifted back",
            "moved back in the rotation", "skipping his turn", "skip his turn",
            "no longer starting", "will not make his scheduled start",
            "rotation swap", "swapped rotation spots",
            "start has been moved to", "will now start on",
            "placed on extra rest",
        ],
    },
}

# Accounts whose every post routes to a group's channel with no keyword
# match required. Keys are lowercase X usernames WITHOUT the @.
ACCOUNT_OVERRIDES = {
    "mlbinjurybot": "injury",
}

# ---------------------------------------------------------------------------
# Original general-info categories. A tweet matching ONLY these (no group
# match) posts to the general channel set via /setchannel.
# ---------------------------------------------------------------------------
CATEGORIES = {
    "injury": {
        "emoji": "🚑",
        "label": "Injury/IL",
        "keywords": [
            "injured list", " il ", "il with", "placed on the il", "activated from",
            "mri", "surgery", "hamstring", "oblique", "elbow", "shoulder", "forearm",
            "day-to-day", "day to day", "scratched", "left the game", "exited the game",
            "tightness", "soreness", "strain", "sprain", "tommy john",
            "injury", "injured",
        ],
    },
    "pitch_limit": {
        "emoji": "⚾",
        "label": "Pitch Count/Limit",
        "keywords": [
            "pitch count", "pitch limit", "innings limit", "on a limit",
            "shut down", "shut it down", "will not pitch", "skipping his start",
            "pushed back", "extra rest", "limited to",
        ],
    },
    "bullpen": {
        "emoji": "🔥",
        "label": "Bullpen",
        "keywords": [
            "bullpen", "piggyback", "piggy back",
        ],
    },
    "lineup": {
        "emoji": "📋",
        "label": "Lineup Move",
        "keywords": [
            "starting lineup", "batting order", "out of the lineup", "day off",
            "leadoff", "batting cleanup", "will start tonight", "starting tonight",
            "not in the lineup", "getting the night off", "sitting tonight",
            "on deck", "pinch hit", "pinch-hit",
        ],
    },
    "roster": {
        "emoji": "🔄",
        "label": "Roster Move",
        "keywords": [
            "recalled", "optioned", "designated for assignment", " dfa'd", " dfa ",
            "called up", "selected his contract", "outrighted", "claimed off waivers",
            "released", "signed a", "trade", "traded",
        ],
    },
}


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
    """Original general-category classifier. Returns matched category dicts."""
    text_lower = _pad(text)
    matches = []
    for key, cat in CATEGORIES.items():
        if any(kw in text_lower for kw in cat["keywords"]):
            matches.append({"key": key, "emoji": cat["emoji"], "label": cat["label"]})
    return matches
