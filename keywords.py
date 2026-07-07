"""
Classifies beat reporter tweets into betting-relevant categories based on
keyword matching. Designed to be conservative-ish (avoid firing on totally
unrelated tweets) while catching the real signal: injuries, pitch limits,
lineup moves, roster moves.
"""

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


def classify_tweet(text: str) -> list[dict]:
    """Returns a list of matched category dicts (emoji/label/key), empty if no match."""
    text_lower = f" {text.lower()} "  # pad with spaces so word-boundary-ish keywords match cleanly
    matches = []
    for key, cat in CATEGORIES.items():
        if any(kw in text_lower for kw in cat["keywords"]):
            matches.append({"key": key, "emoji": cat["emoji"], "label": cat["label"]})
    return matches
