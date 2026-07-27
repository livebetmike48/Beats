"""
Classifies beat reporter tweets and routes them to per-group Discord
channels. July 22 redesign: FOUR group channels, no general catch-all --
the old general categories (IL/roster/lineup) were removed entirely per
the Discord restructure. A tweet matching none of the groups never posts
(and on the filtered stream, never gets delivered or billed).

ACCOUNT_OVERRIDES -- accounts whose EVERY post routes to a group's channel,
no keyword match required (e.g. @MLBInjuryBot -> injury).

July 27 changes
---------------
1. "pen" added to Live Action (alongside the existing "bullpen").

2. WORD-BOUNDARY MATCHING for short single-word keywords. The old matcher
   was a plain substring test, which is what you want for phrases
   ("pinch hit" should catch "pinch hitting") but is wrong for a word as
   short as "pen": it is a substring of open, opener, happened, suspended,
   pending, penciled, spending, expensive, depending. Those tweets are
   already being delivered for other keywords, so they cost nothing extra
   -- but they would flood the Live Action channel with junk.

   Rule: a keyword that is a SINGLE token of EXACT_WORD_MAX_LEN chars or
   fewer is matched on word boundaries (\\bpen\\b). Everything longer, and
   every multi-word phrase, keeps the exact substring behavior it has
   today. No existing keyword is a single token of <= 4 chars, so this
   changes the behavior of exactly one keyword: the new "pen". Stemming
   still works everywhere it did before ("grimace" still catches
   "grimaced", "scratched" still catches "scratched.").

   X's own side needs no equivalent fix: stream rule terms are tokenized,
   so the rule term `pen` already only matches the standalone word.

3. Keywords can now live in the DB so they're editable from Discord
   (/addword, /removeword). DEFAULT_GROUPS below is the seed used on
   first boot; after that the DB is the source of truth and bot.py calls
   apply_keywords() with what it read. This module still has no imports
   from storage -- bot.py wires the two together.
"""
import copy
import re

# Single-token keywords this short get word-boundary matching instead of
# substring matching. Raising this would change existing keywords' behavior
# -- at 4, nothing that existed before July 27 is affected.
EXACT_WORD_MAX_LEN = 4

DEFAULT_GROUPS = {
    "live_action": {
        "emoji": "\u26a1",
        "label": "Live Action",
        "keywords": [
            "warming up", "getting loose", "on deck",
            "pinch hit", "pinch hitter", "pinch-hit", "pinch-hitter",
            "stretching", "bullpen", "pen",
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

# Live keyword state. Starts as a copy of the defaults so this module works
# standalone (rule building, tests); bot.py overwrites it from the DB at
# startup and after every /addword or /removeword.
GROUPS = copy.deepcopy(DEFAULT_GROUPS)

# Accounts whose every post routes to a group's channel with no keyword
# match required. Keys are lowercase X usernames WITHOUT the @.
ACCOUNT_OVERRIDES = {
    "mlbinjurybot": "injury",
}

# July 22 redesign: general categories removed -- everything routes via
# GROUPS above. Kept as an empty dict so bot.py's general-category code
# path stays valid and simply never matches.
CATEGORIES = {}

# group_key -> (substring_terms, compiled_boundary_regexes)
_MATCHERS: dict[str, tuple[list[str], list[re.Pattern]]] = {}


def _pad(text: str) -> str:
    return f" {text.lower()} "


def needs_word_boundary(phrase: str) -> bool:
    """True for single-token keywords short enough that substring matching
    would fire on unrelated longer words."""
    p = phrase.strip()
    return bool(p) and len(p) <= EXACT_WORD_MAX_LEN and not any(ch.isspace() for ch in p)


def _build_matchers():
    _MATCHERS.clear()
    for key, grp in GROUPS.items():
        subs, rgxs = [], []
        for kw in grp["keywords"]:
            k = kw.strip().lower()
            if not k:
                continue
            if needs_word_boundary(k):
                rgxs.append(re.compile(r"\b" + re.escape(k) + r"\b"))
            else:
                subs.append(k)
        _MATCHERS[key] = (subs, rgxs)


_build_matchers()


def apply_keywords(mapping: dict) -> int:
    """Overwrite the live keyword lists from {group_key: [phrase, ...]} and
    rebuild the matchers. Groups absent from the mapping keep what they
    have. Returns the number of groups updated."""
    updated = 0
    for key, phrases in (mapping or {}).items():
        if key in GROUPS:
            GROUPS[key]["keywords"] = list(phrases)
            updated += 1
    _build_matchers()
    return updated


def all_keywords() -> set[str]:
    """Every live keyword across groups and categories -- what the stream
    rule builder turns into rule terms."""
    out = set()
    for grp in GROUPS.values():
        out.update(grp["keywords"])
    for cat in CATEGORIES.values():
        out.update(cat["keywords"])
    return {k for k in out if k and k.strip()}


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
        subs, rgxs = _MATCHERS.get(key, ([], []))
        hit = any(s in text_lower for s in subs) or any(r.search(text_lower) for r in rgxs)
        if hit:
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
