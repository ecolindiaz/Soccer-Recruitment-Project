"""
league_overlap.py — find the leagues we can actually model & trade.

The betting thesis only works on leagues that exist in ALL THREE data worlds:

  1. football-data.co.uk  — historical results + CLOSING ODDS (incl. Pinnacle).
                            This is the load-bearing source: it's the only one
                            that gives a historical *market price* to beat.
  2. FotMob               — xG / shots / lineups / form (the signal features),
                            via the scraper in ../fotmob.
  3. Kalshi               — where we'd actually place the prematch trade.

A league missing from any one of the three is untradeable-or-unmodelable, so
this script computes the intersection and emits the shortlist, with the IDs
each downstream step needs (football-data code for odds, FotMob league id for
features).

WHY THIS ISN'T FULLY AUTOMATIC
------------------------------
- football-data's league catalog is small and stable, so it's hard-coded below.
- FotMob's list is fetched live from the same endpoint the scraper uses.
- Kalshi has no clean "list all soccer leagues" feed, and its coverage is the
  whole question, so you supply that list yourself from the tradability audit
  (see --kalshi). Provide an empty file to just see the football-data ∩ FotMob
  overlap (still useful — that's the modelable universe before tradability).

USAGE
-----
    # 1. do the Kalshi audit, put one tradable league name per line:
    #      Danish Superliga
    #      Eliteserien
    #      ...
    # 2. then:
    python league_overlap.py --kalshi kalshi_leagues.txt --out overlap.csv

Requires: requests (already in ../fotmob/requirements.txt). pandas optional
(nicer CSV); falls back to the stdlib csv module if it's absent.

NOTE ON HISTORY DEPTH: downstream we pull 5 seasons for the shortlist (see
SEASONS_BACK). This file only selects leagues; the focused scrape/backtest
consumes SEASONS_BACK.
"""

import argparse
import csv
import sys
from difflib import SequenceMatcher

import requests

# How many seasons of history the downstream backtest uses for the shortlist.
SEASONS_BACK = 5

# Same endpoint + headers the FotMob scraper uses, for consistency.
FOTMOB_ALL_LEAGUES = "https://www.fotmob.com/api/data/allLeagues"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fotmob.com",
}

# Fuzzy-match acceptance threshold (0..1). Below this we treat it as "no match".
MATCH_THRESHOLD = 0.72

# --------------------------------------------------------------------------
# football-data.co.uk catalog.
# `code` is the file/division code used in their CSV URLs. `fmt` marks which of
# their two formats a league lives in: "main" = the per-division season CSVs
# that carry full closing-odds columns (this is what we want); "extra" = the
# single "new leagues" file, which has RESULTS + a thinner odds set. Country is
# used to constrain fuzzy matching against FotMob.
# --------------------------------------------------------------------------
FOOTBALL_DATA_LEAGUES = [
    # England
    {"code": "E0",  "country": "England",     "name": "Premier League",        "fmt": "main"},
    {"code": "E1",  "country": "England",     "name": "Championship",          "fmt": "main"},
    {"code": "E2",  "country": "England",     "name": "League One",            "fmt": "main"},
    {"code": "E3",  "country": "England",     "name": "League Two",            "fmt": "main"},
    {"code": "EC",  "country": "England",     "name": "National League",       "fmt": "main"},
    # Scotland
    {"code": "SC0", "country": "Scotland",    "name": "Premiership",           "fmt": "main"},
    {"code": "SC1", "country": "Scotland",    "name": "Championship",          "fmt": "main"},
    {"code": "SC2", "country": "Scotland",    "name": "League One",            "fmt": "main"},
    {"code": "SC3", "country": "Scotland",    "name": "League Two",            "fmt": "main"},
    # Germany
    {"code": "D1",  "country": "Germany",     "name": "Bundesliga",            "fmt": "main"},
    {"code": "D2",  "country": "Germany",     "name": "2. Bundesliga",         "fmt": "main"},
    # Italy
    {"code": "I1",  "country": "Italy",       "name": "Serie A",               "fmt": "main"},
    {"code": "I2",  "country": "Italy",       "name": "Serie B",               "fmt": "main"},
    # Spain
    {"code": "SP1", "country": "Spain",       "name": "La Liga",               "fmt": "main"},
    {"code": "SP2", "country": "Spain",       "name": "La Liga 2",             "fmt": "main"},
    # France
    {"code": "F1",  "country": "France",      "name": "Ligue 1",               "fmt": "main"},
    {"code": "F2",  "country": "France",      "name": "Ligue 2",               "fmt": "main"},
    # Rest of main set (top flights)
    {"code": "N1",  "country": "Netherlands", "name": "Eredivisie",            "fmt": "main"},
    {"code": "B1",  "country": "Belgium",     "name": "Pro League",            "fmt": "main"},
    {"code": "P1",  "country": "Portugal",    "name": "Primeira Liga",         "fmt": "main"},
    {"code": "T1",  "country": "Turkey",      "name": "Super Lig",             "fmt": "main"},
    {"code": "G1",  "country": "Greece",      "name": "Super League",          "fmt": "main"},
    # "New leagues" (extra file, thinner odds — usable but check columns)
    {"code": "ARG", "country": "Argentina",   "name": "Liga Profesional",     "fmt": "extra"},
    {"code": "AUT", "country": "Austria",      "name": "Bundesliga",           "fmt": "extra"},
    {"code": "BRA", "country": "Brazil",       "name": "Serie A",              "fmt": "extra"},
    {"code": "CHN", "country": "China",        "name": "Super League",         "fmt": "extra"},
    {"code": "DNK", "country": "Denmark",      "name": "Superliga",            "fmt": "extra"},
    {"code": "FIN", "country": "Finland",      "name": "Veikkausliiga",        "fmt": "extra"},
    {"code": "IRL", "country": "Ireland",      "name": "Premier Division",     "fmt": "extra"},
    {"code": "JPN", "country": "Japan",        "name": "J1 League",            "fmt": "extra"},
    {"code": "MEX", "country": "Mexico",       "name": "Liga MX",              "fmt": "extra"},
    {"code": "NOR", "country": "Norway",       "name": "Eliteserien",          "fmt": "extra"},
    {"code": "POL", "country": "Poland",       "name": "Ekstraklasa",          "fmt": "extra"},
    {"code": "ROU", "country": "Romania",      "name": "Liga I",               "fmt": "extra"},
    {"code": "RUS", "country": "Russia",       "name": "Premier League",       "fmt": "extra"},
    {"code": "SWE", "country": "Sweden",       "name": "Allsvenskan",          "fmt": "extra"},
    {"code": "SWZ", "country": "Switzerland",  "name": "Super League",         "fmt": "extra"},
    {"code": "USA", "country": "USA",          "name": "MLS",                  "fmt": "extra"},
]

# football-data country name -> FotMob country name, where they differ.
COUNTRY_ALIASES = {
    "USA": "USA",
    "China": "China PR",
}

def _norm(s):
    """Normalize for fuzzy comparison: lowercase, punctuation -> space, collapse
    whitespace. Deliberately keeps every word — the distinguishing tokens in a
    league name ARE words like 'premier'/'super'/'league', so stripping them
    (an earlier bug) made unrelated leagues collapse to empty and match at 1.0."""
    s = (s or "").lower()
    s = "".join(c if c.isalnum() else " " for c in s)
    return " ".join(s.split())


def _ratio(a, b):
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()


def fetch_fotmob_leagues():
    """Return a flat list of {id, ccode, country, name} from FotMob allLeagues.

    Covers popular/international plus every country's leagues. Raises on a
    hard network/HTTP failure so the caller knows the FotMob side is missing
    rather than silently producing a half-answer.
    """
    resp = requests.get(FOTMOB_ALL_LEAGUES, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    out = []
    for country in data.get("countries", []) or []:
        if not isinstance(country, dict):
            continue
        cname = country.get("name")
        ccode = country.get("ccode")
        for lg in country.get("leagues", []) or []:
            if isinstance(lg, dict) and lg.get("id"):
                out.append({"id": lg.get("id"), "ccode": ccode,
                            "country": cname, "name": lg.get("name")})
    return out


def best_fotmob_match(fd_league, fotmob_leagues):
    """Best FotMob league for a football-data league, constrained to the same
    country first, then falling back to a global best if the country match is
    weak. Returns (match_dict, score) or (None, 0.0)."""
    target_country = COUNTRY_ALIASES.get(fd_league["country"], fd_league["country"])

    # Restrict to same-country candidates. If none exist we return NO match
    # rather than falling back to a global best — matching a league to a
    # different country is meaningless and produces false positives.
    same_country = [f for f in fotmob_leagues
                    if _ratio(f["country"], target_country) >= 0.85]
    if not same_country:
        return (None, 0.0)

    best, best_score = None, 0.0
    for f in same_country:
        score = _ratio(fd_league["name"], f["name"])
        if score > best_score:
            best, best_score = f, score
    return (best, best_score) if best_score >= MATCH_THRESHOLD else (None, best_score)


def load_kalshi(path):
    """Read the Kalshi-tradable league names (one per line, '#' comments ok).

    Returns None if no path given (caller then reports football-data ∩ FotMob
    only). Returns a possibly-empty list otherwise.
    """
    if not path:
        return None
    names = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
    return names


def kalshi_match(fd_league, kalshi_names):
    """Best fuzzy match of a league to the user's Kalshi list. Returns
    (name, score) or (None, 0.0)."""
    best, best_score = None, 0.0
    for k in kalshi_names:
        score = _ratio(fd_league["name"] + " " + fd_league["country"], k)
        score = max(score, _ratio(fd_league["name"], k))
        if score > best_score:
            best, best_score = k, score
    return (best, best_score) if best_score >= MATCH_THRESHOLD else (None, best_score)


def build_rows(fotmob_leagues, kalshi_names):
    rows = []
    for fd in FOOTBALL_DATA_LEAGUES:
        fm, fm_score = best_fotmob_match(fd, fotmob_leagues)
        has_fotmob = fm is not None

        if kalshi_names is None:
            has_kalshi, k_name, k_score = None, "", 0.0
        else:
            k_name, k_score = kalshi_match(fd, kalshi_names)
            has_kalshi = k_name is not None

        # "all three": football-data is a given (we're iterating its catalog).
        if kalshi_names is None:
            in_all = has_fotmob                      # football-data ∩ FotMob
        else:
            in_all = has_fotmob and bool(has_kalshi)  # all three

        rows.append({
            "in_all_three": in_all,
            "country": fd["country"],
            "league": fd["name"],
            "footballdata_code": fd["code"],
            "footballdata_fmt": fd["fmt"],
            "fotmob_id": fm["id"] if fm else "",
            "fotmob_name": fm["name"] if fm else "",
            "fotmob_score": round(fm_score, 2),
            "kalshi_match": k_name,
            "kalshi_score": round(k_score, 2),
        })
    # Shortlist first, then by country/league.
    rows.sort(key=lambda r: (not r["in_all_three"], r["country"], r["league"]))
    return rows


def write_csv(rows, path):
    fields = list(rows[0].keys())
    try:
        import pandas as pd
        pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8")
    except Exception:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)


def print_summary(rows, kalshi_names):
    shortlist = [r for r in rows if r["in_all_three"]]
    third = "all three sources" if kalshi_names is not None else "football-data ∩ FotMob"
    print(f"\n=== SHORTLIST ({len(shortlist)} leagues in {third}) ===")
    print(f"{'Country':<14}{'League':<22}{'FD':<5}{'FotMob ID':<11}{'Kalshi'}")
    print("-" * 70)
    for r in shortlist:
        print(f"{r['country']:<14}{r['league']:<22}{r['footballdata_code']:<5}"
              f"{str(r['fotmob_id']):<11}{r['kalshi_match']}")
    if kalshi_names is None:
        print("\n(No Kalshi list supplied — this is the MODELABLE universe. Re-run "
              "with --kalshi once you've done the tradability audit to get the "
              "truly TRADEABLE shortlist.)")
    print(f"\nHistory depth downstream: {SEASONS_BACK} seasons.\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--kalshi", help="path to a file of Kalshi-tradable league "
                    "names (one per line). Omit to see football-data ∩ FotMob only.")
    ap.add_argument("--out", default="overlap.csv", help="output CSV path "
                    "(default: overlap.csv)")
    args = ap.parse_args()

    try:
        kalshi_names = load_kalshi(args.kalshi)
    except OSError as e:
        print(f"Couldn't read Kalshi file: {e}", file=sys.stderr)
        return 2

    print("Fetching FotMob league list...")
    try:
        fotmob_leagues = fetch_fotmob_leagues()
    except Exception as e:
        print(f"FotMob fetch failed ({type(e).__name__}: {e}). "
              "Check network access to fotmob.com.", file=sys.stderr)
        return 1
    print(f"  {len(fotmob_leagues)} FotMob leagues loaded.")

    rows = build_rows(fotmob_leagues, kalshi_names)
    write_csv(rows, args.out)
    print_summary(rows, kalshi_names)
    print(f"Full matrix (incl. near-misses to eyeball) written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
