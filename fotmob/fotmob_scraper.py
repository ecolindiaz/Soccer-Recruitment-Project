import requests
import pandas as pd
import os
import time
import random
import re
import json
import concurrent.futures
import threading
from datetime import datetime
from tqdm import tqdm
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# =============================================================================
# ⚙️ CONFIGURATION
# =============================================================================
BASE_OUTPUT_FOLDER = "fotmob_data"

# How many requests to run at once. Start here — if you see 429s or a rising
# error rate in the console, drop this to 3-4. If it's running clean after
# 15-20 minutes, 10-12 is probably still safe. Don't go much higher than that;
# the goal is meaningfully faster, not maximally fast.
MAX_WORKERS = 8

# Set of tier names to SKIP match-scraping for entirely — goes straight to
# player/team profiling using whatever match data already exists on disk.
# Use this when you're confident a tier's matches are already fully scraped
# and don't want the resume-check overhead/risk of it deciding otherwise.
# Leave as an empty set to scrape every tier; e.g. {"Tier0", "Tier1"} to skip.
SKIP_MATCH_PHASE_TIERS = set()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fotmob.com",
}

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Shared session — reuses TCP/TLS connections across requests instead of
# opening a fresh one each time. Thread-safe for concurrent use by design.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Tracks consecutive 429s across threads so we can back off globally, not
# just per-request, if the server starts pushing back on our overall pace.
_rate_limit_lock = threading.Lock()
_consecutive_429s = 0

# Season depth per priority tier — deepest history for what matters most,
# shallower for the long tail where 15-year-old lower-division history has
# little value. Edit to taste.
SEASON_CAP_TIER0 = 12   # popular + international tournaments
SEASON_CAP_TIER1 = 8    # major countries' FULL domestic pyramid (all leagues/cups)
SEASON_CAP_TIER2 = 6    # every other country's top flight
SEASON_CAP_TIER3 = 3    # everything else, everywhere (2nd tiers, cups, lower divisions)

# Countries that get their ENTIRE domestic structure included (not just
# top flight) — England alone has 23 competitions once you count the full
# pyramid + cups, this is where most of the "importance" budget goes.
MAJOR_CCODES = {"ENG", "ESP", "GER", "ITA", "FRA", "NED", "POR"}

# Keywords that flag a genuine domestic cup / qualifier / playoff — used
# only to pick the correct TOP-FLIGHT entry for tier-2 countries (confirmed
# necessary: Armenia's first list entry was "Armenian Cup", Lithuania's was
# "A Lyga Qualification", not the real leagues). Everything else, cups
# included, still gets scraped eventually — just in tier 3, not mistaken
# for a country's real league.
EXCLUDE_KEYWORDS = ["cup", "qualification", "quals", "playoff", "play-off",
                     "super cup", "trophy", "shield", "relegation"]

# Fixed, already-confirmed-correct IDs for continental cups and international
# tournaments — these don't map cleanly onto the per-country structure, so
# they're merged in separately as guaranteed tier-0 entries.
CUPS_AND_INTERNATIONAL = [
    (42, "Cup_Champions_League"), (73, "Cup_Europa_League"), (10216, "Cup_Conference_League"),
    (77, "Intl_World_Cup"), (50, "Intl_EURO"), (44, "Intl_Copa_America"),
    (132, "ENG_Cup_FA_Cup"), (133, "ENG_Cup_EFL_Carabao_Cup"),
    (134, "FRA_Cup_Coupe_de_France"), (138, "ESP_Cup_Copa_del_Rey"),
    (209, "GER_Cup_DFB_Pokal"), (141, "ITA_Cup_Coppa_Italia"),
]


# =============================================================================
# 🔧 SHARED HELPERS
# =============================================================================
def get_json(url, debug_label=None, max_retries=3):
    """Direct JSON fetch with retry-backoff — no HTML/regex parsing needed.
    Uses the shared SESSION for connection reuse. Tracks consecutive 429s
    globally so a sustained rate-limit signal pauses ALL threads, not just
    the one that hit it."""
    global _consecutive_429s
    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.get(url, timeout=15)

            if resp.status_code == 200:
                with _rate_limit_lock:
                    _consecutive_429s = 0
                try:
                    return resp.json()
                except Exception:
                    return None

            if resp.status_code == 429:
                with _rate_limit_lock:
                    _consecutive_429s += 1
                    streak = _consecutive_429s
                # If several threads are all hitting 429 at once, this is a
                # real signal to slow down hard, not just retry politely.
                if streak >= 5:
                    print(f"   🛑 Multiple consecutive 429s — pausing 30s to cool down.")
                    time.sleep(30)
                    with _rate_limit_lock:
                        _consecutive_429s = 0

            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait)
                continue

            if debug_label and resp.status_code != 404:
                print(f"   ⚠️  {debug_label}: status {resp.status_code}")
            return None
        except Exception as e:
            if attempt < max_retries:
                time.sleep((2 ** attempt) + random.uniform(0, 1))
                continue
            if debug_label:
                print(f"   ⚠️  {debug_label}: exception {e}")
            return None
    return None


def flatten_json(y, prefix=''):
    out = {}
    if isinstance(y, dict):
        keys = set(y.keys())
        if keys <= {'value', 'fmt'} and 'value' in keys:
            return y['value']
        for key, val in y.items():
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(val, dict):
                flattened = flatten_json(val, new_key)
                out.update(flattened) if isinstance(flattened, dict) else out.update({new_key: flattened})
            elif isinstance(val, list):
                out[new_key] = str(val)
            else:
                out[new_key] = val
    return out


def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def save_batch(folder_path, type_name, new_rows):
    if not new_rows:
        return
    df = pd.DataFrame(new_rows)
    df.columns = [str(c).replace(' ', '_').replace('.', '') for c in df.columns]
    filename = os.path.join(folder_path, f"{type_name}.csv")
    if not os.path.exists(filename):
        df.to_csv(filename, mode='w', header=True, index=False, encoding='utf-8')
    else:
        try:
            existing_cols = pd.read_csv(filename, nrows=0).columns.tolist()
            if set(df.columns) - set(existing_cols):
                existing_df = pd.read_csv(filename, low_memory=False)
                pd.concat([existing_df, df], ignore_index=True).to_csv(
                    filename, mode='w', header=True, index=False, encoding='utf-8')
            else:
                df.reindex(columns=existing_cols).to_csv(
                    filename, mode='a', header=False, index=False, encoding='utf-8')
        except Exception:
            df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8')


def get_existing_ids(folder_path, filename, id_col):
    path = os.path.join(folder_path, filename)
    if not os.path.exists(path):
        return set()
    try:
        df = pd.read_csv(path, usecols=[id_col], low_memory=False)
        return set(df[id_col].dropna().unique())
    except Exception:
        return set()


def collect_all_ids_from_disk(match_folder):
    """
    Rebuilds the full player/team ID worklist by reading it back off every
    league's saved CSVs, rather than relying on in-memory tracking during
    the match-scraping loop. This is what makes player/team profiling
    properly resumable across crashes/restarts — matches scraped in a
    PREVIOUS run never re-enter the in-memory collection this run, so
    without this disk scan their players/teams would silently never get
    profiled even though the match data itself is safely saved.
    """
    player_ids = set()
    team_ids = set()

    if not os.path.exists(match_folder):
        return player_ids, team_ids

    for league_folder in os.listdir(match_folder):
        league_path = os.path.join(match_folder, league_folder)
        if not os.path.isdir(league_path):
            continue

        lineups_path = os.path.join(league_path, "lineups.csv")
        if os.path.exists(lineups_path):
            try:
                ln = pd.read_csv(lineups_path, usecols=['id'], low_memory=False)
                for v in ln['id'].dropna().unique():
                    try:
                        player_ids.add(int(v))
                    except (ValueError, TypeError):
                        pass
            except Exception:
                pass

        match_info_path = os.path.join(league_path, "match_info.csv")
        if os.path.exists(match_info_path):
            try:
                mi = pd.read_csv(match_info_path, usecols=['Home_ID', 'Away_ID'], low_memory=False)
                for col in ['Home_ID', 'Away_ID']:
                    for v in mi[col].dropna().unique():
                        try:
                            team_ids.add(int(v))
                        except (ValueError, TypeError):
                            pass
            except Exception:
                pass

    return player_ids, team_ids


# =============================================================================
# PHASE 0 — DYNAMIC LEAGUE LIST BUILDING
# =============================================================================
def build_league_list():
    """
    Builds the FULL scrape list at runtime, every competition FotMob tracks
    across all 94 countries, ordered into priority tiers so an interrupted
    run has already captured the highest-value data:

      Tier 0: 'popular' + 'international' from FotMob itself, plus the
              confirmed-correct continental/international cup IDs.
      Tier 1: EVERY competition (full domestic pyramid + cups) for the
              major footballing nations (big-5 + NED/POR).
      Tier 2: The real top-flight league for every OTHER country.
      Tier 3: Everything else, everywhere — second divisions, domestic
              cups, qualifiers, lower tiers. The long tail, ~350+ items.

    EXCLUDE_KEYWORDS only affects which entry gets picked as tier 2's "the
    real league" for non-major countries — nothing is dropped from the
    scrape entirely, cups/qualifiers just land in tier 3 instead of being
    mistaken for a country's actual top flight (confirmed real problem —
    see comment on EXCLUDE_KEYWORDS above).
    """
    print("🌍 Building full league list from live FotMob data...")
    data = get_json("https://www.fotmob.com/api/data/allLeagues", debug_label="allLeagues")
    if not data:
        print("   ⚠️  Couldn't fetch allLeagues — falling back to cups/international only.")
        return [(lid, name, SEASON_CAP_TIER0) for lid, name in CUPS_AND_INTERNATIONAL]

    countries = data.get('countries', [])
    seen_ids = set()
    tiered_list = []  # (league_id, folder_name, season_cap) in final priority order

    def safe_name(name, lid):
        return re.sub(r'[^A-Za-z0-9]+', '_', name or f'league_{lid}')

    def add(lid, folder_name, cap):
        if lid and lid not in seen_ids:
            seen_ids.add(lid)
            tiered_list.append((lid, folder_name, cap))

    # --- TIER 0: popular + international + confirmed cups/tournaments ---
    for entry in data.get('popular', []) or []:
        if isinstance(entry, dict):
            add(entry.get('id'), f"Tier0_Popular_{safe_name(entry.get('name'), entry.get('id'))}",
                SEASON_CAP_TIER0)
    for entry in data.get('international', []) or []:
        if isinstance(entry, dict):
            add(entry.get('id'), f"Tier0_Intl_{safe_name(entry.get('name'), entry.get('id'))}",
                SEASON_CAP_TIER0)
    for lid, name in CUPS_AND_INTERNATIONAL:
        add(lid, f"Tier0_{name}", SEASON_CAP_TIER0)

    # --- TIER 1: full domestic pyramid for major countries ---
    for country in countries:
        if not isinstance(country, dict):
            continue
        ccode = country.get('ccode')
        if ccode not in MAJOR_CCODES:
            continue
        for lg in country.get('leagues', []) or []:
            if isinstance(lg, dict):
                add(lg.get('id'), f"Tier1_{ccode}_{safe_name(lg.get('name'), lg.get('id'))}",
                    SEASON_CAP_TIER1)

    # --- TIER 2: the real top flight for every other country ---
    tier2_covered_ids = set()  # track so tier 3 doesn't re-add these
    for country in countries:
        if not isinstance(country, dict):
            continue
        ccode = country.get('ccode')
        if ccode in MAJOR_CCODES:
            continue
        leagues_here = country.get('leagues', []) or []
        if not leagues_here:
            continue
        # Pick the first entry that doesn't look like a cup/qualifier
        top_flight = None
        for lg in leagues_here:
            name_lower = (lg.get('name') or '').lower()
            if not any(kw in name_lower for kw in EXCLUDE_KEYWORDS):
                top_flight = lg
                break
        if not top_flight:  # every entry was excluded — fall back to raw first
            top_flight = leagues_here[0]

        lid = top_flight.get('id')
        add(lid, f"Tier2_{ccode}_{safe_name(top_flight.get('name'), lid)}", SEASON_CAP_TIER2)
        tier2_covered_ids.add(lid)

    # --- TIER 3: everything else, everywhere — the long tail ---
    for country in countries:
        if not isinstance(country, dict):
            continue
        ccode = country.get('ccode')
        if ccode in MAJOR_CCODES:
            continue  # already fully covered in tier 1
        for lg in country.get('leagues', []) or []:
            if isinstance(lg, dict):
                add(lg.get('id'), f"Tier3_{ccode}_{safe_name(lg.get('name'), lg.get('id'))}",
                    SEASON_CAP_TIER3)

    print(f"   ✅ Built list of {len(tiered_list)} total leagues/competitions.")
    tier_counts = {}
    for _, name, _ in tiered_list:
        tier = name.split('_')[0]
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    for tier, count in sorted(tier_counts.items()):
        print(f"      {tier}: {count} competitions")
    print()

    return tiered_list


NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL
)


def get_matches_for_season(league_id, season_str):
    """
    IMPORTANT: /api/data/leagues ignores the season query param and always
    returns the CURRENT season regardless of what's requested (confirmed —
    requesting season=2023-2024 returned 2026/2027 fixtures). The HTML/
    __NEXT_DATA__ league page is the only method confirmed to actually
    respect historical season selection, so that's what we use here.
    matchDetails/playerData/teams stay on the fast direct-JSON endpoints —
    only fixture discovery needs this heavier path.
    """
    for season_param in [season_str.replace('/', '-'), season_str]:
        url = f"https://www.fotmob.com/leagues/{league_id}/matches?season={season_param}"
        try:
            resp = SESSION.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            match = NEXT_DATA_RE.search(resp.text)
            if not match:
                continue
            next_data = json.loads(match.group(1))
            page_props = next_data.get('props', {}).get('pageProps', {})
            fixtures = page_props.get('fixtures', {})
            all_matches = fixtures.get('allMatches', [])
            match_ids = [m['id'] for m in all_matches if isinstance(m, dict) and 'id' in m]
            if match_ids:
                # Return a dict shaped like the JSON endpoint's response so
                # parse_league_table/parse_league_transfers still work —
                # page_props has the same table/transfers keys.
                return match_ids, page_props
        except Exception:
            continue
    return None, None


def parse_league_table(league_id, league_name, season_label, data):
    rows = []
    table = data.get('table', [])
    for entry in table if isinstance(table, list) else []:
        table_data = entry.get('data', {})
        for split_name in ['all', 'home', 'away', 'form', 'xg']:
            for team_row in table_data.get('table', {}).get(split_name, []) or []:
                r = {'League': league_name, 'League_ID': league_id, 'Season': season_label,
                     'Split': split_name}
                r.update(flatten_json(team_row, ''))
                rows.append(r)
    return rows


def parse_league_transfers(league_id, league_name, season_label, data):
    rows = []
    transfers = data.get('transfers')
    if not transfers:
        return rows
    entries = transfers.get('data', []) if isinstance(transfers, dict) else []
    for t in entries or []:
        if not isinstance(t, dict):
            continue
        r = {'League': league_name, 'League_ID': league_id, 'Season': season_label}
        r.update(flatten_json(t, ''))
        rows.append(r)
    return rows


# =============================================================================
# PHASE 2 — MATCH SCRAPE
# =============================================================================
def get_match_details(match_id):
    return get_json(f"https://www.fotmob.com/api/data/matchDetails?matchId={match_id}",
                     debug_label=f"match {match_id}")


def get_base_info(match_id, season_label, league_name, league_id, data):
    general = data.get('general', {})
    header = data.get('header', {})
    teams = header.get('teams', [{}, {}])
    return {
        'League': league_name, 'League_ID': league_id, 'Season': season_label,
        'Match_ID': match_id, 'Match_Time': general.get('matchTimeUTC'),
        'Home_Team': teams[0].get('name') if len(teams) > 0 else 'Unknown',
        'Home_ID': teams[0].get('id') if len(teams) > 0 else None,
        'Away_Team': teams[1].get('name') if len(teams) > 1 else 'Unknown',
        'Away_ID': teams[1].get('id') if len(teams) > 1 else None,
        'Score': header.get('status', {}).get('scoreStr')
    }


def parse_match_info(match_id, season_label, league_name, league_id, data):
    try:
        row = get_base_info(match_id, season_label, league_name, league_id, data)
        if 'general' in data:
            row.update(flatten_json(data['general'], 'Gen'))
        if 'header' in data:
            hc = data['header'].copy()
            hc.pop('events', None)
            row.update(flatten_json(hc, 'Head'))
        match_facts = data.get('content', {}).get('matchFacts', {})
        if 'infoBox' in match_facts:
            row.update(flatten_json(match_facts['infoBox'], 'Info'))
        weather = data.get('content', {}).get('weather')
        if weather:
            row.update(flatten_json(weather, 'Weather'))
        return [row]
    except Exception:
        return []


def parse_coaches(match_id, season_label, league_name, league_id, data):
    try:
        rows = []
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        lineup_obj = data.get('content', {}).get('lineup', {})
        teams = data.get('header', {}).get('teams', [{}, {}])

        def process(team_key, team_name, team_id):
            coach = lineup_obj.get(team_key, {}).get('coach')
            if isinstance(coach, list) and coach:
                coach = coach[0]
            if isinstance(coach, dict):
                r = base.copy()
                r['Team_ID'] = team_id
                r['Team_Name'] = team_name
                r.update(flatten_json(coach, 'Coach'))
                rows.append(r)

        process('homeTeam', teams[0].get('name'), teams[0].get('id'))
        process('awayTeam', teams[1].get('name'), teams[1].get('id'))
        return rows
    except Exception:
        return []


def parse_lineups(match_id, season_label, league_name, league_id, data):
    try:
        rows = []
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        lineup_obj = data.get('content', {}).get('lineup', {})
        teams = data.get('header', {}).get('teams', [{}, {}])

        def process(team_key, team_name, team_id):
            team_data = lineup_obj.get(team_key, {})
            if not team_data:
                return
            for item in team_data.get('starters', []):
                r = base.copy()
                r.update({'Team_ID': team_id, 'Team_Name': team_name, 'Role': 'Starter'})
                r.update(flatten_json(item, ''))
                rows.append(r)
            for item in team_data.get('subs', team_data.get('bench', [])):
                r = base.copy()
                r.update({'Team_ID': team_id, 'Team_Name': team_name, 'Role': 'Bench'})
                r.update(flatten_json(item, ''))
                rows.append(r)

        process('homeTeam', teams[0].get('name'), teams[0].get('id'))
        process('awayTeam', teams[1].get('name'), teams[1].get('id'))
        return rows
    except Exception:
        return []


def parse_momentum(match_id, season_label, league_name, league_id, data):
    try:
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        momentum = data.get('content', {}).get('matchFacts', {}).get('momentum', {})
        return [dict(base, **flatten_json(p, 'Mom'))
                for p in momentum.get('main', {}).get('data', [])]
    except Exception:
        return []


def parse_match_events(match_id, season_label, league_name, league_id, data):
    try:
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        events = data.get('content', {}).get('matchFacts', {}).get('events', {}).get('events', [])
        return [dict(base, **flatten_json(e, 'Evt')) for e in events]
    except Exception:
        return []


def parse_shotmap(match_id, season_label, league_name, league_id, data):
    try:
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        shots = data.get('content', {}).get('shotmap', {}).get('shots', [])
        return [dict(base, **flatten_json(s, 'Shot')) for s in shots]
    except Exception:
        return []


def parse_player_stats(match_id, season_label, league_name, league_id, data):
    try:
        rows = []
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        root = data.get('content', {}).get('playerStats', {})
        for p_id, p_data in root.items():
            if not isinstance(p_data, dict):
                continue
            row = base.copy()
            for k, v in p_data.items():
                if k == 'stats':
                    continue
                if isinstance(v, dict):
                    for sk, sv in v.items():
                        row[f"Meta_{k}_{sk}"] = sv
                else:
                    row[f"Meta_{k}"] = v
            for group in p_data.get('stats', []):
                title = group.get('title', 'Unk').replace(' ', '_')
                row.update(flatten_json(group.get('stats', {}), title))
            rows.append(row)
        return rows
    except Exception:
        return []


def parse_team_stats(match_id, season_label, league_name, league_id, data):
    try:
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        home_row = dict(base, Team_Side='Home')
        away_row = dict(base, Team_Side='Away')
        periods = data.get('content', {}).get('stats', {}).get('Periods', {})
        for period_name, period_data in periods.items():
            for cat in period_data.get('stats', []):
                cat_title = cat.get('title', 'Unk')
                for item in cat.get('stats', []):
                    stat_name = item.get('title', 'Unk')
                    values = item.get('stats', [])
                    if len(values) >= 2:
                        col = f"Stats_{period_name}_{cat_title}_{stat_name}".replace(' ', '_')
                        h_val, a_val = values[0], values[1]
                        if isinstance(h_val, dict):
                            h_val = h_val.get('value', h_val.get('statValue', str(h_val)))
                        if isinstance(a_val, dict):
                            a_val = a_val.get('value', a_val.get('statValue', str(a_val)))
                        home_row[col] = h_val
                        away_row[col] = a_val
        return [home_row, away_row]
    except Exception:
        return []


def parse_h2h(match_id, season_label, league_name, league_id, data):
    try:
        base = get_base_info(match_id, season_label, league_name, league_id, data)
        h2h = data.get('content', {}).get('h2h', {})
        rows = []
        for m in h2h.get('matches', []):
            r = base.copy()
            r.update(flatten_json(m, 'H2H'))
            rows.append(r)
        return rows
    except Exception:
        return []


# =============================================================================
# PHASE 3 — PLAYER PROFILES
# =============================================================================
def get_player_data(player_id):
    return get_json(f"https://www.fotmob.com/api/data/playerData?id={player_id}",
                     debug_label=f"player {player_id}")


def parse_player_bio(player_id, data):
    row = {'Player_ID': player_id, 'Name': data.get('name'),
           'Contract_End': (data.get('contractEnd') or {}).get('utcTime')}

    for item in data.get('playerInformation', []) or []:
        title = item.get('title')
        value = item.get('value', {})
        if title:
            row[f"Info_{str(title).replace(' ', '_')}"] = (
                value.get('fallback') if isinstance(value, dict) else value)

    pos = data.get('positionDescription', {})
    primary = pos.get('primaryPosition', {})
    row['Primary_Position'] = primary.get('label')
    positions = pos.get('positions', [])
    if positions:
        row['Detailed_Position'] = positions[0].get('strPos', {}).get('label')
        row['Detailed_Position_Short'] = positions[0].get('strPosShort', {}).get('label')

    injury = data.get('injuryInformation')
    if injury:
        row['Injury_Status'] = injury.get('name')
        row['Injury_Expected_Return'] = (injury.get('expectedReturn') or {}).get('expectedReturnFallback')
        row['Injury_Last_Updated'] = (injury.get('lastUpdated') or {}).get('utcTime')

    primary_team = data.get('primaryTeam') or {}
    if isinstance(primary_team, dict):
        row.update(flatten_json(primary_team, 'PrimaryTeam'))

    meta = data.get('meta', {}).get('personJSONLD', {})
    if meta:
        row['Weight_kg'] = (meta.get('weight') or {}).get('value')

    traits = data.get('traits')
    if traits:
        row['Traits'] = str(traits)

    return [row]


def parse_career_history(player_id, data):
    rows = []
    career = (data.get('careerHistory') or {}).get('careerItems', {})

    def process(entries, career_type):
        for entry in entries or []:
            if isinstance(entry, dict):
                r = {'Player_ID': player_id, 'Career_Type': career_type}
                r.update(flatten_json(entry, ''))
                rows.append(r)

    for level in ['senior', 'youth']:
        level_data = career.get(level, {})
        process(level_data.get('teamEntries'), f"{level}_team")
        process(level_data.get('seasonEntries'), f"{level}_season")

    return rows


def parse_trophies(player_id, data):
    rows = []
    trophies = data.get('trophies')
    if not trophies:
        return rows
    for team_entry in trophies if isinstance(trophies, list) else []:
        if not isinstance(team_entry, dict):
            continue
        team_name = team_entry.get('teamName') or team_entry.get('name')
        for trophy in team_entry.get('trophies', []) or []:
            r = {'Player_ID': player_id, 'Team_Name': team_name}
            r.update(flatten_json(trophy, 'Trophy'))
            rows.append(r)
    return rows


def parse_market_values(player_id, data):
    rows = []
    values = data.get('marketValues')
    entries = (values.get('values') if isinstance(values, dict) else values) or []
    for v in entries:
        if isinstance(v, dict):
            r = {'Player_ID': player_id}
            r.update(flatten_json(v, 'MV'))
            rows.append(r)
    return rows


def parse_stat_seasons(player_id, data):
    rows = []
    for season in data.get('statSeasons') or []:
        season_name = season.get('seasonName')
        for t in season.get('tournaments', []) or []:
            r = {'Player_ID': player_id, 'Season': season_name}
            r.update(flatten_json(t, 'Tournament'))
            rows.append(r)
    return rows


# =============================================================================
# PHASE 4 — TEAM PROFILES
# =============================================================================
def get_team_data(team_id):
    return get_json(f"https://www.fotmob.com/api/data/teams?id={team_id}",
                     debug_label=f"team {team_id}")


def parse_team_info(team_id, data):
    row = {'Team_ID': team_id}
    details = data.get('details', {})
    row.update(flatten_json(details, 'Details'))
    return [row]


def parse_team_squad(team_id, data):
    rows = []
    squad = data.get('squad', {}).get('squad', [])
    for group in squad if isinstance(squad, list) else []:
        title = group.get('title', 'unknown')
        for member in group.get('members', []) or []:
            if not isinstance(member, dict):
                continue
            r = {'Team_ID': team_id, 'Position_Group': title}
            r.update(flatten_json(member, ''))
            rows.append(r)
    return rows


def parse_team_coach_history(team_id, data):
    rows = []
    for c in (data.get('history') or {}).get('coachHistory', []) or []:
        r = {'Team_ID': team_id}
        r.update(flatten_json(c, ''))
        rows.append(r)
    return rows


def parse_team_historical_table(team_id, data):
    rows = []
    hist = (data.get('history') or {}).get('historicalTableData', {})
    for division in hist.get('divisions', []) or []:
        for rank in hist.get('ranks', []) or []:
            r = {'Team_ID': team_id}
            r.update(flatten_json(division, 'Division'))
            r.update(flatten_json(rank, 'Rank'))
            rows.append(r)
    return rows


# =============================================================================
# 🚀 SCRAPE FUNCTIONS (one per phase, reusable per-tier)
# =============================================================================
def scrape_matches(leagues_to_scrape, match_folder, current_year, all_player_ids, all_team_ids):
    """Scrapes match data for a given list of (id, name, season_cap) leagues.
    Mutates all_player_ids/all_team_ids in place as matches are found."""
    for i, (l_id, l_name, season_cap) in enumerate(leagues_to_scrape):
        league_path = ensure_folder(os.path.join(match_folder, f"{l_name}_{l_id}"))
        print(f"\n{'=' * 60}\n🏟️  League {i+1}/{len(leagues_to_scrape)}: {l_name} "
              f"({season_cap}yr depth)\n{'=' * 60}")

        processed_matches = get_existing_ids(league_path, "match_info.csv", "Match_ID")
        print(f"   ℹ️  {len(processed_matches)} matches already scraped.")

        start_year_scan = current_year - season_cap
        for year_start in range(start_year_scan, current_year + 1):
            season_options = [f"{year_start}/{year_start + 1}", f"{year_start}"]
            matches, league_data = None, None
            valid_season = ""
            for s_opt in season_options:
                matches, league_data = get_matches_for_season(l_id, s_opt)
                if matches:
                    valid_season = s_opt
                    break
            if not matches:
                continue

            # Save league-level table/transfers once per season (cheap, already fetched)
            save_batch(BASE_OUTPUT_FOLDER, "league_tables",
                       parse_league_table(l_id, l_name, valid_season, league_data))
            save_batch(BASE_OUTPUT_FOLDER, "league_transfers",
                       parse_league_transfers(l_id, l_name, valid_season, league_data))

            new_matches = [m for m in matches if m not in processed_matches]
            if not new_matches:
                print(f"   ⏩ Season {valid_season} fully done.")
                continue

            print(f"   📋 Season {valid_season}: {len(matches)} total matches found, "
                  f"{len(new_matches)} new to scrape.")

            fetch_failed_count = 0
            not_finished_count = 0
            saved_count = 0

            # Fetch concurrently (the slow, network-bound part).
            # Saving/parsing happens sequentially below as results come
            # in, so file writes are never touched by more than one
            # thread at a time.
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_id = {executor.submit(get_match_details, m_id): m_id for m_id in new_matches}
                with tqdm(total=len(new_matches), desc=f"   {valid_season}", unit="match") as pbar:
                    for future in concurrent.futures.as_completed(future_to_id):
                        m_id = future_to_id[future]
                        pbar.update(1)
                        try:
                            details = future.result()
                        except Exception:
                            fetch_failed_count += 1
                            continue
                        if not details:
                            fetch_failed_count += 1
                            continue
                        if not details.get('header', {}).get('status', {}).get('finished'):
                            not_finished_count += 1
                            continue

                        try:
                            save_batch(league_path, "match_info", parse_match_info(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "coaches", parse_coaches(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "lineups", parse_lineups(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "momentum", parse_momentum(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "match_events", parse_match_events(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "shotmaps", parse_shotmap(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "player_stats", parse_player_stats(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "team_stats", parse_team_stats(m_id, valid_season, l_name, l_id, details))
                            save_batch(league_path, "h2h", parse_h2h(m_id, valid_season, l_name, l_id, details))
                            saved_count += 1

                            # NOTE: .get(key, {}) only supplies the default when the
                            # key is MISSING — if the key exists but is JSON null,
                            # .get() returns None anyway. Guard with "or {}"/"or []"
                            # everywhere we immediately call a dict/list method.
                            teams = (details.get('header') or {}).get('teams') or [{}, {}]
                            for t in teams:
                                if isinstance(t, dict) and t.get('id'):
                                    all_team_ids.add(t['id'])
                            player_stats_block = (details.get('content') or {}).get('playerStats') or {}
                            for p_id in player_stats_block.keys():
                                try:
                                    all_player_ids.add(int(p_id))
                                except (ValueError, TypeError):
                                    pass
                        except Exception as e:
                            print(f"\n   ⚠️  Match {m_id}: unexpected error during save/ID-collection "
                                  f"({type(e).__name__}: {e}) — skipped, rest of run continues.")

            print(f"   ✅ Season {valid_season} summary: {saved_count} saved, "
                  f"{not_finished_count} not-yet-finished (skipped), "
                  f"{fetch_failed_count} fetch failures.")
            time.sleep(1)
        print(f"   ✅ Finished {l_name}")


def scrape_players(match_folder, profile_folder, all_player_ids):
    """Profiles every player discovered so far (in-memory set) PLUS anything
    sitting on disk from any previous run — so this is safe to call multiple
    times across tiers without missing or re-doing work."""
    print(f"\n\n{'=' * 60}\n👤 PLAYER PROFILES\n{'=' * 60}")
    print("   Rebuilding full worklist from disk (covers matches scraped in")
    print("   ANY previous run, not just this session's in-memory tracking)...")
    disk_player_ids, disk_team_ids = collect_all_ids_from_disk(match_folder)
    all_player_ids |= disk_player_ids
    print(f"   Total unique players discovered so far: {len(all_player_ids)}")
    already_done = get_existing_ids(profile_folder, "player_bio.csv", "Player_ID")
    remaining_players = [p for p in all_player_ids if p not in already_done]
    print(f"   {len(already_done)} already done, {len(remaining_players)} remaining.")

    if not remaining_players:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(get_player_data, p_id): p_id for p_id in remaining_players}
        with tqdm(total=len(remaining_players), desc="   Players", unit="player") as pbar:
            for future in concurrent.futures.as_completed(future_to_id):
                p_id = future_to_id[future]
                pbar.update(1)
                try:
                    data = future.result()
                except Exception:
                    continue
                if not data:
                    continue
                try:
                    save_batch(profile_folder, "player_bio", parse_player_bio(p_id, data))
                    save_batch(profile_folder, "player_career_history", parse_career_history(p_id, data))
                    save_batch(profile_folder, "player_trophies", parse_trophies(p_id, data))
                    save_batch(profile_folder, "player_market_value_history", parse_market_values(p_id, data))
                    save_batch(profile_folder, "player_stat_seasons", parse_stat_seasons(p_id, data))
                except Exception as e:
                    print(f"\n   ⚠️  Player {p_id}: unexpected error ({type(e).__name__}: {e}) — skipped.")


def scrape_teams(match_folder, team_folder, all_team_ids):
    """Same pattern as scrape_players — disk-rebuilt worklist, safe to call
    repeatedly across tiers."""
    print(f"\n\n{'=' * 60}\n🏟️  TEAM PROFILES\n{'=' * 60}")
    disk_player_ids, disk_team_ids = collect_all_ids_from_disk(match_folder)
    all_team_ids |= disk_team_ids
    print(f"   Total unique teams discovered so far: {len(all_team_ids)}")
    already_done_teams = get_existing_ids(team_folder, "team_info.csv", "Team_ID")
    remaining_teams = [t for t in all_team_ids if t not in already_done_teams]
    print(f"   {len(already_done_teams)} already done, {len(remaining_teams)} remaining.")

    if not remaining_teams:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(get_team_data, t_id): t_id for t_id in remaining_teams}
        with tqdm(total=len(remaining_teams), desc="   Teams", unit="team") as pbar:
            for future in concurrent.futures.as_completed(future_to_id):
                t_id = future_to_id[future]
                pbar.update(1)
                try:
                    data = future.result()
                except Exception:
                    continue
                if not data:
                    continue
                try:
                    save_batch(team_folder, "team_info", parse_team_info(t_id, data))
                    save_batch(team_folder, "team_squad", parse_team_squad(t_id, data))
                    save_batch(team_folder, "team_coach_history", parse_team_coach_history(t_id, data))
                    save_batch(team_folder, "team_historical_table", parse_team_historical_table(t_id, data))
                except Exception as e:
                    print(f"\n   ⚠️  Team {t_id}: unexpected error ({type(e).__name__}: {e}) — skipped.")


# =============================================================================
# 🚀 MAIN EXECUTION
# =============================================================================
def main():
    ensure_folder(BASE_OUTPUT_FOLDER)
    match_folder = ensure_folder(os.path.join(BASE_OUTPUT_FOLDER, "match_data"))
    profile_folder = ensure_folder(os.path.join(BASE_OUTPUT_FOLDER, "profile_data"))
    team_folder = ensure_folder(os.path.join(BASE_OUTPUT_FOLDER, "team_data"))

    current_year = datetime.now().year

    all_player_ids = set()
    all_team_ids = set()

    try:
        league_list = build_league_list()

        # Group leagues by tier (Tier0/Tier1/Tier2/Tier3), preserving the
        # order build_league_list() already produced within each tier.
        tier_order = ["Tier0", "Tier1", "Tier2", "Tier3"]
        leagues_by_tier = {t: [] for t in tier_order}
        for entry in league_list:
            _, l_name, _ = entry
            tier = l_name.split('_')[0]
            if tier in leagues_by_tier:
                leagues_by_tier[tier].append(entry)
            else:
                leagues_by_tier.setdefault(tier, []).append(entry)

        # Process EACH TIER completely (matches -> players -> teams) before
        # moving to the next tier — so an interrupted run has already banked
        # full, useful data (not just match rows with no player/team profiles
        # to go with them) for the most important competitions first.
        for tier in tier_order:
            tier_leagues = leagues_by_tier.get(tier, [])
            if not tier_leagues:
                continue

            # Quick visibility check: how many of this tier's leagues already
            # have SOME data on disk, before diving into the real work.
            already_have_data = sum(
                1 for (lid, lname, _) in tier_leagues
                if os.path.exists(os.path.join(match_folder, f"{lname}_{lid}", "match_info.csv"))
            )
            print(f"\n\n{'#' * 70}\n### {tier}: {len(tier_leagues)} leagues "
                  f"({already_have_data} already have some data on disk) ###\n{'#' * 70}")

            if tier in SKIP_MATCH_PHASE_TIERS:
                print(f"   ⏭️  Skipping match-scraping for {tier} (configured to skip) — "
                      f"going straight to player/team profiling using existing data.")
            else:
                scrape_matches(tier_leagues, match_folder, current_year, all_player_ids, all_team_ids)
            scrape_players(match_folder, profile_folder, all_player_ids)
            scrape_teams(match_folder, team_folder, all_team_ids)

        print("\n\n✅ ALL TIERS COMPLETE.")

    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user. Everything saved so far is safe — rerun anytime to resume.")


if __name__ == "__main__":
    main()