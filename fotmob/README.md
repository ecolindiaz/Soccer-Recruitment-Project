# FotMob Data Scraper

A self-contained scraper that pulls a broad slice of FotMob's public data —
match details, lineups, events, shotmaps, per-player and per-team match stats,
league tables, transfers, and full player/team profiles — and writes everything
to CSV.

It builds its own competition list at runtime from FotMob's `allLeagues`
endpoint (every competition across ~94 countries), orders that list into
priority tiers, and scrapes tier-by-tier so an interrupted run has always
banked the highest-value data first. The whole thing is **resumable**: rerun it
anytime and it skips whatever is already on disk.

## Setup

```bash
cd fotmob
python3 -m venv .venv && source .venv/bin/activate   # optional but recommended
pip install -r requirements.txt
```

## Usage

```bash
python fotmob_scraper.py
```

That's it. The run is long — it walks the full worldwide competition list — but
it's safe to stop with `Ctrl+C` at any point and rerun later to pick up where it
left off. Everything already written stays valid.

## Output

All data lands under `fotmob_data/` (git-ignored — regenerate it by running the
scraper, don't commit it):

```
fotmob_data/
├── league_tables.csv          # standings per league/season/split
├── league_transfers.csv       # transfers per league/season
├── match_data/
│   └── <Tier>_<League>_<id>/   # one folder per competition
│       ├── match_info.csv
│       ├── coaches.csv
│       ├── lineups.csv
│       ├── momentum.csv
│       ├── match_events.csv
│       ├── shotmaps.csv
│       ├── player_stats.csv
│       ├── team_stats.csv
│       └── h2h.csv
├── profile_data/
│   ├── player_bio.csv
│   ├── player_career_history.csv
│   ├── player_trophies.csv
│   ├── player_market_value_history.csv
│   └── player_stat_seasons.csv
└── team_data/
    ├── team_info.csv
    ├── team_squad.csv
    ├── team_coach_history.csv
    └── team_historical_table.csv
```

## How the scrape is prioritized

Competitions are grouped into four tiers, and each tier is fully processed
(matches → player profiles → team profiles) before the next one starts:

| Tier  | Scope                                                       | Season depth |
|-------|-------------------------------------------------------------|--------------|
| Tier 0| FotMob "popular" + international + continental/domestic cups | 12 seasons   |
| Tier 1| Full domestic pyramid for the major nations (big-5 + NED/POR)| 8 seasons   |
| Tier 2| The real top flight of every other country                  | 6 seasons    |
| Tier 3| Everything else (2nd tiers, cups, lower divisions)          | 3 seasons    |

## Configuration

The knobs live at the top of `fotmob_scraper.py`:

- **`MAX_WORKERS`** (default `8`) — concurrent requests. Drop to 3–4 if you see
  `429`s or a rising error rate; the scraper already backs off globally on
  sustained rate-limiting, but a lower worker count is the first lever to pull.
- **`SEASON_CAP_TIER0..3`** — how many seasons of history to pull per tier.
- **`MAJOR_CCODES`** — countries that get their entire domestic structure (Tier 1)
  rather than just a top flight.
- **`SKIP_MATCH_PHASE_TIERS`** — set of tier names (e.g. `{"Tier0", "Tier1"}`) to
  skip match-scraping for, going straight to player/team profiling from data
  already on disk. Default is empty (scrape everything).

## Notes

- Data comes from FotMob's public/undocumented JSON endpoints; field shapes can
  change without notice. The parsers guard against missing/null keys and skip
  individual bad records rather than aborting the run.
- Fixture discovery deliberately uses the HTML `__NEXT_DATA__` league page
  because the `/api/data/leagues` JSON endpoint ignores the requested season and
  always returns the current one. Match/player/team detail still use the fast
  direct-JSON endpoints.
- Be considerate with request volume. Keep `MAX_WORKERS` reasonable.
