# Betting research (prematch soccer → Kalshi)

Research pipeline for a **prematch** soccer trading model aimed at Kalshi,
focused on **smaller leagues** where the market is softer. Strategy in one line:
build our own probability for a match outcome, and only trade where it beats the
**closing line** (the sharpest available price).

> Status: early. This directory currently holds the **league-overlap mapper**.
> The backtest harness (walk-forward, CLV vs. Pinnacle close) comes next.

## The three data worlds

A league is only workable if it exists in **all three**:

1. **football-data.co.uk** — historical results + **closing odds** (incl.
   Pinnacle). The load-bearing source: the only one giving a historical *market
   price to beat*. This is our backtest benchmark.
2. **FotMob** — xG, shots, lineups, form (the signal features), via the scraper
   in [`../fotmob`](../fotmob).
3. **Kalshi** — where the prematch trade is actually placed. Coverage here is
   the open question, so it's supplied manually from a tradability audit.

## Step 1 — `league_overlap.py`

Computes the intersection and emits the shortlist, with the IDs each downstream
step needs (football-data code for odds, FotMob league id for features).

```bash
pip install -r ../fotmob/requirements.txt      # requests (+ pandas, optional)

# See the modelable universe (football-data ∩ FotMob) — no Kalshi list yet:
python league_overlap.py --out overlap.csv

# After the Kalshi tradability audit, list tradable leagues one per line
# (copy the template), then get the truly TRADEABLE shortlist:
cp kalshi_leagues.example.txt kalshi_leagues.txt   # then edit it
python league_overlap.py --kalshi kalshi_leagues.txt --out overlap.csv
```

Output: a ranked `overlap.csv` (shortlist first, then near-misses to eyeball)
plus a printed summary. Fuzzy matches carry a score — **skim the near-misses**,
since league names differ across sources and the matcher is deliberately
conservative.

**Needs live network to `fotmob.com`** for the league list. If it's blocked in
your environment, run it locally.

## History depth

Downstream backtests use **5 seasons** (`SEASONS_BACK` in `league_overlap.py`)
— deep enough to validate, shallow enough that small-league coverage stays
usable.

## Next up (not built yet)

- **Odds benchmark loader** — football-data closing odds → de-vigged implied
  probabilities.
- **Point-in-time feature store** — FotMob-derived features stamped with when
  we'd have known them (no look-ahead).
- **Walk-forward evaluator** — scores any model by log-loss/Brier and **CLV vs.
  Pinnacle close**, with a locked holdout and multiple-comparisons discipline.
- **Baseline model**, then the creative metrics (rolling xG rating, rest,
  motivation, …).
