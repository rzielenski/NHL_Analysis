from NHL_Odds_Retriever import get_odds
from NHL_Win_Prediction import get_WP
import time
from datetime import datetime, timedelta
import requests
import json
import os
import tensorflow as tf

tf.keras.config.enable_unsafe_deserialization()

PROFIT_FILE = "profit.json"
total_profit = 0.0
scheduled_jobs = []  # (run_time, function, args)

def load_profit():
    if os.path.exists(PROFIT_FILE):
        with open(PROFIT_FILE, "r") as f:
            return json.load(f).get("total_profit", 0.0)
    return 0.0

def save_profit(profit):
    with open(PROFIT_FILE, "w") as f:
        json.dump({"total_profit": profit}, f)

def get_EV(wager, gameId):
    print(f"[{datetime.utcnow()}] Running get_EV for game {gameId}")
    try:
        odds = get_odds(gameId)
        print(odds)
    except Exception as e:
        print(f"[ERROR] Odds failed: {e}")
        return

    homeTeam, homeOdds = odds[1], odds[2]
    awayTeam, awayOdds = odds[3], odds[4]

    try:
        home_WP = get_WP(homeTeam, awayTeam, gameId)
    except Exception as e:
        print(f"[ERROR] WP failed: {e}")
        return

    print(f"{homeTeam} WP: {home_WP:.2f}")
    away_WP = 1 - home_WP

    if homeOdds > 0:
        home_payout = wager * (homeOdds / 100)
    else:
        home_payout = wager / (abs(homeOdds) / 100)

    if awayOdds > 0:
        away_payout = wager * (awayOdds / 100)
    else:
        away_payout = wager / (abs(awayOdds) / 100)

    home_EV = (home_WP * home_payout) - ((1 - home_WP) * wager)
    away_EV = (away_WP * away_payout) - ((1 - away_WP) * wager)

    best_EV = max(home_EV, away_EV)
    team = homeTeam if home_EV >= away_EV else awayTeam
    bestOdds = homeOdds if home_EV >= away_EV else awayOdds

    if best_EV > 0:
        print(f"[{datetime.utcnow()}] +EV found: Bet on {team} with EV = {best_EV:.2f}")
        profit_time = datetime.utcnow() + timedelta(hours=4)
        schedule_job(profit_time, get_profits, (wager, bestOdds, gameId, team))
    else:
        print(f"[{datetime.utcnow()}] No positive EV for game {gameId}")

def get_profits(wager, odds, gameId, teamName):
    global total_profit
    game_url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/boxscore"

    res = requests.get(game_url)
    game_data = res.json()

    if game_data["homeTeam"]["score"] > game_data["awayTeam"]["score"]:
        home_win = True
    else:
        home_win = False

    if game_data["homeTeam"]["abbrev"] == teamName:
        if home_win:
            if odds > 0:
                mult = (odds + 100) / 100
                total_profit += ((mult * wager) - wager)
            else:
                mult = 1 + (100 / abs(odds))
                total_profit += ((mult * wager) - wager)
        else:
            total_profit -= wager
    else:
        if home_win:
            total_profit -= wager
        else:
            if odds > 0:
                mult = (odds + 100) / 100
                total_profit += ((mult * wager) - wager)
            else:
                mult = 1 + (100 / abs(odds))
                total_profit += ((mult * wager) - wager)
    
    save_profit(total_profit)
    print(f"[{datetime.utcnow()}] Game {gameId} resolved. Updated total profit: ${total_profit:.2f}")

def schedule_job(run_time, func, args):
    scheduled_jobs.append((run_time, func, args))
    print(f"[{datetime.utcnow()}] Scheduled job at {run_time} UTC: {func.__name__}({args})")

def get_game_times():
    url = "https://api-web.nhle.com/v1/partner-game/US/now"
    try:
        res = requests.get(url)
        data = res.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch games: {e}")
        return

    now = datetime.utcnow()
    for game in data.get("games", []):
        game_id = game["gameId"]
        start_time = datetime.strptime(game["startTimeUTC"], "%Y-%m-%dT%H:%M:%SZ")
        if start_time <= now:
            print(f"[{datetime.utcnow()}] Skipping game {game_id}, already started.")
            continue

        ev_time = start_time - timedelta(minutes=30)
        if ev_time > now:
            schedule_job(ev_time, get_EV, (25, game_id))

def schedule_daily_game_fetch():
    now = datetime.utcnow()
    next_run = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)

    schedule_job(next_run, get_game_times, ())
    print(f"[{datetime.utcnow()}] Scheduled daily get_game_times for {next_run} UTC")

# === Main ===
total_profit = load_profit()
print(f"[INIT] Loaded profit: ${total_profit:.2f}")

get_game_times()

schedule_daily_game_fetch()

try:
    while True:
        now = datetime.utcnow()
        for job in scheduled_jobs[:]:
            run_time, func, args = job
            if now >= run_time:
                func(*args)
                scheduled_jobs.remove(job)

                if func == get_game_times:
                    schedule_daily_game_fetch()

        time.sleep(1)
except KeyboardInterrupt:
    save_profit(total_profit)
    print(f"\n[EXIT] Final profit: ${total_profit:.2f}")
