import requests
import csv
import datetime
import time
import pandas as pd

def time_to_float(time_str):
    minutes, seconds = map(int, time_str.split(":"))
    return minutes + seconds / 60

def average_ice_time(times):
    total_seconds = sum(time_to_float(t) * 60 for t in times)
    avg_seconds = total_seconds /  max(1, len(times))  
    return avg_seconds / 60  

def get_all_data(year, year2):
    game_count = 1
    processed_players = set()
    
    team_codes = {
        1 : "NJD", 2 : "NYI", 3 : "NYR",
        4 : "PHI", 5 : "PIT", 6 : "BOS",
        7 : "BUF", 8 : "MTL", 9 : "OTT",
        10 : "TOR", 12 : "CAR", 13 : "FLA",
        14 : "TBL", 15 : "WSH", 16 : "CHI",
        17 : "DET", 18 : "NSH", 19 : "STL",
        20 : "CGY", 21 : "COL", 22 : "EDM",
        23 : "VAN", 24 : "ANA", 25 : "DAL",
        26 : "LAK", 27 : "PHX", 28 : "SJS",
        29 : "CBJ", 30 : "MIN", 52 : "WPG", 
        53 : "ARI", 54 : "VGK", 55 : "SEA", 
        59 : "UTA"

    }

    team_data = pd.DataFrame(columns=["team", "season", "gamesPlayed", "wins", "losses", "xGoalsFor", "xReboundsFor", "xShotAttemptsPercentage", "shotsOnGoalFor", "missedShotsFor",
                      "blockedShotAttemptsFor", "shotAttemptsFor", "goalsFor",	"reboundsFor", "penaltiesFor", "penaltyMinutesFor", "faceOffsWonFor", "faceOffsLostFor", "hitsFor", 
                      "takeaways", "giveaways",	"xGoalsAgainst", "xReboundsAgainst", "shotsOnGoalAgainst", "missedShotsAgainst", "blockedShotAttemptsAgainst", "shotAttemptsAgainst", 
                      "goalsAgainst", "reboundsAgainst", "penaltiesAgainst", "penaltyMinutesAgainst", "hitsAgainst"])
    
    player_data = pd.DataFrame(columns=["playerId", "season", "name", "team", "position", "gamesPlayed", "xIcetime", "xShifts", "xGoals", "xRebounds", "primaryAssists", 
                                        "secondaryAssists", "shotsOnGoal", "xShotsOnGoal", "missedShots", "blockedShotAttempts", "shotAttempts", "points", "goals", "rebounds", 
                                        "penalties", "penaltyMinutes", "faceOffsWon", "hits", "takeaways", "giveaways", "faceOffsLost", 
                                        "penaltyMinutesDrawn", "penaltiesDrawn", "shotsBlockedByPlayer"])

    goalie_data = pd.DataFrame(columns=["playerId", "season", "name", "team", "gamesPlayed", "savePercentage", "GAA", "goalsAgainst", "saves", "shotsFaced", "rebounds", 
                                        "reboundPercentage"]) 

    season = f"{year}_{year2}"
    
    teams_data_file = f"{season}_All_Teams_Data_TEST.csv"
    players_data_file = f"{season}_All_Players_Data_TEST.csv"
    goalies_data_file = f"{season}_All_Goalies_Data_TEST.csv"

    processed_players = set()

    while True:
        if game_count % 100 == 0:
            print(f"Processed {game_count} games so far...")
            print(team_data.head(3))

        game_id = f"{year}02{game_count:04}"
        url = f'https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play'
        
        res = requests.get(url)
        if res.status_code == 404:
            print("Error 404: Not Found")
            break

        data = res.json()
    
        away = data.get('awayTeam', [])
        home = data.get('homeTeam', [])

        # --------------------------- BASIC TEAM DATA ---------------------------------------

        home_win = True
        if away["score"] > home["score"]:
            home_win = False
        
        # ------- AWAY TEAM -----------
        # Add away team to data if not already there
        if away["abbrev"] not in team_data["team"].values:
            new_row = pd.DataFrame([{
                "team": away["abbrev"],
                "season": year,
                "gamesPlayed": 1,
                "wins": int(not home_win),
                "losses": int(home_win),
                "xGoalsFor": away["score"],
                "shotsOnGoalFor": away["sog"],
                "goalsFor": away["score"],
                "xGoalsAgainst": home["score"],
                "shotsOnGoalAgainst": home["sog"],
                "goalsAgainst": home["score"]
            }])
            team_data = pd.concat([team_data, new_row], ignore_index=True)
        else:
            index = team_data.index[team_data["team"] == away["abbrev"]].tolist()
            if len(index) > 0:
                index = index[0]
                team_data.loc[index, "gamesPlayed"] += 1
                if not home_win:
                    team_data.loc[index, "wins"] += 1
                else:
                    team_data.loc[index, "losses"] += 1
                
                if away.get("sog") is not None:
                    team_data.loc[index, "xGoalsFor"] = (int(team_data.loc[index, "goalsFor"]) + away["score"]) / team_data.loc[index, "gamesPlayed"]
                    team_data.loc[index, "shotsOnGoalFor"] += away["sog"]
                    team_data.loc[index, "goalsFor"] += away["score"]

                if home.get("sog") is not None:
                    team_data.loc[index, "xGoalsAgainst"] = (int(team_data.loc[index, "goalsAgainst"]) + home["score"]) / team_data.loc[index, "gamesPlayed"]
                    team_data.loc[index, "shotsOnGoalAgainst"] += home["sog"]
                    team_data.loc[index, "goalsAgainst"] += home["score"]


        # ------- HOME TEAM -----------
        if home["abbrev"] not in team_data["team"].values:
            new_row = pd.DataFrame([{
                "team": home["abbrev"],
                "season": year,
                "gamesPlayed": 1,
                "wins": int(home_win),
                "losses": int(not home_win),
                "xGoalsFor": home["score"],
                "shotsOnGoalFor": home["sog"],
                "goalsFor": home["score"],
                "xGoalsAgainst": away["score"],
                "shotsOnGoalAgainst": away["sog"],
                "goalsAgainst": away["score"]
            }])

            team_data = pd.concat([team_data, new_row], ignore_index=True)
        else:
            index = team_data.index[team_data["team"] == home["abbrev"]]
            
            if len(index) > 0:
                index = index[0]
                team_data.loc[index, "gamesPlayed"] += 1
                if not home_win:
                    team_data.loc[index, "wins"] += 1
                else:
                    team_data.loc[index, "losses"] += 1
                if home.get("sog") is not None:
                    team_data.loc[index, "xGoalsFor"] = (int(team_data.loc[index, "goalsFor"]) + home["score"]) / team_data.loc[index, "gamesPlayed"]
                    team_data.loc[index, "shotsOnGoalFor"] += home["sog"]
                    team_data.loc[index, "goalsFor"] += home["score"]

                if away.get("sog") is not None:
                    team_data.loc[index, "xGoalsAgainst"] = (int(team_data.loc[index, "goalsAgainst"]) + away["score"]) / team_data.loc[index, "gamesPlayed"]
                    team_data.loc[index, "shotsOnGoalAgainst"] += away["sog"]
                    team_data.loc[index, "goalsAgainst"] += away["score"]
        

        game_key = game_id

        for player in data.get("rosterSpots", []):
            player_id = player.get("playerId")
            team_id = player.get("teamId")
            team_abbrev = team_codes.get(team_id, "UNK")
            pos_code = player.get("positionCode")
            
            # Create a unique player-game key
            player_game_key = f"{player_id}_{game_key}"
            
            # Skip if already processed for this game
            if player_game_key in processed_players:
                continue
    
            processed_players.add(player_game_key)
 
            # Get player name
            first_name = player.get("firstName", {}).get("default", "")
            last_name = player.get("lastName", {}).get("default", "")
            full_name = f"{first_name}{last_name}"
            
            # Process skaters and goalies separately
            if pos_code != "G":
                if player_id not in player_data["playerId"].values:
                    # Add new player
                    new_player = pd.DataFrame([{
                        "playerId": player_id,
                        "season": year,
                        "name": full_name,
                        "team": team_abbrev,
                        "position": pos_code,
                        "gamesPlayed" : 1
                    }])
                    player_data = pd.concat([player_data, new_player], ignore_index=True)
                else:
                    # Update existing player
                    p_idx = player_data.index[player_data["playerId"] == player_id].tolist()[0]
                    # Update team if needed (for traded players)
                    player_data.at[p_idx, "team"] = team_abbrev
                    player_data.at[p_idx, "gamesPlayed"] += 1
            else:
                # Process goalie
                if player_id not in goalie_data["playerId"].values:
                    # Add new goalie
                    new_goalie = pd.DataFrame([{
                        "playerId": player_id,
                        "season": year,
                        "name": full_name,
                        "team": team_abbrev,
                    }])
                    goalie_data = pd.concat([goalie_data, new_goalie], ignore_index=True)
                else:
                    # Update existing goalie
                    g_idx = goalie_data.index[goalie_data["playerId"] == player_id].tolist()[0]
                    # Update team if needed (for traded goalies)
                    goalie_data.at[g_idx, "team"] = team_abbrev
            
        team_data.fillna(0, inplace=True)    
        player_data.fillna(0, inplace=True)
        goalie_data.fillna(0, inplace=True)

        
        shot_count = 0
        for event in data.get("plays", []):
            match event["typeCode"]:
                # FACEOFF EVENT
                case 502:    
                    if event["details"]["winningPlayerId"] is not None:
                        w_index = player_data.index[player_data["playerId"] == event["details"]["winningPlayerId"]]
                        if len(w_index) > 0:
                            w_index = w_index[0]
                         
                        player_data.loc[w_index, "faceOffsWon"] += 1
                        t_index = team_data.index[team_data["team"] == player_data.loc[w_index, "team"]]
                        if len(t_index) > 0:
                            t_index = t_index[0]
                            team_data.loc[t_index, "faceOffsWonFor"] += 1
                        
                    if event["details"]["losingPlayerId"] is not None:
                        l_index = player_data.index[player_data["playerId"] == event["details"]["losingPlayerId"]]
                        if len(l_index) > 0:
                            l_index = l_index[0]
                            player_data.loc[l_index, "faceOffsLost"] += 1
                            t_index = team_data.index[team_data["team"] == player_data.loc[l_index, "team"]]
                            team_data.loc[t_index, "faceOffsLostFor"] += 1

                # HIT EVENT 
                case 503:
                    if event["details"]["hittingPlayerId"] is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["hittingPlayerId"]]
                        if len(index) > 0:
                            index = index[0]
                            player_data.loc[index, "hits"] += 1
                            t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                            if len(t_index) > 0:
                                t_index = t_index[0]
                                team_data.loc[t_index, "hitsFor"] += 1

                    if event["details"]["hitteePlayerId"] is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["hitteePlayerId"]]
                        if len(index) > 0:
                            index = index[0]
                            t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                            if len(t_index) > 0:
                                t_index = t_index[0]
                                team_data.loc[t_index, "hitsAgainst"] += 1

                # GIVEAWAY EVENT
                case 504:
                    if event["details"]["playerId"] is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["playerId"]]
                        if len(index) > 0:
                            index = index[0]
                            player_data.loc[index, "giveaways"] += 1
                            t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                            if len(t_index) > 0:
                                t_index = t_index[0]
                                team_data.loc[t_index, "giveaways"] += 1

                # EVENT GOAL!!! 
                case 505:              
                   goalie_id = event["details"].get("goalieInNetId")
                   if goalie_id is not None:
                        ga_index = goalie_data.index[goalie_data["playerId"] == goalie_id]
                        if len(ga_index) > 0:
                            ga_index = ga_index[0]
                            goalie_data.loc[ga_index, "goalsAgainst"] += 1
                            goalie_data.loc[ga_index, "shotsFaced"] += 1
                            goalie_data.loc[ga_index, "GAA"] = goalie_data.loc[ga_index, "goalsAgainst"] / (max(1, goalie_data.loc[ga_index, "gamesPlayed"]))
                            goalie_data.loc[ga_index, "savePercentage"] = goalie_data.loc[ga_index, "goalsAgainst"] / goalie_data.loc[ga_index, "shotsFaced"]

                   if event["details"].get("assist1PlayerId") is not None:  
                        pa_index = player_data.index[player_data["playerId"] == event["details"]["assist1PlayerId"]] 
                        if len(pa_index) > 0:
                            pa_index = pa_index[0]
                            player_data.loc[pa_index, "primaryAssists"] += 1
                            player_data.loc[pa_index, "points"] += 1
                   
                   if event["details"].get("assist2PlayerId") is not None:
                        sa_index = player_data.index[player_data["playerId"] == event["details"]["assist2PlayerId"]] 
                        if len(sa_index) > 0:
                            sa_index = sa_index[0]
                            player_data.loc[sa_index, "secondaryAssists"] += 1
                            player_data.loc[sa_index, "points"] += 1

                   if event["details"].get("scoringPlayerId") is not None:
                        gs_index = player_data.index[player_data["playerId"] == event["details"]["scoringPlayerId"]] 
                        if len(gs_index) > 0:
                            gs_index = gs_index[0]
                            player_data.loc[gs_index, "goals"] += 1
                            player_data.loc[gs_index, "xGoals"] = player_data.loc[gs_index, "goals"] / player_data.loc[gs_index, "gamesPlayed"]

                # EVENT SHOT ON GOAL
                case 506:
                   shooting_player_id = event["details"].get("shootingPlayerId")
                   goalie_id = event["details"].get("goalieInNetId")
                   if shooting_player_id is not None:
                    index = player_data.index[player_data["playerId"] == shooting_player_id]
                    if len(index) > 0:
                        index = index[0]
                        player_data.loc[index, "shotsOnGoal"] += 1
                        player_data.loc[index, "xShotsOnGoal"] = player_data.loc[index, "shotsOnGoal"] / player_data.loc[index, "gamesPlayed"]
                        player_data.loc[index, "shotAttempts"] += 1
                        player_data.loc[index, "rebounds"] += 1
                        player_data.loc[index, "xRebounds"] = player_data.loc[index, "rebounds"] / player_data.loc[index, "gamesPlayed"]

                        t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                        if len(t_index) > 0:
                            t_index = t_index[0]
                            team_data.loc[t_index, "shotsOnGoalFor"] += 1
                            team_data.loc[t_index, "shotAttemptsFor"] += 1
                            team_data.loc[t_index, "xShotAttemptsPercentage"] = team_data.loc[t_index, "shotAttemptsFor"] / (team_data.loc[t_index, "shotAttemptsAgainst"] + team_data.loc[t_index, "shotAttemptsFor"])
                            team_data.loc[t_index, "reboundsFor"] += 1
                            team_data.loc[t_index, "xReboundsFor"] = team_data.loc[t_index, "reboundsFor"] / team_data.loc[t_index, "gamesPlayed"]

                   if goalie_id is not None:
                    ga_index = goalie_data.index[goalie_data["playerId"] == goalie_id]
                    if len(ga_index) > 0:
                        ga_index = ga_index[0]
                        if shot_count == 0:
                            goalie_data.loc[ga_index, "gamesPlayed"] += 1
                            shot_count += 1
                    
                        goalie_data.loc[ga_index, "saves"] += 1
                        goalie_data.loc[ga_index, "shotsFaced"] += 1
                        goalie_data.loc[ga_index, "savePercentage"] = 1 - (goalie_data.loc[ga_index, "goalsAgainst"] / goalie_data.loc[ga_index, "shotsFaced"])
                        goalie_data.loc[ga_index, "rebounds"] += 1
                        goalie_data.loc[ga_index, "reboundPercentage"] = goalie_data.loc[ga_index, "rebounds"] / goalie_data.loc[ga_index, "shotsFaced"]

                        gt_index = team_data.index[team_data["team"] == goalie_data.loc[ga_index, "team"]]
                        if len(gt_index) > 0:
                            gt_index = gt_index[0]
                            team_data.loc[gt_index, "shotsOnGoalAgainst"] += 1
                            team_data.loc[gt_index, "shotAttemptsAgainst"] += 1
                            team_data.loc[gt_index, "xShotAttemptsPercentage"] = team_data.loc[gt_index, "shotAttemptsFor"] / (team_data.loc[gt_index, "shotAttemptsAgainst"] + team_data.loc[gt_index, "shotAttemptsFor"])
                            team_data.loc[gt_index, "reboundsAgainst"] += 1
                            team_data.loc[gt_index, "xReboundsAgainst"] = team_data.loc[gt_index, "reboundsAgainst"] / team_data.loc[gt_index, "gamesPlayed"]

                   lastSave = [index, ga_index, t_index, gt_index]


                # EVENT MISSED SHOT
                case 507:
                   shooting_player_id = event["details"].get("shootingPlayerId")
                   goalie_id = event["details"].get("goalieInNetId")
                   if shooting_player_id is not None:
                    index = player_data.index[player_data["playerId"] == shooting_player_id]
                    if len(index) > 0:
                        index = index[0]
                        player_data.loc[index, "missedShots"] += 1
                        player_data.loc[index, "shotAttempts"] += 1

                        t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                        if len(t_index) > 0:
                            t_index = t_index[0]
                            team_data.loc[t_index, "missedShotsFor"] += 1
                            team_data.loc[t_index, "shotAttemptsFor"] += 1
                            team_data.loc[t_index, "xShotAttemptsPercentage"] = team_data.loc[t_index, "shotAttemptsFor"] / (team_data.loc[t_index, "shotAttemptsAgainst"] + team_data.loc[t_index, "shotAttemptsFor"])

                   if goalie_id is not None:
                    ga_index = goalie_data.index[goalie_data["playerId"] == goalie_id]
                    if len(ga_index) > 0:
                        ga_index = ga_index[0]
                        gt_index = team_data.index[team_data["team"] == goalie_data.loc[ga_index, "team"]]
                        if len(gt_index) > 0:
                            gt_index = gt_index[0]
                            team_data.loc[gt_index, "missedShotsAgainst"] += 1
                            team_data.loc[gt_index, "shotAttemptsAgainst"] += 1
                            team_data.loc[gt_index, "xShotAttemptsPercentage"] = team_data.loc[gt_index, "shotAttemptsFor"] / (team_data.loc[gt_index, "shotAttemptsAgainst"] + team_data.loc[gt_index, "shotAttemptsFor"])


                # EVENT BLOCKED SHOT
                case 508:
                   shooting_player_id = event["details"].get("shootingPlayerId")
                   blocking_player_id = event["details"].get("blockingPlayerId")
                   if shooting_player_id is not None:
                    index = player_data.index[player_data["playerId"] == shooting_player_id]
                    if len(index) > 0:
                        index = index[0]
                        player_data.loc[index, "blockedShotAttempts"] += 1
                        player_data.loc[index, "shotAttempts"] += 1

                        t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                        if len(t_index) > 0:
                            t_index = t_index[0]
                            team_data.loc[t_index, "blockedShotAttemptsFor"] += 1
                            team_data.loc[t_index, "shotAttemptsFor"] += 1
                            team_data.loc[t_index, "xShotAttemptsPercentage"] = team_data.loc[t_index, "shotAttemptsFor"] / (team_data.loc[t_index, "shotAttemptsAgainst"] + team_data.loc[t_index, "shotAttemptsFor"])

                    if blocking_player_id is not None:
                        ga_index = player_data.index[player_data["playerId"] == blocking_player_id]
                        if len(ga_index) > 0:
                            ga_index = ga_index[0]
                            gt_index = team_data.index[team_data["team"] == player_data.loc[ga_index, "team"]]
                            if len(gt_index) > 0:
                                gt_index = gt_index[0]
                                team_data.loc[gt_index, "blockedShotAttemptsAgainst"] += 1
                                team_data.loc[gt_index, "shotAttemptsAgainst"] += 1
                                team_data.loc[gt_index, "xShotAttemptsPercentage"] = team_data.loc[gt_index, "shotAttemptsFor"] / (team_data.loc[gt_index, "shotAttemptsAgainst"] + team_data.loc[gt_index, "shotAttemptsFor"])


                case 509:
                    if event["details"].get("committedByPlayerId") is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["committedByPlayerId"]]
                        if len(index) > 0:
                            index = index[0]
                            player_data.loc[index, "penalties"] += 1
                            if event["details"]["duration"] is not None:
                                player_data.loc[index, "penaltyMinutes"] += event["details"]["duration"]

                                t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                                if len(t_index) > 0:
                                    t_index = t_index[0]
                                    team_data.loc[t_index, "penaltiesFor"] += 1
                                    team_data.loc[t_index, "penaltyMinutesFor"] += event["details"]["duration"]

                    if event["details"].get("drawnByPlayerId") is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["drawnByPlayerId"]]
                        if len(index) > 0:
                            index = index[0]
                            player_data.loc[index, "penaltiesDrawn"] += 1
                            if event["details"]["duration"] is not None:
                                player_data.loc[index, "penaltyMinutesDrawn"] += event["details"]["duration"]

                                t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                                if len(t_index) > 0:
                                    t_index = t_index[0]
                                    team_data.loc[t_index, "penaltiesAgainst"] += 1
                                    team_data.loc[t_index, "penaltyMinutesAgainst"] += event["details"]["duration"]

                # EVENT STOPPAGE
                case 516:
                    if event["details"]["reason"] == "goalie-stopped-after-sog": 
                        player_data.loc[lastSave[0], "rebounds"] -= 1
                        player_data.loc[lastSave[0], "xRebounds"] = player_data.loc[lastSave[0], "rebounds"] / player_data.loc[lastSave[0], "gamesPlayed"]

                        team_data.loc[lastSave[2], "reboundsFor"] -= 1
                        team_data.loc[lastSave[2], "xReboundsFor"] = team_data.loc[lastSave[2], "reboundsFor"] / team_data.loc[lastSave[2], "gamesPlayed"]

                        goalie_data.loc[lastSave[1], "rebounds"] -= 1
                        goalie_data.loc[lastSave[1], "reboundPercentage"] = goalie_data.loc[lastSave[1], "rebounds"] / goalie_data.loc[lastSave[1], "shotsFaced"]

                        team_data.loc[lastSave[3], "reboundsAgainst"] -= 1
                        team_data.loc[lastSave[3], "xReboundsAgainst"] = team_data.loc[lastSave[3], "reboundsAgainst"] / team_data.loc[lastSave[3], "gamesPlayed"]

                # EVENT TAKEAWAY
                case 525:
                   if event["details"]["playerId"] is not None:
                        index = player_data.index[player_data["playerId"] == event["details"]["playerId"]]
                        if len(index) > 0:
                            index = index[0]
                            player_data.loc[index, "takeaways"] += 1
                            t_index = team_data.index[team_data["team"] == player_data.loc[index, "team"]]
                            if len(t_index) > 0:
                                t_index = t_index[0]
                                team_data.loc[t_index, "takeaways"] += 1
                
                case _:
                   pass
        
        

        game_count += 1

    for index, row in player_data.iterrows(): 
        p_url = f'https://api-web.nhle.com/v1/player/{player_data.loc[index, "playerId"]}/game-log/{year}{year2}/2'
        p_res = requests.get(p_url)
        p_data = p_res.json()

        ice_times = []
        shifts = 0
        for game in p_data.get("gameLog", []):
            ice_times.append(game["toi"])
            shifts += 1
        player_data.loc[index, "xIcetime"] = average_ice_time(ice_times)
        player_data.loc[index, "xShifts"] = shifts / player_data.loc[index, "gamesPlayed"]
            
    team_data.to_csv(teams_data_file, index=False)
    print("Team Data saved at " + teams_data_file)

    player_data.to_csv(players_data_file, index=False)
    print("Team Data saved at " + players_data_file)

    goalie_data.to_csv(goalies_data_file, index=False)
    print("Team Data saved at " + goalies_data_file)

for i in range(2021, 2025):
    get_all_data(i, i+1)