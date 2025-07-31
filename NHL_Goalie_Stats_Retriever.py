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


    goalie_data = pd.DataFrame(columns=["playerId", "season", "name", "team", "gamesPlayed", "savePercentage", "GAA", "goalsAgainst", "saves", "shotsFaced", "rebounds", 
                                        "reboundPercentage"]) 

    season = f"{year}_{year2}"
    

    goalies_data_file = f"{season}_All_Goalies_Data_TEST.csv"

    processed_players = set()

    while True:
        if game_count % 100 == 0:
            print(f"Processed {game_count} games so far...")

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

        home_win = away["score"] < home["score"]
            
        #print(home["abbrev"], away["abbrev"], ": ", home["abbrev"] if home_win else away["abbrev"])
        
        
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
                pass
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
                    goalie_data.fillna(0, inplace=True)
                else:
                    # Update existing goalie
                    g_idx = goalie_data.index[goalie_data["playerId"] == player_id].tolist()[0]
                    # Update team if needed (for traded goalies)
                    goalie_data.at[g_idx, "team"] = team_abbrev
            
        
        
        

        
        starting_goalie_id = 0
        for event in data.get("plays", []):
            match event["typeCode"]:
                
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
                            goalie_data.loc[ga_index, "savePercentage"] = 1 - goalie_data.loc[ga_index, "goalsAgainst"] / goalie_data.loc[ga_index, "shotsFaced"]


                # EVENT SHOT ON GOAL
                case 506:
                    goalie_id = event["details"].get("goalieInNetId")

                    if goalie_id is not None:
                        ga_index = goalie_data.index[goalie_data["playerId"] == goalie_id]
                        if len(ga_index) > 0:
                            ga_index = ga_index[0]
                            if starting_goalie_id == 0:
                                goalie_data.loc[ga_index, "gamesPlayed"] += 1
                                starting_goalie_id = goalie_id
                            elif starting_goalie_id > 2:
                                if starting_goalie_id != goalie_id:
                                    goalie_data.loc[ga_index, "gamesPlayed"] += 1
                                    starting_goalie_id = 1
                                    
                            goalie_data.loc[ga_index, "saves"] += 1
                            goalie_data.loc[ga_index, "shotsFaced"] += 1
                            goalie_data.loc[ga_index, "savePercentage"] = 1 - (goalie_data.loc[ga_index, "goalsAgainst"] / goalie_data.loc[ga_index, "shotsFaced"])
                            goalie_data.loc[ga_index, "rebounds"] += 1
                            goalie_data.loc[ga_index, "reboundPercentage"] = goalie_data.loc[ga_index, "rebounds"] / goalie_data.loc[ga_index, "shotsFaced"]
 

        game_count += 1


    goalie_data.to_csv(goalies_data_file, index=False)
    print("Goalie Data saved at " + goalies_data_file)

for i in range(2014, 2019):
    get_all_data(i, i+1)

for i in range(2020, 2025):
    get_all_data(i, i+1)