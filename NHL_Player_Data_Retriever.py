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

def add_players(team, players, playerId_dict, year, teamName):
    for forward in team["forwards"]:
        index = playerId_dict.get(forward["playerId"], None)

        if index is None:
            index = players.shape[0]

            playerId_dict[forward["playerId"]] = index

            new_row = pd.DataFrame([{'playerId': forward["playerId"], 'season': year,
                                    'name': forward["name"]["default"],'team': teamName,
                                    'position': forward["position"], 'gamesPlayed': 1,
                                    'icetime': time_to_float(forward["toi"]), 'shifts': forward["shifts"],
                                    'goals': forward["goals"], 'assists': forward["assists"],
                                    'sog': forward["sog"], 'blockedShots': forward["blockedShots"],
                                    'pm': forward["plusMinus"], 'pim': forward["pim"],
                                    'hits': forward["hits"], 'giveaways': forward["giveaways"],
                                    'takeaways': forward["takeaways"], 'AccFaceOffPercentage': forward["faceoffWinningPctg"]}])
            
            players.loc[len(players)] = new_row.iloc[0]
        
        else:
            players.loc[index, ['gamesPlayed', 'icetime', 'shifts', 'goals', 'assists', 'sog',
                            'blockedShots', 'pm', 'pim', 'hits', 'giveaways', 'takeaways', 'AccFaceOffPercentage']] = [players.at[index, 'gamesPlayed'] + 1,
                                                                                                                        players.at[index, 'icetime'] + time_to_float(forward["toi"]),
                                                                                                                        players.at[index, 'shifts'] + forward["shifts"],
                                                                                                                        players.at[index, 'goals'] + forward["goals"],
                                                                                                                        players.at[index, 'assists'] + forward["assists"],
                                                                                                                        players.at[index, 'sog'] + forward["sog"],
                                                                                                                        players.at[index, 'blockedShots'] + forward["blockedShots"],
                                                                                                                        players.at[index, 'pm'] + forward["plusMinus"],
                                                                                                                        players.at[index, 'pim'] + forward["pim"],
                                                                                                                        players.at[index, 'hits'] + forward["hits"],
                                                                                                                        players.at[index, 'giveaways'] + forward["giveaways"],
                                                                                                                        players.at[index, 'takeaways'] + forward["takeaways"],
                                                                                                                        players.at[index, 'AccFaceOffPercentage'] + forward["faceoffWinningPctg"]]

    for defenseman in team["defense"]:
        index = playerId_dict.get(defenseman["playerId"], None)

        if index is None:
            index = players.shape[0]

            playerId_dict[defenseman["playerId"]] = index

            new_row = pd.DataFrame([{'playerId': defenseman["playerId"], 'season': year,
                                    'name': defenseman["name"]["default"],'team': teamName,
                                    'position': defenseman["position"], 'gamesPlayed': 1,
                                    'icetime': time_to_float(defenseman["toi"]), 'shifts': defenseman["shifts"],
                                    'goals': defenseman["goals"], 'assists': defenseman["assists"],
                                    'sog': defenseman["sog"], 'blockedShots': defenseman["blockedShots"],
                                    'pm': defenseman["plusMinus"], 'pim': defenseman["pim"],
                                    'hits': defenseman["hits"], 'giveaways': defenseman["giveaways"],
                                    'takeaways': defenseman["takeaways"], 'AccFaceOffPercentage': defenseman["faceoffWinningPctg"]}])
            
            players.loc[len(players)] = new_row.iloc[0]
        
        else:
            players.loc[index, ['gamesPlayed', 'icetime', 'shifts', 'goals', 'assists', 'sog',
                            'blockedShots', 'pm', 'pim', 'hits', 'giveaways', 'takeaways', 'AccFaceOffPercentage']] = [players.at[index, 'gamesPlayed'] + 1,
                                                                                                                        players.at[index, 'icetime'] + time_to_float(forward["toi"]),
                                                                                                                        players.at[index, 'shifts'] + forward["shifts"],
                                                                                                                        players.at[index, 'goals'] + forward["goals"],
                                                                                                                        players.at[index, 'assists'] + forward["assists"],
                                                                                                                        players.at[index, 'sog'] + forward["sog"],
                                                                                                                        players.at[index, 'blockedShots'] + forward["blockedShots"],
                                                                                                                        players.at[index, 'pm'] + forward["plusMinus"],
                                                                                                                        players.at[index, 'pim'] + forward["pim"],
                                                                                                                        players.at[index, 'hits'] + forward["hits"],
                                                                                                                        players.at[index, 'giveaways'] + forward["giveaways"],
                                                                                                                        players.at[index, 'takeaways'] + forward["takeaways"],
                                                                                                                        players.at[index, 'AccFaceOffPercentage'] + forward["faceoffWinningPctg"]]   
     
def get_all_player_data(year, year2):

    game_count = 1
    playerId_dict = {}
    players = pd.DataFrame(columns=['playerId', 'season', 'name', 'team', 'position', 'gamesPlayed', 'icetime', 'shifts', 'goals', 'assists', 'sog',
                                    'blockedShots', 'pm', 'pim', 'hits', 'giveaways', 'takeaways', 'AccFaceOffPercentage'])

    data_file = f'{year}_{year2}_All_Players_Data.csv'

    game_id = game_id = f"{year}02{game_count:04}"

    while True:
        if game_count % 100 == 0:
            print(f"Processed {game_count} games so far...")

        game_id = f"{year}02{game_count:04}"
        url = f'https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore'
        
        res = requests.get(url)
        if res.status_code == 404:
            print("Error 404: Not Found")
            break

        data = res.json()
        
        base = data.get("playerByGameStats", None)
        if base is None:
            break
        team = base["awayTeam"]
        add_players(team, players, playerId_dict, year, data["awayTeam"]["abbrev"])

        base = data.get("playerByGameStats", None)
        if base is None:
            break
        team = base["awayTeam"]
        add_players(team, players, playerId_dict, year, data["homeTeam"]["abbrev"])

        game_count += 1
        
    players.to_csv(data_file, index=False)

    print(f"CSV file '{data_file}' created successfully.")

#for i in range(2014, 2025):
get_all_player_data(2024, 2025)
