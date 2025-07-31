import requests
import csv
import datetime
import time
import pandas as pd

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

def time_to_float(time_str):
    minutes, seconds = map(int, time_str.split(":"))
    return minutes + seconds / 60

def get_player_rating(player):
    xPoints = (player['goals'] + player['assists']) / player['gamesPlayed']
    xBlocks = player['blockedShots'] / player['gamesPlayed']
    xToi = player['icetime'] / player['gamesPlayed']
    xHits = player['hits'] / player['gamesPlayed']
    xTkwy = player['takeaways'] / player['gamesPlayed']
    xGvwy = player['giveaways'] / player['gamesPlayed']
    # temporary: avg faceoff needs to take into acc num faceoffs total which is in play by play data not boxscore
    xFO = player['AccFaceOffPercentage']
    xPim = player['pim'] / player['gamesPlayed']
    xPlusMinus = player['pm'] / player['gamesPlayed']

    if player['position'] == 'D':
        rating = (
            0.3 * xPoints +
            0.25 * xBlocks +
            0.15 * (xToi / 1200) +
            0.1 * xHits +
            0.15 * xTkwy -
            0.4 * xGvwy -
            0.15 * xPim +
            0.1 * xPlusMinus
        )
    else:  
        rating = (
            0.45 * xPoints +
            0.05 * xBlocks +
            0.15 * (xToi / 1200) +
            0.1 * xHits +
            0.15 * xTkwy -
            0.4 * xGvwy -
            0.15 * xPim +
            0.1 * xPlusMinus +
            0.1 * (xFO - 0.5)  
        )
    return round(rating, 3)
def get_all_player_rating(game_data, away, year):
    player_ids = []
    awayId = game_data["awayTeam"]["id"]
    for player in game_data["rosterSpots"]:
        if away:
            if player["teamId"] == awayId:
                player_ids.append(player["playerId"]) 
        else:
            if player["teamId"] != awayId:
                player_ids.append(player["playerId"]) 
    players = pd.read_csv(f'{year}_{year+1}_All_Players_Data.csv')

    player_rating = 0
    for playerId in player_ids:
        index = players.loc[players['playerId'] == playerId].index
        if not index.empty:
            player_rating += get_player_rating(players.loc[index[0]])
    return player_rating

def get_goalie_rating(game_data, away, year):
     goalies = pd.read_csv(f'{year}_{year+1}_All_Goalies_Data_TEST.csv')
     goalie_count = 0
     rating = 0
     awayId = game_data["awayTeam"]["id"]
     for goalie in game_data["rosterSpots"]:
        if goalie["positionCode"] != "G":
            continue
        if away:
            if goalie["teamId"] == awayId:
                goalie_count += 1
                index = goalies.loc[goalies['playerId'] == goalie["playerId"]].index

                if not index.empty:
                    goalie_row = goalies.loc[index[0]]  # Extract first matching row
                    rating += ( (.01 * goalie_row['gamesPlayed']) + (2 * goalie_row['savePercentage']) - (1.5 * goalie_row['GAA']) - (2 * goalie_row['reboundPercentage']) )
        else:
            if goalie["teamId"] != awayId:
                goalie_count += 1
                index = goalies.loc[goalies['playerId'] == goalie["playerId"]].index

                if not index.empty:
                    goalie_row = goalies.loc[index[0]]  # Extract first matching row
                    rating += ( (.005 * goalie_row['gamesPlayed']) + (goalie_row['savePercentage']) - (.2 * goalie_row['GAA']) - (.3 * goalie_row['reboundPercentage']) )
     
     
     if goalie_count == 0:
        return 0
        '''
        for goalie in game_data["playerByGameStats"]["awayTeam" if away else "homeTeam"]["goalies"]:
            index = goalies.loc[goalies['playerId'] == goalie["playerId"]].index
            if not index.empty:
                goalie_row = goalies.loc[index[0]]  # Extract first matching row
                rating += ( (.005 * goalie_row['gamesPlayed']) + (goalie_row['savePercentage']) - (.2 * goalie_row['GAA']) - (.3 * goalie_row['reboundPercentage']) )
            goalie_count += 1
        '''
     return rating / goalie_count

def get_all_game_scores(year, year2):
    
    games = pd.DataFrame(columns=['Away', 'Away_Score', 'Home', 'Home_Score', 'gameId', 'Away_xPlayerRating', 'Away_GoalieRating', 'Home_xPlayerRating', 'Home_GoalieRating'])

    start_date = datetime.date(year, 10, 1)
    end_date = datetime.date(year2, 4, 15)
    current_date = start_date

    game_file =  current_date.strftime("%Y") + '_' + end_date.strftime("%Y") + '_All_Game_Scores.csv'
    # Get all teams

    game_count = 1
    while True:
        game_id = f"{year}02{game_count:04}"
        game_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

        res = requests.get(game_url)
        if res.status_code == 404:
            print("Error 404: Not Found")
            break
        game = res.json()

        if game_count % 100 == 0:
            print(f"Processed {game_count} games so far...")
        away_players_rating = get_all_player_rating(game, True, year)
        home_players_rating = get_all_player_rating(game, False, year)
        away_goalie_rating = get_goalie_rating(game, True, year)
        home_goalie_rating = get_goalie_rating(game, False, year)
        
        new_row = pd.DataFrame([{'Away': game["awayTeam"]["abbrev"], 'Away_Score': game["awayTeam"]["score"],
                                'Home': game["homeTeam"]["abbrev"], 'Home_Score': game["homeTeam"]["score"],
                                'gameId': game["id"], 'Away_xPlayerRating': away_players_rating,
                                'Away_GoalieRating': away_goalie_rating, 'Home_xPlayerRating': home_players_rating,
                                'Home_GoalieRating': home_goalie_rating}])
        games.loc[len(games)] = new_row.iloc[0]
        game_count += 1

    games.to_csv(game_file, index=False)

    print(f"CSV file '{game_file}' created successfully.")

'''
for i in range(2014, 2019):
    get_all_game_scores(i, i+1)
for i in range(2020, 2024):
    get_all_game_scores(i, i+1)
'''