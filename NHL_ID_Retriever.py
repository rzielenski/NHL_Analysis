import requests
import csv
import datetime
import time

def get_all_game_scores(year):
    games = []

    start_date = datetime.date(year, 10, 1)
    end_date = datetime.date(year+1, 4, 15)
    current_date = start_date

    game_file =  current_date.strftime("%Y") + '_' + end_date.strftime("%Y") + '_All_Game_Scores.csv'
    # Get all teams

    base_url = 'https://api-web.nhle.com/v1/score/'

    games.append(["Away", "Away_Score", "Home", "Home_Score"])

    while current_date <= end_date:
        date_as_str = current_date.strftime("%Y-%m-%d")
        url = f"{base_url}{date_as_str}"

        res = requests.get(url)
        data = res.json()

        

        for game in data.get('games', []):
            games.append([game["awayTeam"]["abbrev"], game["awayTeam"]["score"], game["homeTeam"]["abbrev"], game["homeTeam"]["score"]]) 
        
        current_date += datetime.timedelta(days=1)
        time.sleep(.2)   

    with open(game_file, 'w', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerows(games)

    print(f"CSV file '{game_file}' created successfully.")

get_all_game_scores(2022)
