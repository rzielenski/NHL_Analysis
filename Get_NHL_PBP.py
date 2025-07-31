import requests
import pandas as pd
import  numpy as np
import concurrent.futures

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

def get_PBP_conc(year):
    max_games = 1312  # NHL regular season total
    all_rows = []  # Collect all rows here
    session = requests.Session()  # Reuse connections

    def fetch_game(count):
        game_rows = []
        game_id = f"{year}02{count:04}"
        url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
        res = session.get(url)
        if res.status_code != 200:
            return game_rows 
        
        data = res.json()
        home_id = data['homeTeam']['id']
        away_id = data['awayTeam']['id']
        
        for play in data.get('plays', []):
            period = play['periodDescriptor']['number']
            time = play['timeInPeriod']
            event = play['typeDescKey']
            team_id = play.get('details', {}).get('eventOwnerTeamId')
            team = 'HOME' if team_id == home_id else 'AWAY' if team_id == away_id else None
            if team:
                game_rows.append({'id': game_id, 'home_abb': team_codes[home_id], 'away_abb': team_codes[away_id], 'event': event, 'team': team})
        
        return game_rows

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor: 
        future_to_game = {executor.submit(fetch_game, count): count for count in range(1, max_games + 1)}
        for future in concurrent.futures.as_completed(future_to_game):
            count = future_to_game[future]
            if count % 100 == 0:
                print(f"Processed {count} games...")
            try:
                all_rows.extend(future.result())
            except Exception as e:
                print(f"Error in game {count}: {e}")

    df = pd.DataFrame(all_rows)
    df.to_parquet(rf'C:\Users\Richard\OneDrive\Documentos\CS465_Project\data\{year}_{year+1}_PBP.parquet', compression='snappy')

def show_df():
    df = pd.read_parquet(r'C:\Users\Richard\OneDrive\Documentos\CS465_Project\data\2023_2024_PBP.parquet')
    print(df.head(10))

#get_PBP_conc(2023)
if __name__ == "__main__":
    get_PBP_conc(2023)