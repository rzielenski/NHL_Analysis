import requests

def get_odds(gameId):
    url = "https://api-web.nhle.com/v1/partner-game/US/now"

    res = requests.get(url)
    data = res.json()

    for game in data.get("games"):
        if game["gameId"] == gameId:
            new_game = ([game["gameId"], game["homeTeam"]["abbrev"], next((odd["value"] for odd in game["homeTeam"]["odds"] if odd["description"] == "MONEY_LINE_2_WAY"), None), 
                          game["awayTeam"]["abbrev"], next((odd["value"] for odd in game["awayTeam"]["odds"] if odd["description"] == "MONEY_LINE_2_WAY"), None)])
    return new_game