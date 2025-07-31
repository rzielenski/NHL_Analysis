import requests
import pandas as pd

def get_team_game_stats(home_id, away_id, game_id):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    res = requests.get(url)
    data = res.json()
    home_team_stats = pd.Series(0, index=['id', 'season_team_id', 'game_id', 'sogFor', 'missedShotsFor', 'blockedShotsFor', 'shotAttemptsFor', 'goalsFor', 'reboundsFor', 'penaltiesFor', 'PIMFor', 'faceOffsWon', 'faceOffsLost',
                                            'hitsFor', 'takeawaysFor', 'giveawaysFor', 'sogAgainst', 'missedShotsAgainst', 'blockedShotsAgainst', 'shotAttemptsAgainst', 'goalsAgainst',
                                            'reboundsAgainst', 'penaltiesAgainst', 'PIMAgainst', 'hitsAgainst'])

    away_team_stats = pd.Series(0, index=['id', 'season_team_id', 'game_id', 'sogFor', 'missedShotsFor', 'blockedShotsFor', 'shotAttemptsFor', 'goalsFor', 'reboundsFor', 'penaltiesFor', 'PIMFor', 'faceOffsWon', 'faceOffsLost',
                                            'hitsFor', 'takeawaysFor', 'giveawaysFor', 'sogAgainst', 'missedShotsAgainst', 'blockedShotsAgainst', 'shotAttemptsAgainst', 'goalsAgainst',
                                            'reboundsAgainst', 'penaltiesAgainst', 'PIMAgainst', 'hitsAgainst'])
    
    season = data["season"]

    home_team_stats['game_id'] = int(game_id)
    away_team_stats['game_id'] = int(game_id)

    home_team_stats['season_team_id'] = int(f"{season}{home_id}")
    away_team_stats['season_team_id'] = int(f"{season}{away_id}")

    home_team_stats['id'] = int(f"{game_id}{home_id}")
    away_team_stats['id'] = int(f"{game_id}{away_id}")

    last_shot_id = 0
    for event in data.get("plays", []):
        match event["typeCode"]:
            # FACEOFF EVENT
            case 502:    
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['faceOffsWon'] += 1
                        away_team_stats['faceOffsLost'] += 1
                    else:
                        home_team_stats['faceOffsLost'] += 1
                        away_team_stats['faceOffsWon'] += 1

            # HIT EVENT 
            case 503:    
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['hitsFor'] += 1
                        away_team_stats['hitsAgainst'] += 1
                    else:
                        home_team_stats['hitsAgainst'] += 1
                        away_team_stats['hitsFor'] += 1

            # GIVEAWAY EVENT
            case 504:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['giveawaysFor'] += 1
                    else:
                        away_team_stats['giveawaysFor'] += 1
            
            
            # EVENT GOAL!!! 
            case 505:              
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['goalsFor'] += 1
                        away_team_stats['goalsAgainst'] += 1
                        home_team_stats['sogFor'] += 1
                        away_team_stats['sogAgainst'] += 1
                        home_team_stats['shotAttemptsFor'] += 1
                        away_team_stats['shotAttemptsAgainst'] += 1
                    else:
                        home_team_stats['goalsAgainst'] += 1
                        away_team_stats['goalsFor'] += 1
                        home_team_stats['sogAgainst'] += 1
                        away_team_stats['sogFor'] += 1
                        home_team_stats['shotAttemptsAgainst'] += 1
                        away_team_stats['shotAttemptsFor'] += 1

            # EVENT SHOT ON GOAL
            case 506:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['sogFor'] += 1
                        away_team_stats['sogAgainst'] += 1
                        home_team_stats['reboundsFor'] += 1
                        away_team_stats['reboundsAgainst'] += 1
                        home_team_stats['shotAttemptsFor'] += 1
                        away_team_stats['shotAttemptsAgainst'] += 1
                    else:
                        home_team_stats['sogAgainst'] += 1
                        away_team_stats['sogFor'] += 1
                        home_team_stats['reboundsAgainst'] += 1
                        away_team_stats['reboundsFor'] += 1
                        home_team_stats['shotAttemptsAgainst'] += 1
                        away_team_stats['shotAttemptsFor'] += 1
                    last_shot_id = id

            # EVENT MISSED SHOT
            case 507:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['missedShotsFor'] += 1
                        away_team_stats['missedShotsAgainst'] += 1
                        home_team_stats['shotAttemptsFor'] += 1
                        away_team_stats['shotAttemptsAgainst'] += 1
                    else:
                        home_team_stats['missedShotsAgainst'] += 1
                        away_team_stats['missedShotsFor'] += 1
                        home_team_stats['shotAttemptsAgainst'] += 1
                        away_team_stats['shotAttemptsFor'] += 1

            # EVENT BLOCKED SHOT
            case 508:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['blockedShotsFor'] += 1
                        away_team_stats['blockedShotsAgainst'] += 1
                        home_team_stats['shotAttemptsFor'] += 1
                        away_team_stats['shotAttemptsAgainst'] += 1
                    else:
                        home_team_stats['blockedShotsAgainst'] += 1
                        away_team_stats['blockedShotsFor'] += 1
                        home_team_stats['shotAttemptsAgainst'] += 1
                        away_team_stats['shotAttemptsFor'] += 1

            case 509:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if (pim := event["details"]["duration"]) is not None:
                        if id == home_id:
                            home_team_stats['penaltiesFor'] += 1
                            away_team_stats['penaltiesAgainst'] += 1
                            home_team_stats['PIMFor'] += pim
                            away_team_stats['PIMAgainst'] += pim
                        else:
                            home_team_stats['penaltiesAgainst'] += 1
                            away_team_stats['penaltiesFor'] += 1
                            home_team_stats['PIMAgainst'] += pim
                            away_team_stats['PIMFor'] += pim

            # EVENT STOPPAGE
            case 516:
                if event["details"]["reason"] == "goalie-stopped-after-sog": 
                    if last_shot_id == home_id:
                        home_team_stats['reboundsFor'] -= 1
                        away_team_stats['reboundsAgainst'] -= 1
                    else:
                        home_team_stats['reboundsAgainst'] -= 1
                        away_team_stats['reboundsFor'] -= 1

            # EVENT TAKEAWAY
            case 525:
                if (id := event["details"]["eventOwnerTeamId"]) is not None:
                    if id == home_id:
                        home_team_stats['takeawaysFor'] += 1
                    else:
                        away_team_stats['takeawaysFor'] += 1
            
            case _:
                pass
    return tuple(home_team_stats), tuple(away_team_stats)
