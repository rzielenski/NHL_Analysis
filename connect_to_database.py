
import psycopg2
import pandas as pd
import requests
from NHL_Game_Retriever import time_to_float
from NHL_Get_Team_Data import get_team_game_stats

team_codes = {
            1 : "NJD", 2 : "NYI", 3 : "NYR", 4 : "PHI", 5 : "PIT", 
            6 : "BOS", 7 : "BUF", 8 : "MTL", 9 : "OTT", 10 : "TOR", 
            11: "ATL", 12 : "CAR", 13 : "FLA", 14 : "TBL", 15 : "WSH", 
            16 : "CHI", 17 : "DET", 18 : "NSH", 19 : "STL", 20 : "CGY", 
            21 : "COL", 22 : "EDM", 23 : "VAN", 24 : "ANA", 25 : "DAL",
            26 : "LAK", 27 : "PHX", 28 : "SJS", 29 : "CBJ", 30 : "MIN", 
            31 : "TBL", 32 : "QUE", 33 : "WIN", 34 : "NSH", 35 : "CLR", 
            36 : "SEN", 37 : "HAM", 38 : "PIR", 39 : "QUA", 40 : "DCG", 
            41 : "MWN", 42 : "QBD", 43 : "MMR", 44 : "NYA", 45 : "SLE", 
            46 : "OAK", 47 : "AFM", 48 : "KCS", 49 : "CLE", 50 : "DFL",
            51 : "BRK", 52 : "WPG", 53 : "ARI", 54 : "VGK", 55 : "SEA", 
            56 : "CGD", 57 : "TAN", 58: "TSP", 59 : "UTA"
}

team_names = {y: x for x, y in team_codes.items()}

def get_PP_PK(team, year):
    hostname = 'database-1.c52o66wwou5d.us-east-2.rds.amazonaws.com'
    database = 'Sports'
    username = 'postgres'
    pwd = 'CS465Project1-4*'
    port_id = 5432

    conn = None
    crsr = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        
        crsr = conn.cursor()
        crsr.execute(f"SELECT PPPercentage, PKPercentage FROM TeamSeasonStats WHERE season_team_id = {year}{year+1}{team_names[team]}")
        ret = crsr.fetchall()
        conn.commit()
        print(ret)
        return ret
    except Exception as error:
        print(error)
    finally:
        if crsr is not None:
            crsr.close()
        if conn is not None:
            conn.close()
            #print('Database connection terminated')
    
def get_players(year):
    hostname = 'database-1.c52o66wwou5d.us-east-2.rds.amazonaws.com'
    database = 'Sports'
    username = 'postgres'
    pwd = 'CS465Project1-4*'
    port_id = 5432

    conn = None
    crsr = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )

        crsr = conn.cursor()
        crsr.execute("SELECT p.player_id, p.name, ps.gamesPlayed, ps.goals, ps.assists, ps.hits, ps.takeaways, ps.giveaways FROM PlayerSeasonStats ps" /
                     "JOIN SeasonTeamPlayers stp ON ps.season_team_player_id = stp.id" /
                     "JOIN SeasonTeams st ON stp.season_team_id = st.season_team_id" /
                     "JOIN Players p ON stp.player_id = p.player_id" /
                     f"WHERE st.season_id = {year}{year+1}" /
                     "AND ps.gamesPlayed > 5")
        
        ret = crsr.fetchall()
        conn.commit()
        return ret
    except Exception as error:
        print(error)
    finally:
        if crsr is not None:
            crsr.close()
        if conn is not None:
            conn.close()
            #print('Database connection terminated')
    
def get_all_teams():
    hostname = 'database-1.c52o66wwou5d.us-east-2.rds.amazonaws.com'
    database = 'Sports'
    username = 'postgres'
    pwd = 'CS465Project1-4*'
    port_id = 5432

    conn = None
    crsr = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        
        crsr = conn.cursor()
        crsr.execute(f"SELECT * FROM SeasonTeams")
        ret = crsr.fetchall()
        conn.commit()
        #print(ret)
        return ret
    except Exception as error:
        print(error)
    finally:
        if crsr is not None:
            crsr.close()
        if conn is not None:
            conn.close()
            #print('Database connection terminated')
def connect():
    
    hostname = 'database-1.c52o66wwou5d.us-east-2.rds.amazonaws.com'
    database = 'Sports'
    username = 'postgres'
    pwd = 'CS465Project1-4*'
    port_id = 5432

    conn = None
    crsr = None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        
        crsr = conn.cursor()
        
        
        '''# Insert team season data
        crsr.execute(f"SELECT * FROM TeamSeasonStats")
        teams = crsr.fetchall()

        for team in teams:
            season_team_id = team[1]    
            xReboundsFor = 0
            xShotAttemptsPercentage = 0
            missedShotsFor = 0
            blockedShotsFor = 0
            shotAttemptsFor = 0
            reboundsFor = 0
            penaltiesFor = 0
            penaltyMinutesFor = 0
            hitsFor = 0
            takeawaysFor = 0
            giveawaysFor = 0

            xReboundsAgainst = 0
            missedShotsAgainst = 0
            blockedShotsAgainst = 0
            shotAttemptsAgainst = 0
            reboundsAgainst = 0
            penaltiesAgainst = 0
            penaltyMinutesAgainst = 0
            hitsAgainst = 0

            crsr.execute(f"SELECT * FROM TeamGameStats WHERE season_team_id = {season_team_id}")
            games = crsr.fetchall()
            for game in games:
                missedShotsFor += game[4]
                blockedShotsFor += game[5]
                shotAttemptsFor += game[6]
                reboundsFor += game[8]
                penaltiesFor += game[9]
                penaltyMinutesFor += game[10]
                hitsFor += game[13]
                takeawaysFor += game[14]
                giveawaysFor += game[15]

                missedShotsAgainst += game[17]
                blockedShotsAgainst += game[18]
                shotAttemptsAgainst += game[19]
                reboundsAgainst += game[21]
                penaltiesAgainst += game[22]
                penaltyMinutesAgainst += game[23]
                hitsAgainst += game[24]

            xReboundsFor = reboundsFor / max(team[2], 1)
            xReboundsAgainst = reboundsAgainst / max(team[2], 1)
            xShotAttemptsPercentage = shotAttemptsFor / max((shotAttemptsAgainst + shotAttemptsFor), 1)

            add_homeTeamStats = UPDATE TeamSeasonStats
                                    SET 
                                        xReboundsFor = %s, 
                                        xshotattmeptspercentage = %s, 
                                        missedShotsFor = %s, 
                                        blockedShotsFor = %s, 
                                        shotAttemptsFor = %s, 
                                        reboundsFor = %s, 
                                        penaltiesFor = %s, 
                                        penaltyMinutesFor = %s,
                                        hitsFor = %s, 
                                        takeawaysFor = %s, 
                                        giveawaysFor = %s, 
                                        xReboundsAgainst = %s, 
                                        missedShotsAgainst = %s, 
                                        blockedShotsAgainst = %s, 
                                        shotAttemptsAgainst = %s, 
                                        reboundsAgainst = %s, 
                                        penaltiesAgainst = %s, 
                                        penaltyMinutesAgainst = %s,
                                        hitsAgainst = %s
                                    WHERE id = %s;
                                
            add_values = (xReboundsFor, xShotAttemptsPercentage, missedShotsFor, blockedShotsFor, shotAttemptsFor, reboundsFor, penaltiesFor, penaltyMinutesFor, hitsFor, takeawaysFor, giveawaysFor,
                          xReboundsAgainst, missedShotsAgainst, blockedShotsAgainst, shotAttemptsAgainst, reboundsAgainst, penaltiesAgainst, penaltyMinutesAgainst, hitsAgainst, season_team_id)
            
            crsr.execute(add_homeTeamStats, add_values)

            conn.commit()                                  
        '''

        '''# Insert Player Season Stats
        crsr.execute(f"SELECT * FROM playergamestats WHERE season_team_id > 2014000000")
        players = crsr.fetchall()
        count = 0
        for player in players:
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} Games...")
            id = player[0]
            game_id = player[1]
            player_id = player[2]
            season_team_id = player[3]    
            season_team_player_id = int(f"{season_team_id}{player_id}")

            
            crsr.execute("SELECT 1 FROM PlayerSeasonStats WHERE player_id = %s", (player_id,))
            if not crsr.fetchone():
                # Insert player
                insert_player = 'INSERT INTO PlayerSeasonStats (id, season_team_player_id, player_id) VALUES (%s, %s, %s)'
                add_p_values = (season_team_player_id, season_team_player_id, player_id)
                crsr.execute(insert_player, add_p_values)
                conn.commit()
            crsr.execute("SELECT * FROM PlayerSeasonStats WHERE season_team_player_id = %s", (season_team_player_id,))
            pss = crsr.fetchone()
            if pss:
                gamesplayed = (pss[2] if pss[2] is not None else 0) + 1
                icetime = (pss[3] if pss[3] is not None else 0) + player[4]
                shifts = (pss[4] if pss[4] is not None else 0) + player[5]
                goals = (pss[5] if pss[5] is not None else 0) + player[6]
                assists = (pss[6] if pss[6] is not None else 0) + player[7]
                sog = (pss[7] if pss[7] is not None else 0) + player[8]
                blockedshots = (pss[8] if pss[8] is not None else 0) + player[9]
                pim = (pss[9] if pss[9] is not None else 0) + player[10]
                hits = (pss[10] if pss[10] is not None else 0) + player[11]
                giveaways = (pss[11] if pss[11] is not None else 0) + player[12]
                takeaways = (pss[12] if pss[12] is not None else 0) + player[13]
                add_homeTeamStats = 
                                    UPDATE PlayerSeasonStats
                                    SET
                                        gamesplayed = %s,
                                        icetime = %s,
                                        shifts = %s,
                                        goals = %s,
                                        assists = %s,
                                        sog = %s,
                                        blockedshots = %s,
                                        pim = %s,
                                        hits = %s,
                                        giveaways = %s,
                                        takeaways = %s
                                    WHERE id = %s
                                    
                            
                add_values = (gamesplayed, icetime, shifts, goals, assists, sog, blockedshots, pim, hits, giveaways, takeaways, season_team_player_id)
                
                crsr.execute(add_homeTeamStats, add_values)

                conn.commit()   
            else:
                print("pss not valid")
        '''

        '''# Insert Teams
        for i in range(2002, 2025):
            game_count = 1
            while True:
                game_id = f"{i}02{game_count:04}"
                game_count += 1
                url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
                res = requests.get(url)
                if res.status_code == 404:
                    print("Error 404: Not Found")
                    break
                if game_count % 100 == 0:
                    print(f"Processed {game_count} games...")
                data = res.json()
                season = data["season"]
                home_id = data["homeTeam"]["id"]
                away_id = data["awayTeam"]["id"]
                date = data["gameDate"]
                venue = data["venue"]["default"]
                add_game = 'INSERT INTO Games (game_id, season_id, home_team_id, away_team_id, game_date, venue) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING'
                add_values = (game_id, season, int(f"{season}{home_id}"), int(f"{season}{away_id}"), date, venue)
                crsr.execute(add_game, add_values)
                
        conn.commit()
        '''
        
        '''# Insert Team Game Stats
        for i in range(2014, 2025):
            game_count = 1
            while True:
                game_id = f"{i}02{game_count:04}"
                url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
                res = requests.get(url)
                if res.status_code == 404:
                    print("Error 404: Not Found")
                    break
                if game_count % 100 == 0:
                    print(f"Processed {game_count} games...")
                data = res.json()
                season = data["season"]
                home_id = data["homeTeam"]["id"]
                away_id = data["awayTeam"]["id"]

                add_homeTeamStats = INSERT INTO TeamGameStats (id, season_team_id, game_id, shotsOnGoalFor, missedShotsFor, blockedShotsFor, shotAttemptsFor, goalsFor, reboundsFor, penaltiesFor, penaltyMinutesFor, faceOffsWon, faceOffsLost,
                                            hitsFor, takeawaysFor, giveawaysFor, shotsOnGoalAgainst, missedShotsAgainst, blockedShotsAgainst, shotAttemptsAgainst, goalsAgainst,
                                            reboundsAgainst, penaltiesAgainst, penaltyMinutesAgainst, hitsAgainst) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING
                                    
                add_awayTeamStats = INSERT INTO TeamGameStats (id, season_team_id, game_id, shotsOnGoalFor, missedShotsFor, blockedShotsFor, shotAttemptsFor, goalsFor, reboundsFor, penaltiesFor, penaltyMinutesFor, faceOffsWon, faceOffsLost,
                                            hitsFor, takeawaysFor, giveawaysFor, shotsOnGoalAgainst, missedShotsAgainst, blockedShotsAgainst, shotAttemptsAgainst, goalsAgainst,
                                            reboundsAgainst, penaltiesAgainst, penaltyMinutesAgainst, hitsAgainst) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING
                                    
                add_home_values, add_away_values = get_team_game_stats(home_id, away_id, game_id)
                crsr.execute(add_homeTeamStats, add_home_values)
                crsr.execute(add_awayTeamStats, add_away_values)
                conn.commit()
                game_count += 1 
        '''
        # Insert Goalie Stats
        for i in range(2014, 2025):
            game_count = 1
            while True:
                game_id = f"{i}02{game_count:04}"
                url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
                res = requests.get(url)
                if res.status_code == 404:
                    print("Error 404: Not Found")
                    break
                if game_count % 100 == 0:
                    print(f"Processed {game_count} games...")
                data = res.json()
                season = data["season"]
                home_id = data["homeTeam"]["id"]
                away_id = data["awayTeam"]["id"]
                    
                for player in data["playerByGameStats"]["awayTeam"]["goalies"]:

                    team_id = away_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goalsAgainst = player["goalsAgainst"]
                    savePercentage = player["savePctg"]
                    shotsAgainst = player["shotsAgainst"]
                    saves = player["saves"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)

                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, int(f"{season}{team_id}"), toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
            
                for player in data["playerByGameStats"]["awayTeam"]["defense"]:
                    
                    team_id = away_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goals = player["goals"]
                    assists = player["assists"]
                    sog = player["sog"]
                    blockedShots = player["blockedShots"]
                    pim = player["pim"]
                    hits = player["hits"]
                    gaway = player["giveaways"]
                    taway = player["takeaways"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)
                        

                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, f"{season}{team_id}", toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
                

        '''# Insert Player Game Stats
        for i in range(2014, 2025):
            game_count = 1
            while True:
                game_id = f"{i}02{game_count:04}"
                url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
                res = requests.get(url)
                if res.status_code == 404:
                    print("Error 404: Not Found")
                    break
                if game_count % 100 == 0:
                    print(f"Processed {game_count} games...")
                data = res.json()
                season = data["season"]
                home_id = data["homeTeam"]["id"]
                away_id = data["awayTeam"]["id"]
                    
                for player in data["playerByGameStats"]["awayTeam"]["forwards"]:

                    team_id = away_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goals = player["goals"]
                    assists = player["assists"]
                    sog = player["sog"]
                    blockedShots = player["blockedShots"]
                    pim = player["pim"]
                    hits = player["hits"]
                    gaway = player["giveaways"]
                    taway = player["takeaways"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)

                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, int(f"{season}{team_id}"), toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
            
                for player in data["playerByGameStats"]["awayTeam"]["defense"]:
                    
                    team_id = away_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goals = player["goals"]
                    assists = player["assists"]
                    sog = player["sog"]
                    blockedShots = player["blockedShots"]
                    pim = player["pim"]
                    hits = player["hits"]
                    gaway = player["giveaways"]
                    taway = player["takeaways"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)
                        

                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, f"{season}{team_id}", toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
                
                for player in data["playerByGameStats"]["homeTeam"]["forwards"]:
                    
                    team_id = home_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goals = player["goals"]
                    assists = player["assists"]
                    sog = player["sog"]
                    blockedShots = player["blockedShots"]
                    pim = player["pim"]
                    hits = player["hits"]
                    gaway = player["giveaways"]
                    taway = player["takeaways"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)
                        
                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, int(f"{season}{team_id}"), toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
            
                for player in data["playerByGameStats"]["homeTeam"]["defense"]:
                    
                    team_id = home_id
                    player_id = player["playerId"]
                    toi = time_to_float(player["toi"])
                    shifts = player["shifts"]
                    goals = player["goals"]
                    assists = player["assists"]
                    sog = player["sog"]
                    blockedShots = player["blockedShots"]
                    pim = player["pim"]
                    hits = player["hits"]
                    gaway = player["giveaways"]
                    taway = player["takeaways"]

                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (player_id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, name, position) VALUES (%s, %s, %s)'
                        name = player["name"]["default"]
                        add_p_values = (player_id, f"{name}", player["position"])
                        crsr.execute(insert_player, add_p_values)
                        
                    
                    add_playerGameStats = 'INSERT INTO PlayerGameStats (id, game_id, player_id, season_team_id, icetime, shifts, goals, assists, sog, blockedShots, pim, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{game_id}{player_id}"), game_id, player_id, f"{season}{team_id}", toi, shifts, goals, assists, sog, blockedShots, pim, hits, gaway, taway)
                    crsr.execute(add_playerGameStats, add_values)
                game_count += 1
                conn.commit()
        '''

        '''# Insert SeasonTeamPlayers

        teams = get_all_teams()
        
        for team in teams:
            team_name = team_codes[team[2]]
            season = team[1]
            url = f"https://api-web.nhle.com/v1/roster/{team_name}/{season}"
            
            # Make the request
            res = requests.get(url)
            if res.status_code == 404:
                continue
            # Check if the request was successful
            if res.status_code == 200:
                try:
                    data = res.json()  # Try to parse the response as JSON
                except ValueError as e:
                    print(f"Error parsing JSON for {url}: {e}")
                    print(f"Response text: {res.text}")  # Print the raw response for debugging
                    continue  # Skip this iteration and move to the next team
                
                # If JSON parsing is successful, continue processing the data
                for player in data["forwards"]:
                    id = player["id"]
                    num = player.get("sweaterNumber", 0)

                    # Check if player exists
                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, player_name, birth_date, position, handedness) VALUES (%s, %s, %s, %s, %s)'
                        firstName = player["firstName"]["default"]
                        lastName = player["lastName"]["default"]
                        add_p_values = (
                            id,
                            f"{firstName} {lastName}",
                            player.get("birthDate", ""),
                            player.get("positionCode", ""),
                            player.get("shootsCatches", "")
                        )
                        crsr.execute(insert_player, add_p_values)

                    # Always insert into SeasonTeamPlayers (after ensuring player exists)
                    add_seasonTeamPlayers = 'INSERT INTO SeasonTeamPlayers (id, season_team_id, player_id, jersey_number) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{team[1]}{team[2]}{id}"), team[0], id, num)
                    crsr.execute(add_seasonTeamPlayers, add_values)

                    conn.commit()

                for player in data["defensemen"]:
                    id = player["id"]
                    num = player.get("sweaterNumber", 0)

                    # Check if player exists
                    crsr.execute("SELECT 1 FROM Players WHERE player_id = %s", (id,))
                    if not crsr.fetchone():
                        # Insert player
                        insert_player = 'INSERT INTO Players (player_id, player_name, birth_date, position, handedness) VALUES (%s, %s, %s, %s, %s)'
                        firstName = player["firstName"]["default"]
                        lastName = player["lastName"]["default"]
                        add_p_values = (
                            id,
                            f"{firstName} {lastName}",
                            player.get("birthDate", ""),
                            player.get("positionCode", ""),
                            player.get("shootsCatches", "")
                        )
                        crsr.execute(insert_player, add_p_values)

                    # Always insert into SeasonTeamPlayers (after ensuring player exists)
                    add_seasonTeamPlayers = 'INSERT INTO SeasonTeamPlayers (id, season_team_id, player_id, jersey_number) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                    add_values = (int(f"{team[1]}{team[2]}{id}"), team[0], id, num)
                    crsr.execute(add_seasonTeamPlayers, add_values)

                    conn.commit()

            else:
                print(f"Request failed for {url} with status code {res.status_code}.")
                print(f"Response text: {res.text}")

        '''
        
        ''' # INSERT ALL TEAMS
        url = f"https://api.nhle.com/stats/rest/en/team"

        res = requests.get(url)
        
        data = res.json()
        for team in data["data"]:
            team_id = team.get("id", "")
            abbrev = team.get("triCode", "")

            add_seasonTeam = 'INSERT INTO Teams (team_id, abbreviation) VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING'
            add_values = (team_id, abbrev)
            crsr.execute(add_seasonTeam, add_values)
            conn.commit()
        '''
        ''' #INSERT TEAM SEASON STATS
        for i in range(1917, 2025):
            url = f"https://api.nhle.com/stats/rest/en/team/summary?sort=shotsForPerGame&cayenneExp=seasonId={i}{i+1}%20and%20gameTypeId=2"

            res = requests.get(url)

            if res.status_code == 404:
                print("Error 404: Not Found")
                continue
            
            data = res.json()
            z_count = 0
            for team in data["data"]:
                team_id = team.get("teamId")
                add_seasonTeam = 'INSERT INTO SeasonTeams (season_team_id, season_id, team_id) VALUES (%s, %s, %s)'
                add_values = (f"{i}{i+1}{team_id}", f"{i}{i+1}", team_id)
                crsr.execute(add_seasonTeam, add_values)
                conn.commit()
        '''
        '''
        # INSERT TEAM SEASON STATS
        for i in range(1960, 2025):
            url = f"https://api.nhle.com/stats/rest/en/team/summary?sort=shotsForPerGame&cayenneExp=seasonId={i}{i+1}%20and%20gameTypeId=2"

            res = requests.get(url)

            if res.status_code == 404:
                print("Error 404: Not Found")
                break
                

            data = res.json()
            
            for team in data["data"]:
                fo = team.get("faceoffWinPct", 0)
                gp = team.get("gamesPlayed", 0)
                xGF = team.get("goalsForPerGame", 0)
                xGA = team.get("goalsAgainstPerGame", 0)
                w = team.get("wins", 0)
                l = team.get("losses", 0) 
                otL = team.get("otLosses", 0)
                xPP = team.get("powerPlayNetPct", 0)
                xPK = team.get("penaltyKillNetPct", 0)
                xP = team.get("pointPct", 0)
                xSF = team.get("shotsForPerGame", 0)
                xSA = team.get("shotsAgainstPerGame", 0)
                team_id = team.get("teamId")
                season_team_id = f"{i}{i+1}{team_id}"

                add_players = 'INSERT INTO TeamSeasonStats (id, season_team_id, gamesPlayed, wins, losses, otLosses, xGoalsFor, xGoalsAgainst,' \
                'xShotsOnGoalFor, xShotsOnGoalAgainst, shotsOnGoalFor, shotsOnGoalAgainst, faceOffPercentage, pkPercentage, ppPercentage, pointPercentage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING'
                add_values = (season_team_id, season_team_id, gp, w, l, otL, xGF, xGA, xSF, xSA, int(xSF * gp), int(xSA * gp), fo, xPK, xPP, xP)

                crsr.execute(add_players, add_values)
                conn.commit()
        '''
        '''
        df = pd.read_csv('2014_2015_All_Players_Data.csv')
        for index, player in df.iterrows():
            player = df.iloc[index]
            p_id = int(player.iloc[0])
            p_name = player.iloc[2]
            p_team =player.iloc[3]
            p_position = player.iloc[4]
            p_gamesPlayed = int(player.iloc[5])
            p_icetime = float(player.iloc[6])
            p_shifts = int(player.iloc[7])
            p_goals = int(player.iloc[8])
            p_assists = int(player.iloc[9])
            p_sog = int(player.iloc[10])
            p_blockedShots = int(player.iloc[11])
            p_penaltyMinutes = int(player.iloc[12])
            p_PIM = int(player.iloc[13])
            p_hits = int(player.iloc[14])
            p_giveaways = int(player.iloc[15])
            p_takeaways = int(player.iloc[16])

        insert_player = 'INSERT INTO PlayerSeasonStats (id, , gamesPlayed, iceTime, shifts, goals, assists, sog, blockedShots, penaltyMinutes, PIM, hits, giveaways, takeaways) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        insert_values = (p_id, p_name, p_team, p_position, p_gamesPlayed, p_icetime, p_shifts, p_goals, p_assists, p_sog, p_blockedShots, p_penaltyMinutes, p_PIM, p_hits, p_giveaways, p_takeaways)
        crsr.execute(insert_player, insert_values)
        '''

        

    except Exception as error:
        print(error)
    finally:
        if crsr is not None:
            crsr.close()
        if conn is not None:
            conn.close()
            print('Database connection terminated')

if __name__ == "__main__":
    connect()