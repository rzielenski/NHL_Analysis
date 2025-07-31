import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import GridSearchCV
import tensorflow as tf
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, BatchNormalization, LeakyReLU
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.callbacks import EarlyStopping
import seaborn as sns
import matplotlib.pyplot as plt
import joblib
from connect_to_database import get_PP_PK

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

team_data_paths = ['2014_2015_All_Teams_Data_TEST.csv',
                    '2015_2016_All_Teams_Data_TEST.csv',
                    '2016_2017_All_Teams_Data_TEST.csv',
                    '2017_2018_All_Teams_Data_TEST.csv',
                    '2018_2019_All_Teams_Data_TEST.csv',
                    '2020_2021_All_Teams_Data_TEST.csv',
                    '2021_2022_All_Teams_Data_TEST.csv',
                    '2022_2023_All_Teams_Data_TEST.csv',
                    '2023_2024_All_Teams_Data_TEST.csv']


game_scores_paths = ['2014_2015_All_Game_Scores.csv',
                  '2015_2016_All_Game_Scores.csv',
                  '2016_2017_All_Game_Scores.csv',
                  '2017_2018_All_Game_Scores.csv',
                  '2018_2019_All_Game_Scores.csv',
                  '2020_2021_All_Game_Scores.csv',
                  '2021_2022_All_Game_Scores.csv',
                  '2022_2023_All_Game_Scores.csv',
                  '2023_2024_All_Game_Scores.csv']



def merge_data(list_game_data, list_team_data):
  all_games = []
  all_labels = []

  for i, season_game_data in enumerate(list_game_data):
    for j, game_data in season_game_data.iterrows():
      df_game_data = game_data
      if j % 100 == 0:
        print(f"Processed {j} games...")

      #data_for_NN = pd.DataFrame(columns=["Home_GF", "Home_GF_Pct", "Home_GA", "Home_Shots_For", "Home_Shooting_Pct", "Home_Save_Pct", "Home_PDO", "Home_PP_Pct", "Home_PK_Pct",
      #                                    "Away_GF", "Away_GF_Pct", "Away_GA", "Away_Shots_For", "Away_Shooting_Pct", "Away_Save_Pct", "Away_PDO", "Away_PP_Pct", "Away_PK_Pct"])

      label = int(df_game_data['Home_Score'] > df_game_data['Away_Score'])

      home_team = df_game_data['Home']
      away_team = df_game_data['Away']

      year = int(str(df_game_data['gameId'])[:4])

      teams = list_team_data[i]

      h_data = teams.loc[teams['team'] == home_team].iloc[0]
      a_data = teams.loc[teams['team'] == away_team].iloc[0]
      
      h_st = get_PP_PK(home_team, year)
      a_st = get_PP_PK(away_team, year)

      h_pp, h_pk = h_st[0]
      a_pp, a_pk = a_st[0]

      row = {
          "Home_GF": h_data['goalsFor'],
          "Home_GF_Pct": h_data['goalsFor'] / (h_data['goalsFor'] + h_data['goalsAgainst']),
          "Home_GA": h_data['goalsAgainst'],
          "Home_Shots_For": h_data['shotsOnGoalFor'],
          "Home_Shooting_Pct": h_data['xShotAttemptsPercentage'],
          "Home_Save_Pct": h_data['goalsAgainst'] / h_data['shotsOnGoalAgainst'],
          "Home_PDO": h_data['xShotAttemptsPercentage'] + (h_data['goalsAgainst'] / h_data['shotsOnGoalAgainst']),
          "Home_PP_Pct": h_pp,
          "Home_PK_Pct": h_pk,
          "Away_GF": a_data['goalsFor'],
          "Away_GF_Pct": a_data['goalsFor'] / (a_data['goalsFor'] + a_data['goalsAgainst']),
          "Away_GA": a_data['goalsAgainst'],
          "Away_Shots_For": a_data['shotsOnGoalFor'],
          "Away_Shooting_Pct": a_data['xShotAttemptsPercentage'],
          "Away_Save_Pct": a_data['goalsAgainst'] / a_data['shotsOnGoalAgainst'],
          "Away_PDO": a_data['xShotAttemptsPercentage'] + (a_data['goalsAgainst'] / a_data['shotsOnGoalAgainst']),
          "Away_PP_Pct": a_pp,
          "Away_PK_Pct": a_pk
      }

      all_games.append(row)
      all_labels.append(label)
  
  
  X = pd.DataFrame(all_games)
  y = pd.Series(all_labels, name="Label")

  X.to_csv('Games_For_NN.csv', index=False)
  y.to_csv('Labels_For_NN.csv', index=False)

  return X, y


def main():
  '''
  all_teams = []
  all_games = []

  for my_path in game_scores_paths:
    all_games.append(pd.read_csv(my_path))

  for my_path in team_data_paths:
    all_teams.append(pd.read_csv(my_path))


  X, y = merge_data(all_games, all_teams)
  '''
  X = pd.read_csv('Games_For_NN.csv')
  y = pd.read_csv('Labels_For_NN.csv')
  # Filter and print only the columns with NaN values
  nan_counts = X.isna().sum()
  nan_columns = nan_counts[nan_counts > 0]
  print(nan_columns)

  # split into train and test
  X_train, X_test, y_train, y_test = train_test_split(
      X, y, test_size=0.2, random_state=42
  )

  scaler = StandardScaler()
  X_train_scaled = scaler.fit_transform(X_train)
  X_test_scaled = scaler.transform(X_test)

  model = Sequential([
      Dense(64, input_shape=(X_train.shape[1],)),
      BatchNormalization(),
      LeakyReLU(alpha=0.01),
      Dropout(0.3),
      Dense(32),
      BatchNormalization(),
      LeakyReLU(alpha=0.01),
      Dropout(0.1),

      Dense(1, activation='sigmoid')
  ])

  my_optimizer = Adam(learning_rate=0.001)

  model.compile(
      optimizer=my_optimizer,
      loss='binary_crossentropy',
      metrics=['accuracy']
  )

  #model.summary()

  early_stop = EarlyStopping(monitor='val_accuracy', patience=5, restore_best_weights=True)

  history = model.fit(
      X_train_scaled, y_train,
      epochs=50,
      batch_size=64,
      validation_split=0.2,
      callbacks=[early_stop]
  )

  y_pred_prob = model.predict(X_test_scaled)
  y_pred = (y_pred_prob > 0.5).astype(int)

  accuracy = accuracy_score(y_test, y_pred)
  print(f"Neural Network Accuracy: {accuracy:.2%}")

  # Classification report
  print(classification_report(y_test, y_pred))
  
  model.save('NHL_Win_Prediction.keras')
  joblib.dump(scaler, 'scaler.pkl')

def curr_teams_for_NN():
  year = 2024
  all_teams = []
  teams_list = [
    "NJD", "NYI", "NYR",
    "PHI", "PIT", "BOS",
    "BUF", "MTL", "OTT",
    "TOR", "CAR", "FLA",
    "TBL", "WSH", "CHI",
    "DET", "NSH", "STL",
    "CGY", "COL", "EDM",
    "VAN", "ANA", "DAL",
    "LAK", "SJS",
    "CBJ", "MIN", "WPG", 
    "VGK", "SEA", "UTA"
  ]
  teams = pd.read_csv('2024_2025_All_Teams_Data_TEST.csv')
  for team in teams_list:
    h_data = teams.loc[teams['team'] == team].iloc[0]
    
    h_st = get_PP_PK(team, year)

    h_pp, h_pk = h_st[0]

    row = {
        "Team": team,
        "GF": h_data['goalsFor'],
        "GF_Pct": h_data['goalsFor'] / (h_data['goalsFor'] + h_data['goalsAgainst']),
        "GA": h_data['goalsAgainst'],
        "Shots_For": h_data['shotsOnGoalFor'],
        "Shooting_Pct": h_data['xShotAttemptsPercentage'],
        "Save_Pct": h_data['goalsAgainst'] / h_data['shotsOnGoalAgainst'],
        "PDO": h_data['xShotAttemptsPercentage'] + (h_data['goalsAgainst'] / h_data['shotsOnGoalAgainst']),
        "PP_Pct": h_pp,
        "PK_Pct": h_pk,
    }

    all_teams.append(row)

  X = pd.DataFrame(all_teams)

  X.to_csv('Curr_Teams_For_NN.csv', index=False)

main()