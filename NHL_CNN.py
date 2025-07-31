import requests
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import Conv1D, GlobalMaxPooling1D, Flatten, Dense, Dropout
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.metrics import AUC
import joblib
from sklearn.metrics import brier_score_loss


WINDOW_SIZE = 3
def time_to_float(time_str):
    minutes, seconds = map(int, time_str.split(":"))
    return minutes + seconds / 60

column_index = {"timeRemaining": 0, "homeScore": 1, "awayScore": 2, "homeSog": 3, "awaySog": 4,
                "homeBlockedShots": 5, "awayBlockedShots": 6, "homeMissedShots": 7, "awayMissedShots": 8,
                "homeShotAttempts": 9, "awayShotAttempts": 10, "homeHits": 11, "awayHits": 12,
                "homePIM": 13, "awayPIM": 14, "homeGiveaways": 15, "awayGiveaways": 16,
                "homeTakeaways": 17, "awayTakeaways": 18, "homeFaceoffs": 19, "awayFaceoffs": 20,
                "goalDiff": 21, "xFaceoff": 22, "goalDiff_timeRemaining": 23}

def get_all_games(year, year2):

  all_seasons = []
  for i in range(year, year2):
    gameCount = 1
    all_games = []
    while True:
        if gameCount % 100 == 0:
            print(f"Processed {gameCount} games so far...")

        gameId = f"{i}02{gameCount:04}"
        url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"
        res = requests.get(url)

        if res.status_code == 404:
            print("Error 404: Not Found")
            break

        data = res.json()
        game_data = get_game_events(data)
        all_games.append(game_data)
        gameCount += 1

    all_seasons.append(all_games)


    np.save(f"{i}_{i+1}_all_games_CNN.npy", np.array(all_games, dtype=object))

  return all_seasons

def get_game_events(data):
  game_state = np.zeros([1, len(column_index)])
  home_id = data["homeTeam"]["id"]
  for event in data["plays"]:

    new_row = game_state[-1]
    new_row[column_index["timeRemaining"]] = 60 - (time_to_float(event["timeInPeriod"]) * event["periodDescriptor"]["number"])

    match event["typeCode"]:
      # FACEOFF
      case 502:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeFaceoffs"]] += 1
          else:
            new_row[column_index["awayFaceoffs"]] += 1

        new_row[column_index["xFaceoff"]] = new_row[column_index["homeFaceoffs"]] / (new_row[column_index["homeFaceoffs"]] + new_row[column_index["awayFaceoffs"]])

      # HIT
      case 503:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeHits"]] += 1
          else:
            new_row[column_index["awayHits"]] += 1
      # Giveaway
      case 504:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeGiveaways"]] += 1
          else:
            new_row[column_index["awayGiveaways"]] += 1

      # Goal
      case 505:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeScore"]] += 1
          else:
            new_row[column_index["awayScore"]] += 1
          new_row[column_index["goalDiff"]] = new_row[column_index["homeScore"]] - new_row[column_index["awayScore"]]

      # SOG
      case 506:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeSog"]] += 1
            new_row[column_index["homeShotAttempts"]] += 1
          else:
            new_row[column_index["awaySog"]] += 1
            new_row[column_index["awayShotAttempts"]] += 1

      # Missed shot
      case 507:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeMissedShots"]] += 1
            new_row[column_index["homeShotAttempts"]] += 1
          else:
            new_row[column_index["awayMissedShots"]] += 1
            new_row[column_index["awayShotAttempts"]] += 1

      # Blocked Shot
      case 508:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeBlockedShots"]] += 1
            new_row[column_index["awayShotAttempts"]] += 1
          else:
            new_row[column_index["awayBlockedShots"]] += 1
            new_row[column_index["homeShotAttempts"]] += 1

      # Giveaway
      case 509:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homePIM"]] += event["details"]["duration"]
          else:
            new_row[column_index["awayPIM"]] += event["details"]["duration"]

      # Takeaway
      case 525:
        if (teamId := event["details"].get("eventOwnerTeamId", None)) is not None:
          if teamId == home_id:
            new_row[column_index["homeTakeaways"]] += 1
          else:
            new_row[column_index["awayTakeaways"]] += 1

    if new_row[column_index["timeRemaining"]] > 0:
      new_row[column_index["goalDiff_timeRemaining"]] = (new_row[column_index["goalDiff"]] / new_row[column_index["timeRemaining"]])
    else:
        new_row[column_index["goalDiff_timeRemaining"]] = new_row[column_index["goalDiff"]]


    game_state = np.vstack((game_state, new_row))

  return game_state

def preprocess(data):
  X = []
  y = []

  for game in data:
    home_score = game[-1, column_index["homeScore"]]
    away_score = game[-1, column_index["awayScore"]]
    label = 1 if home_score > away_score else 0

    for i in range(len(game) - WINDOW_SIZE + 1):
      window = game[i:i+WINDOW_SIZE]
      X.append(window)
      y.append(label)
  return np.array(X), np.array(y)

def scale(X):
  num_features = X.shape[2]
  X_reshaped = X.reshape(-1, num_features)

  scaler = StandardScaler()
  joblib.dump(scaler, 'cnn_scaler.pkl')
  X_scaled = scaler.fit_transform(X_reshaped)

  X = X_scaled.reshape(-1, WINDOW_SIZE, num_features)

  return X

def main():
  #get_all_games(2014, 2015)
  data = np.load("2014_2015_all_games_CNN.npy", allow_pickle=True)
  X, y = preprocess(data)

  X_train, X_test, y_train, y_test = train_test_split(
      X, y, test_size=0.2, stratify=y, random_state=42
  )


  model = Sequential([
    Conv1D(32, kernel_size=2, activation='relu', input_shape=(WINDOW_SIZE, X.shape[2])),
    Dropout(0.4),
    Conv1D(64, kernel_size=2, activation='relu'),
    Dropout(0.2),
    GlobalMaxPooling1D(),
    Dense(64, activation='relu'),
    Dropout(0.3),
    Dense(32, activation='relu'),
    Dense(1, activation='sigmoid')
  ])

  early_stop = EarlyStopping(monitor='val_accuracy', patience=5, restore_best_weights=True)

  model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy', 'mae'])
  model.fit(X_train, y_train, epochs=10, batch_size=64, validation_split=0.2, callbacks=[early_stop])

  loss, acc, mae = model.evaluate(X_test, y_test)
  print(f"Accuracy: {acc:.4f} | MAE: {mae:.4f}")
  model.save('NHL_Live_Win_Prediction.keras')
  # Predict probabilities
  y_pred_probs = model.predict(X_test)


  import csv

  with open("window_predictions.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["GameID", "Window", "WinProbability", "HomeScore", "AwayScore", "Actual", "Predicted", "Correct"])

    game_id = 0
    for game in data:
        if game_id > 5:
          break
        X_game, y_game = preprocess([game])
        y_pred_probs = model.predict(X_game)

        for i in range(len(X_game)):
            window = X_game[i]
            prob = y_pred_probs[i][0]
            actual = y_game[i]
            predicted = int(prob > 0.5)
            correct = int(predicted == actual)

            home_score = window[-1][column_index["homeScore"]]
            away_score = window[-1][column_index["awayScore"]]

            writer.writerow([game_id, i+1, prob, home_score, away_score, actual, predicted, correct])

        game_id += 1


def testGame():
  url = f'https://api-web.nhle.com/v1/gamecenter/2024020001/play-by-play'
  res = requests.get(url)

  data = res.json()

  X = get_game_events(data)
  window = X[155:155+WINDOW_SIZE]
  s = joblib.load(r'cnn_scaler.pkl')
  m = load_model("NHL_Live_Win_Prediction.keras")
  scaled_window = s.transform(window)

  # Reshape for model: (1, 5, num_features)
  scaled_window = np.expand_dims(scaled_window, axis=0)

  win_prob = m.predict(scaled_window)[0][0]
  print(f"Home team win probability: {win_prob:.2%}")

main()